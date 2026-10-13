"""Anomalies (Botsight.md, 14.3).

Detection is statistics, not an LLM: for every project the last full hour is compared
with the same hour of the week in the previous 4 weeks (median and MAD, robust z-score).
The change is decomposed by segments (handlers, error types, entry points) and deploys
(bot_started) near the hour are found. The LLM only turns these facts into 2-3
sentences; without an LLM a template does.

One query covers all projects and 5 hours, so the cost grows with active projects, not
with the length of the history.
"""

from __future__ import annotations

import json
import logging
import statistics
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import redis
from clickhouse_connect.driver.client import Client

from . import metrics
from .alerts import Alert, AlertSink
from .llm import LLM, LLMError

log = logging.getLogger(__name__)
MSK = ZoneInfo("Europe/Moscow")
WEEKS = 4


@dataclass(frozen=True)
class Metric:
    name: str
    title: str  # in Russian, for texts
    value: str  # ClickHouse aggregate over events
    condition: str  # rows that make up the metric, for decomposition
    segment: str  # how to split the change
    direction: int  # +1: only growth is an anomaly, -1: only a drop
    min_change: float  # absolute change below this is ignored
    min_ratio: float  # and relative change


METRICS = (
    Metric(
        name="errors",
        title="Ошибки",
        value="countIf((type = 'step' AND error != '') OR (type = 'outgoing' AND ok = 0 "
        "AND error NOT IN ('', 'not_modified', 'bot_blocked', 'user_deactivated')))",
        condition="(type = 'step' AND error != '') OR (type = 'outgoing' AND ok = 0 "
        "AND error NOT IN ('', 'not_modified', 'bot_blocked', 'user_deactivated'))",
        segment="if(type = 'step', concat('хендлер ', handler, ': ', error), concat(name, ': ', error))",
        direction=1,
        min_change=10,
        min_ratio=2.0,
    ),
    Metric(
        name="blocks",
        title="Блокировки бота",
        value="countIf(error = 'bot_blocked' OR (name = 'my_chat_member' AND member_status = 'kicked'))",
        condition="error = 'bot_blocked' OR (name = 'my_chat_member' AND member_status = 'kicked')",
        segment="if(name = 'my_chat_member', 'блокировка в чате', concat('ошибка 403 в ', name))",
        direction=1,
        min_change=5,
        min_ratio=2.0,
    ),
    Metric(
        name="active_users",
        title="Активные пользователи",
        value="uniqIf(user_id, type = 'update' AND user_id > 0)",
        condition="type = 'update' AND user_id > 0",
        segment="if(command != '', concat('/', command), if(state != '', state, name))",
        direction=-1,
        min_change=20,
        min_ratio=1.5,
    ),
)


def robust_z(x: float, baseline: list[float]) -> tuple[float, float]:
    """z-score against the median with MAD as the scale. With 4 points the MAD is often
    0, so the scale has a Poisson-like floor sqrt(median) and never drops below 1."""
    med = statistics.median(baseline)
    mad = statistics.median(abs(b - med) for b in baseline)
    scale = max(1.4826 * mad, med**0.5, 1.0)
    return (x - med) / scale, med


@dataclass
class Finding:
    project_id: int
    metric: Metric
    hour: datetime
    current: float
    usual: float
    z: float
    segments: list[dict[str, Any]] = field(default_factory=list)
    deploys: list[str] = field(default_factory=list)

    def facts(self) -> dict[str, Any]:
        return {
            "metric": self.metric.title,
            "hour": self.hour.astimezone(MSK).strftime("%d.%m %H:00 МСК"),
            "current": round(self.current, 1),
            "usual": round(self.usual, 1),
            "change_times": round(self.current / self.usual, 1) if self.usual else None,
            "top_segments": self.segments,
            "bot_restarts_nearby": self.deploys,
        }


EXPLAIN_PROMPT = """Ты объясняешь владельцу Telegram-бота аномалию в его метриках.
Напиши 2–3 коротких предложения на русском, используя только факты из JSON.
Назови метрику, время, насколько она отличается от обычного, главные сегменты и перезапуски бота рядом по времени.
Не придумывай причин, которых нет в фактах. Без вступлений и markdown."""


class AnomalyDetector:
    def __init__(self, ch: Client, llm: LLM, sink: AlertSink, z_threshold: float) -> None:
        self.ch = ch
        self.llm = llm
        self.sink = sink
        self.z_threshold = z_threshold

    def run(self, hour: datetime) -> list[Finding]:
        """Checks the hour [hour, hour + 1h) of every project; sends one alert per finding."""
        hour = hour.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        findings = self.detect(hour)
        for f in findings:
            f.segments = self._segments(f)
            f.deploys = self._deploys(f)
            self.sink.send(self._alert(f))
            metrics.anomalies.labels(f.metric.name).inc()
        return findings

    def _hours_filter(self, hour: datetime) -> tuple[str, dict[str, Any]]:
        parts, params = [], {}
        for k in range(WEEKS + 1):
            start = hour - timedelta(days=7 * k)
            params[f"h{k}"] = start
            parts.append(
                f"(ts >= {{h{k}:DateTime64(3)}} AND ts < {{h{k}:DateTime64(3)}} + INTERVAL 1 HOUR)"
            )
        return "(" + " OR ".join(parts) + ")", params

    def detect(self, hour: datetime) -> list[Finding]:
        where, params = self._hours_filter(hour)
        cols = ", ".join(f"{m.value} AS {m.name}" for m in METRICS)
        rows = self.ch.query(
            f"SELECT project_id, toStartOfHour(ts) AS h, {cols} FROM events "
            f"WHERE {where} GROUP BY project_id, h",
            parameters=params,
        ).result_rows
        series: dict[int, dict[datetime, tuple[float, ...]]] = {}
        for project_id, h, *values in rows:
            h = h.replace(tzinfo=UTC) if h.tzinfo is None else h
            series.setdefault(int(project_id), {})[h] = tuple(float(v) for v in values)

        baseline_hours = [hour - timedelta(days=7 * k) for k in range(1, WEEKS + 1)]
        findings: list[Finding] = []
        for project_id, by_hour in series.items():
            # a project needs history: activity in at least 3 of the 4 baseline hours
            if sum(1 for h in baseline_hours if h in by_hour) < WEEKS - 1:
                continue
            zero = tuple(0.0 for _ in METRICS)
            current = by_hour.get(hour, zero)
            for i, m in enumerate(METRICS):
                baseline = [by_hour.get(h, zero)[i] for h in baseline_hours]
                x = current[i]
                z, usual = robust_z(x, baseline)
                change = (x - usual) * m.direction
                ratio = (
                    (x / usual if m.direction > 0 else usual / x) if x and usual else float("inf")
                )
                if (
                    z * m.direction >= self.z_threshold
                    and change >= m.min_change
                    and ratio >= m.min_ratio
                ):
                    findings.append(Finding(project_id, m, hour, x, usual, z))
        return findings

    def _segments(self, f: Finding) -> list[dict[str, Any]]:
        """Which segments explain the change: per-hour value of every segment, current hour
        against the average of the baseline hours."""
        where, params = self._hours_filter(f.hour)
        m = f.metric
        value = "uniq(user_id)" if m.name == "active_users" else "count()"
        rows = self.ch.query(
            f"SELECT seg, sumIf(v, h = {{cur:DateTime64(3)}}) AS cur, "
            f"sumIf(v, h != {{cur:DateTime64(3)}}) / {WEEKS} AS usual FROM ("
            f"  SELECT {m.segment} AS seg, toStartOfHour(ts) AS h, {value} AS v FROM events"
            f"  WHERE project_id = {{p:UInt32}} AND {where} AND ({m.condition}) GROUP BY seg, h"
            f") GROUP BY seg ORDER BY (cur - usual) * {m.direction} DESC LIMIT 5",
            parameters={**params, "cur": f.hour, "p": f.project_id},
        ).result_rows
        total = (f.current - f.usual) or 1.0
        out = []
        for seg, cur, usual in rows:
            delta = float(cur) - float(usual)
            if delta * m.direction <= 0:
                continue
            out.append(
                {
                    "segment": str(seg)[:120],
                    "current": round(float(cur), 1),
                    "usual": round(float(usual), 1),
                    "share_of_change": round(delta / total, 2),
                }
            )
        return out[:3]

    def _deploys(self, f: Finding) -> list[str]:
        rows = self.ch.query(
            "SELECT ts FROM events WHERE project_id = {p:UInt32} AND type = 'lifecycle' "
            "AND name = 'bot_started' AND ts >= {start:DateTime64(3)} AND ts < {end:DateTime64(3)} "
            "ORDER BY ts LIMIT 5",
            parameters={
                "p": f.project_id,
                "start": f.hour - timedelta(hours=2),
                "end": f.hour + timedelta(hours=1),
            },
        ).result_rows
        out = []
        for (ts,) in rows:
            ts = ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
            out.append(ts.astimezone(MSK).strftime("%H:%M МСК"))
        return out

    def _alert(self, f: Finding) -> Alert:
        facts = f.facts()
        text = self.template(f)
        if self.llm.enabled:
            try:
                reply = self.llm.complete(
                    EXPLAIN_PROMPT,
                    json.dumps(facts, ensure_ascii=False),
                    purpose="anomaly",
                    max_tokens=250,
                )
                if reply.text.strip():
                    text = reply.text.strip()[:1000]
            except LLMError as exc:
                log.warning("anomaly explanation failed, using the template: %s", exc)
        return Alert(
            project_id=f.project_id,
            kind="anomaly",
            title=self.title(f),
            text=text,
            details={"metric": f.metric.name, "z": round(f.z, 2), **facts},
            key=f"{f.metric.name}:{f.hour.isoformat()}",
        )

    @staticmethod
    def title(f: Finding) -> str:
        if f.metric.direction > 0:
            times = f"в {f.current / f.usual:.0f} раз" if f.usual >= 1 else f"до {f.current:.0f}"
            return f"{f.metric.title}: рост {times}"
        drop = 1 - f.current / f.usual if f.usual else 1
        return f"{f.metric.title}: на {drop:.0%} меньше обычного"

    @staticmethod
    def template(f: Finding) -> str:
        hour = f.hour.astimezone(MSK).strftime("%H:00 МСК")
        parts = [f"{f.metric.title} в {hour}: {f.current:.0f} при обычных {f.usual:.0f}."]
        if f.segments:
            top = "; ".join(
                f"{s['segment']} ({s['share_of_change']:.0%} изменения)" for s in f.segments
            )
            parts.append(f"Главный вклад: {top}.")
        if f.deploys:
            parts.append(f"Бот перезапускался рядом по времени: {', '.join(f.deploys)}.")
        return " ".join(parts)


class AnomalyScheduler:
    """Checks each finished hour once, 5 minutes after it ends (data needs ~10 s to arrive);
    a Redis key makes one ai-worker instance do it."""

    def __init__(
        self, detector: AnomalyDetector, rdb: redis.Redis, delay: timedelta = timedelta(minutes=5)
    ) -> None:
        self.detector = detector
        self.rdb = rdb
        self.delay = delay

    def tick(self, now: datetime | None = None) -> list[Finding] | None:
        now = now or datetime.now(UTC)
        hour = (now - self.delay).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        done = f"ai:anomalies:done:{hour.isoformat()}"
        if self.rdb.exists(done):
            return None
        lock = self.rdb.lock(f"ai:anomalies:lock:{hour.isoformat()}", timeout=1800, blocking=False)
        if not lock.acquire():
            return None
        try:
            findings = self.detector.run(hour)
            self.rdb.set(done, len(findings), ex=7 * 86400)  # a failed hour is retried next tick
        finally:
            lock.release()
        log.info("anomalies: hour %s, %d findings", hour.isoformat(), len(findings))
        return findings

    def loop(self, stop: threading.Event, every: float = 60.0) -> None:
        while not stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("anomaly check failed")
            stop.wait(every)
