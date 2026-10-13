"""Hourly topics of a project (Botsight.md, 14.1, items 5-7).

A run clusters the vectors of the last 7 days, keeps topic ids stable by matching
centroids with the previous run, names new topics, counts messages per topic for this
and the previous week and raises an alert when a topic's share grows several times.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import redis
from clickhouse_connect.driver.client import Client

from . import clustering, metrics
from .alerts import Alert, AlertSink
from .config import Config
from .llm import LLM, LLMError, parse_json_object
from .storage import VectorStore

log = logging.getLogger(__name__)

DAY_MS = 86_400_000
MATCH_THRESHOLD = 0.8  # same topic as in the previous run
ASSIGN_THRESHOLD = 0.6  # noise and older texts join the nearest topic above this
EXAMPLES = 10

NAMING_PROMPT = """Ты помогаешь владельцу Telegram-бота понять, о чём ему пишут пользователи.
Тебе дают сообщения пользователей, относящиеся к одной теме.
Ответь JSON-объектом без пояснений: {"name": "...", "description": "..."}.
name — название темы на русском, 2–5 слов, без кавычек и точки в конце, например «Не приходит код подтверждения».
description — одно предложение о том, чего хотят или на что жалуются пользователи.
Не цитируй персональные данные из сообщений."""


@dataclass
class Topic:
    topic_id: int
    name: str = ""
    description: str = ""
    keywords: list[str] = field(default_factory=list)
    centroid: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.float32))
    named_by: str = ""
    size: int = 0
    messages_7d: int = 0
    messages_prev_7d: int = 0
    share_7d: float = 0.0
    share_prev_7d: float = 0.0
    growth: float = 0.0
    example_event_ids: list[str] = field(default_factory=list)


@dataclass
class RunResult:
    project_id: int
    status: str  # done | too_few
    topics: list[Topic] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)


class TopicRunner:
    def __init__(
        self, cfg: Config, vectors: VectorStore, ch: Client, llm: LLM, sink: AlertSink
    ) -> None:
        self.cfg = cfg
        self.vectors = vectors
        self.ch = ch
        self.llm = llm
        self.sink = sink

    def run(self, project_id: int, now_ms: int | None = None) -> RunResult:
        started = time.perf_counter()
        now_ms = now_ms or int(time.time() * 1000)
        try:
            result = self._run(project_id, now_ms)
            metrics.topic_runs.labels(result.status).inc()
            return result
        except Exception:
            metrics.topic_runs.labels("error").inc()
            raise
        finally:
            metrics.topic_run_seconds.observe(time.perf_counter() - started)

    def _run(self, project_id: int, now_ms: int) -> RunResult:
        week_ago = now_ms - 7 * DAY_MS
        points = self.vectors.scroll(
            project_id, now_ms - 14 * DAY_MS, 2 * self.cfg.topics_max_points
        )
        last_ts = np.array([int(p.get("last_ts", 0)) for p in points.payloads], dtype=np.int64)
        words = np.array(
            [int(p.get("words", clustering.MIN_CONTENT_WORDS)) for p in points.payloads],
            dtype=np.int64,
        )
        topical = words >= clustering.MIN_CONTENT_WORDS  # small talk never gets a topic
        recent = np.where((last_ts >= week_ago) & topical)[0]
        if len(recent) > self.cfg.topics_max_points:  # newest texts win when there are too many
            recent = recent[np.argsort(-last_ts[recent])[: self.cfg.topics_max_points]]
        if len(recent) < max(20, 2 * self.cfg.min_cluster_size):
            return RunResult(project_id, "too_few")

        labels = clustering.cluster(points.vectors[recent], self.cfg.min_cluster_size)
        new_centroids = clustering.centroids(points.vectors[recent], labels)

        previous = self._load_topics(project_id)
        next_id = (
            max([t.topic_id for t in previous.values()] + [self._max_topic_id(project_id)]) + 1
        )
        mapping = clustering.match_topics(
            new_centroids,
            {tid: t.centroid for tid, t in previous.items() if t.centroid.size},
            next_id,
            MATCH_THRESHOLD,
        )
        topic_centroids = {mapping[label]: c for label, c in new_centroids.items()}

        # every point of the last 14 days gets a topic (0 = none)
        assigned = clustering.assign(points.vectors, topic_centroids, ASSIGN_THRESHOLD)
        assigned[~topical] = 0
        for idx, label in zip(recent, labels, strict=True):
            if label >= 0:
                assigned[idx] = mapping[int(label)]
        self._store_assignments(project_id, points.ids, assigned, now_ms)

        topics = {tid: previous.get(tid, Topic(tid)) for tid in topic_centroids}
        for tid, c in topic_centroids.items():
            topics[tid].centroid = c
            topics[tid].size = int((assigned == tid).sum())

        self._count_messages(project_id, points.ids, assigned, topics, now_ms)
        self._describe(project_id, points, assigned, topics)
        alerts = self._trends(project_id, topics, now_ms)

        gone = [t for tid, t in previous.items() if tid not in topics]
        self._store_topics(project_id, list(topics.values()), gone, now_ms)
        for a in alerts:
            self.sink.send(a)
        return RunResult(
            project_id, "done", sorted(topics.values(), key=lambda t: -t.messages_7d), alerts
        )

    # --- storage -------------------------------------------------------------

    def _load_topics(self, project_id: int) -> dict[int, Topic]:
        rows = self.ch.query(
            "SELECT topic_id, name, description, keywords, centroid, named_by "
            "FROM topics FINAL WHERE project_id = {p:UInt32} AND active = 1",
            parameters={"p": project_id},
        ).result_rows
        return {
            int(r[0]): Topic(
                topic_id=int(r[0]),
                name=r[1],
                description=r[2],
                keywords=list(r[3]),
                centroid=np.array(r[4], dtype=np.float32),
                named_by=r[5],
            )
            for r in rows
        }

    def _max_topic_id(self, project_id: int) -> int:
        rows = self.ch.query(
            "SELECT max(topic_id) FROM topics WHERE project_id = {p:UInt32}",
            parameters={"p": project_id},
        ).result_rows
        return int(rows[0][0] or 0) if rows else 0

    def _store_assignments(
        self, project_id: int, ids: list[str], assigned: np.ndarray, now_ms: int
    ) -> None:
        at = datetime.fromtimestamp(now_ms / 1000, UTC)
        self.ch.insert(
            "point_topics",
            [(project_id, pid, int(tid), at) for pid, tid in zip(ids, assigned, strict=True)],
            column_names=["project_id", "point_id", "topic_id", "assigned_at"],
        )

    def _count_messages(
        self,
        project_id: int,
        ids: list[str],
        assigned: np.ndarray,
        topics: dict[int, Topic],
        now_ms: int,
    ) -> None:
        """Messages, not unique texts: a complaint repeated by 500 users weighs 500."""
        topic_of = dict(zip(ids, assigned.tolist(), strict=True))
        rows = self.ch.query(
            "SELECT toString(point_id), "
            "countIf(ts >= fromUnixTimestamp64Milli({week:Int64})), "
            "countIf(ts < fromUnixTimestamp64Milli({week:Int64})) "
            "FROM message_points WHERE project_id = {p:UInt32} "
            "AND ts >= fromUnixTimestamp64Milli({start:Int64}) GROUP BY point_id",
            parameters={
                "p": project_id,
                "week": now_ms - 7 * DAY_MS,
                "start": now_ms - 14 * DAY_MS,
            },
        ).result_rows
        total7 = total_prev = 0
        for pid, n7, nprev in rows:
            total7 += n7
            total_prev += nprev
            t = topics.get(int(topic_of.get(pid, 0)))
            if t is not None:
                t.messages_7d += n7
                t.messages_prev_7d += nprev
        enough_history = total_prev >= self.cfg.trend_min_previous_total
        for t in topics.values():
            t.share_7d = t.messages_7d / total7 if total7 else 0.0
            t.share_prev_7d = t.messages_prev_7d / total_prev if total_prev else 0.0
            if not enough_history:
                t.growth = 0.0  # unknown: no previous week to compare with
            else:
                t.growth = min(t.share_7d / max(t.share_prev_7d, 1e-9), 1000.0)

    def _describe(
        self, project_id: int, points: Any, assigned: np.ndarray, topics: dict[int, Topic]
    ) -> None:
        """Examples nearest to the centre; keywords for every topic; LLM names for new ones."""
        examples: dict[int, list[str]] = {}
        for tid, t in topics.items():
            idx = np.where(assigned == tid)[0]
            near = idx[clustering.nearest(points.vectors[idx], t.centroid, EXAMPLES)]
            t.example_event_ids = [str(points.payloads[i].get("event_id", "")) for i in near]
            t.example_event_ids = [e for e in t.example_event_ids if e]
            examples[tid] = t.example_event_ids
        texts = self._texts(project_id, [e for ids in examples.values() for e in ids])
        by_topic = {tid: [texts[e] for e in ids if e in texts] for tid, ids in examples.items()}
        for tid, words in clustering.keywords(by_topic).items():
            topics[tid].keywords = words
        for tid, t in topics.items():
            if t.name and t.named_by == "llm":
                continue
            if self.llm.enabled and by_topic[tid]:
                try:
                    t.name, t.description = self._llm_name(by_topic[tid])
                    t.named_by = "llm"
                    continue
                except LLMError as exc:
                    log.warning("topic naming failed, using keywords: %s", exc)
            # keyword names follow the topic as its messages change
            t.name = ", ".join(t.keywords[:3]) or t.name or f"Тема {tid}"
            t.named_by = "keywords"

    def _texts(self, project_id: int, event_ids: list[str]) -> dict[str, str]:
        if not event_ids:
            return {}
        rows = self.ch.query(
            "SELECT toString(event_id), text FROM messages_text "
            "WHERE project_id = {p:UInt32} AND event_id IN {ids:Array(UUID)}",
            parameters={"p": project_id, "ids": event_ids},
        ).result_rows
        return {r[0]: r[1] for r in rows}

    def _llm_name(self, texts: list[str]) -> tuple[str, str]:
        user = "Сообщения:\n" + "\n".join(f"- {t[:300]}" for t in texts[:EXAMPLES])
        reply = self.llm.complete(NAMING_PROMPT, user, purpose="topic_name", max_tokens=200)
        obj = parse_json_object(reply.text)
        name = str(obj.get("name", "")).strip().strip("«»\"'.")[:100]
        if not name:
            raise LLMError("empty topic name")
        return name, str(obj.get("description", "")).strip()[:300]

    def _trends(self, project_id: int, topics: dict[int, Topic], now_ms: int) -> list[Alert]:
        week = datetime.fromtimestamp(now_ms / 1000, UTC).strftime("%G-W%V")
        out: list[Alert] = []
        for t in topics.values():
            if t.growth < self.cfg.trend_growth or t.messages_7d < self.cfg.trend_min_messages:
                continue
            if t.messages_prev_7d == 0:
                title = f"Новая тема: «{t.name}»"
            else:
                title = f"Тема «{t.name}» выросла в {t.growth:.1f} раза"
            text = (
                f"За 7 дней {t.messages_7d} сообщений — {t.share_7d:.0%} всех сообщений, "
                f"неделей раньше было {t.messages_prev_7d} ({t.share_prev_7d:.0%})."
            )
            out.append(
                Alert(
                    project_id=project_id,
                    kind="topic_trend",
                    title=title,
                    text=text,
                    key=f"{t.topic_id}:{week}",
                    details={
                        "topic_id": t.topic_id,
                        "messages_7d": t.messages_7d,
                        "messages_prev_7d": t.messages_prev_7d,
                        "share_7d": round(t.share_7d, 4),
                        "share_prev_7d": round(t.share_prev_7d, 4),
                        "growth": round(t.growth, 2),
                        "keywords": t.keywords,
                    },
                )
            )
        return out

    def _store_topics(
        self, project_id: int, active: list[Topic], gone: list[Topic], now_ms: int
    ) -> None:
        at = datetime.fromtimestamp(now_ms / 1000, UTC)
        rows = [self._row(project_id, t, 1, at) for t in active] + [
            self._row(project_id, t, 0, at) for t in gone
        ]
        if rows:
            self.ch.insert(
                "topics",
                rows,
                column_names=[
                    "project_id",
                    "topic_id",
                    "name",
                    "description",
                    "keywords",
                    "size",
                    "messages_7d",
                    "messages_prev_7d",
                    "share_7d",
                    "share_prev_7d",
                    "growth",
                    "example_event_ids",
                    "centroid",
                    "active",
                    "named_by",
                    "updated_at",
                ],
            )

    @staticmethod
    def _row(project_id: int, t: Topic, active: int, at: datetime) -> list[Any]:
        return [
            project_id,
            t.topic_id,
            t.name,
            t.description,
            t.keywords,
            t.size,
            t.messages_7d,
            t.messages_prev_7d,
            t.share_7d,
            t.share_prev_7d,
            t.growth,
            t.example_event_ids,
            t.centroid.astype(np.float32).tolist(),
            active,
            t.named_by,
            at,
        ]


class TopicScheduler:
    """Runs projects with new texts once per interval. A Redis lock per project lets
    several ai-worker instances share the work without running a project twice."""

    def __init__(self, runner: TopicRunner, ch: Client, rdb: redis.Redis, interval: float) -> None:
        self.runner = runner
        self.ch = ch
        self.rdb = rdb
        self.interval = interval

    def due_projects(self) -> list[int]:
        rows = self.ch.query(
            "SELECT DISTINCT project_id FROM message_points "
            "WHERE ts >= now64(3) - toIntervalSecond({s:UInt32})",
            parameters={"s": int(self.interval)},
        ).result_rows
        return [int(r[0]) for r in rows]

    def tick(self) -> int:
        done = 0
        for project_id in self.due_projects():
            if self.rdb.exists(f"ai:topics:done:{project_id}"):
                continue
            lock = self.rdb.lock(f"ai:topics:lock:{project_id}", timeout=1800, blocking=False)
            if not lock.acquire():
                continue
            try:
                result = self.runner.run(project_id)
                self.rdb.set(f"ai:topics:done:{project_id}", result.status, ex=int(self.interval))
                done += 1
                log.info(
                    "topics: project %d %s, %d topics, %d alerts",
                    project_id,
                    result.status,
                    len(result.topics),
                    len(result.alerts),
                )
            except Exception:
                log.exception("topics: project %d failed", project_id)
            finally:
                lock.release()
        day = time.strftime("%Y%m%d", time.gmtime())
        if self.rdb.set(f"ai:retention:{day}", 1, nx=True, ex=2 * 86400):
            self.runner.vectors.delete_older(int(time.time() * 1000) - 30 * DAY_MS)
        return done

    def loop(self, stop: threading.Event, every: float = 60.0) -> None:
        while not stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("topics scheduler tick failed")
            stop.wait(every)
