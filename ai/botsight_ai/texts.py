"""From messages.text to vectors (Botsight.md, 14.1, items 1-4).

Every message is normalized; equal texts of a project share one point in Qdrant and one
embedding. Which point each message maps to is written to ClickHouse (message_points),
so topic sizes and trends count messages, not unique texts.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import redis
from clickhouse_connect.driver.client import Client

from . import metrics
from .clustering import content_words
from .embedder import Embedder
from .normalize import normalize, point_id
from .storage import VectorStore, tenant

log = logging.getLogger(__name__)

SAMPLE_EVERY = 10  # above the daily limit one text in SAMPLE_EVERY is kept


@dataclass(frozen=True)
class TextRecord:
    project_id: int
    event_id: str
    ts: int  # unix ms
    text: str

    @classmethod
    def from_kafka(cls, value: bytes) -> TextRecord | None:
        try:
            v: dict[str, Any] = json.loads(value)
            rec = cls(int(v["project_id"]), str(v["event_id"]), int(v["ts"]), str(v["text"]))
        except (ValueError, KeyError, TypeError):
            return None
        return rec if rec.project_id > 0 and rec.ts > 0 else None


class TextIngestor:
    def __init__(
        self,
        embedder: Embedder,
        vectors: VectorStore,
        ch: Client,
        rdb: redis.Redis,
        daily_limit: int,
    ) -> None:
        self.embedder = embedder
        self.vectors = vectors
        self.ch = ch
        self.rdb = rdb
        self.daily_limit = daily_limit

    def process(self, records: list[TextRecord]) -> None:
        """Idempotent for a repeated batch: Qdrant upserts by id, message_points is a
        ReplacingMergeTree, so at-least-once delivery from Kafka is enough."""
        kept = self._apply_daily_limit(records)

        # message -> (point id, normalized text); one embedding per unique text
        rows: list[tuple[int, str, datetime, str]] = []
        latest: dict[str, tuple[int, str, int, str]] = {}  # pid -> (project, event, ts, text)
        for r in kept:
            norm = normalize(r.text)
            if not norm:
                metrics.texts.labels("empty").inc()
                continue
            pid = point_id(r.project_id, norm)
            rows.append((r.project_id, r.event_id, datetime.fromtimestamp(r.ts / 1000, UTC), pid))
            prev = latest.get(pid)
            if prev is None or r.ts >= prev[2]:
                latest[pid] = (r.project_id, r.event_id, r.ts, norm)
        if not rows:
            return

        existing = self.vectors.existing(list(latest))
        new_ids = [pid for pid in latest if pid not in existing]
        if new_ids:
            vecs = self.embedder.embed([latest[pid][3] for pid in new_ids])
            self.vectors.upsert(
                new_ids,
                vecs,
                [
                    {
                        "project_id": tenant(latest[p][0]),
                        "last_ts": latest[p][2],
                        "event_id": latest[p][1],
                        "words": content_words(latest[p][3]),
                    }
                    for p in new_ids
                ],
            )
        self.vectors.set_payloads(
            (pid, {"last_ts": latest[pid][2], "event_id": latest[pid][1]})
            for pid in latest
            if pid in existing
        )
        metrics.texts.labels("embedded").inc(len(new_ids))
        metrics.texts.labels("duplicate").inc(len(rows) - len(new_ids))

        self.ch.insert(
            "message_points", rows, column_names=["project_id", "event_id", "ts", "point_id"]
        )

    def _apply_daily_limit(self, records: list[TextRecord]) -> list[TextRecord]:
        """Per-project daily limit of texts (Botsight.md, 14.1, item 8): above it a stable
        sample of one text in ten keeps trends visible at a bounded cost."""
        by_project: dict[int, list[TextRecord]] = defaultdict(list)
        for r in records:
            by_project[r.project_id].append(r)
        day = time.strftime("%Y%m%d", time.gmtime())
        kept: list[TextRecord] = []
        pipe = self.rdb.pipeline()
        for project_id, group in by_project.items():
            key = f"ai:texts:{project_id}:{day}"
            pipe.incrby(key, len(group))
            pipe.expire(key, 2 * 86400)
        totals = pipe.execute()[0::2]
        for (_project_id, group), total in zip(by_project.items(), totals, strict=True):
            room = max(0, self.daily_limit - (int(total) - len(group)))
            kept.extend(group[:room])
            for r in group[room:]:
                digest = hashlib.blake2b(r.event_id.encode(), digest_size=2).digest()
                if int.from_bytes(digest, "big") % SAMPLE_EVERY == 0:
                    kept.append(r)
                else:
                    metrics.texts.labels("sampled_out").inc()
        return kept
