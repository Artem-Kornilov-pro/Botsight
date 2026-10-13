"""ClickHouse and Qdrant access."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
import numpy as np
from clickhouse_connect.driver.client import Client
from qdrant_client import QdrantClient, models

from .config import ClickHouse
from .embedder import Vectors


def clickhouse(cfg: ClickHouse, **kwargs: Any) -> Client:
    """A client per thread: clickhouse-connect clients are not shared between threads."""
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=kwargs.pop("username", cfg.user),
        password=kwargs.pop("password", cfg.password),
        database=cfg.database,
        autogenerate_session_id=False,
        **kwargs,
    )


def tenant(project_id: int) -> str:
    """The project_id payload value: a keyword, see VectorStore._ensure."""
    return str(project_id)


@dataclass
class Points:
    ids: list[str]
    vectors: Vectors
    payloads: list[dict[str, Any]]

    def __len__(self) -> int:
        return len(self.ids)


class VectorStore:
    """Qdrant collection of message vectors, one point per unique normalized text of a
    project (Botsight.md, 12.6). project_id is the tenant key: Qdrant keeps each project's
    points together, and every query filters by it."""

    def __init__(self, url: str, dim: int, collection: str = "messages") -> None:
        self.client = QdrantClient(url=url, timeout=30)
        self.dim = dim
        self.collection = collection
        self._ensure()

    def _ensure(self) -> None:
        if self.client.collection_exists(self.collection):
            info = self.client.get_collection(self.collection)
            params = info.config.params.vectors
            size = params.size if isinstance(params, models.VectorParams) else None
            if size != self.dim:
                raise RuntimeError(
                    f"Qdrant collection {self.collection} has vectors of size {size}, "
                    f"the embedding model gives {self.dim}: use a new collection after a model change"
                )
            return
        self.client.create_collection(
            self.collection,
            vectors_config=models.VectorParams(size=self.dim, distance=models.Distance.COSINE),
            hnsw_config=models.HnswConfigDiff(payload_m=16, m=0),  # per-tenant graphs
        )
        # project_id is the tenant: a keyword index with is_tenant keeps each project's
        # points together on disk; last_ts is the main range filter (is_principal).
        self.client.create_payload_index(
            self.collection,
            "project_id",
            models.KeywordIndexParams(type=models.KeywordIndexType.KEYWORD, is_tenant=True),
        )
        self.client.create_payload_index(
            self.collection,
            "last_ts",
            models.IntegerIndexParams(
                type=models.IntegerIndexType.INTEGER, lookup=False, range=True, is_principal=True
            ),
        )

    def existing(self, ids: Sequence[str]) -> set[str]:
        if not ids:
            return set()
        found = self.client.retrieve(
            self.collection, ids=list(ids), with_payload=False, with_vectors=False
        )
        return {str(p.id) for p in found}

    def upsert(
        self, ids: Sequence[str], vectors: Vectors, payloads: Sequence[dict[str, Any]]
    ) -> None:
        if not ids:
            return
        self.client.upsert(
            self.collection,
            points=models.Batch(ids=list(ids), vectors=vectors.tolist(), payloads=list(payloads)),
            wait=True,
        )

    def set_payloads(self, updates: Iterable[tuple[str, dict[str, Any]]]) -> None:
        ops: list[models.UpdateOperation] = [
            models.SetPayloadOperation(set_payload=models.SetPayload(payload=payload, points=[pid]))
            for pid, payload in updates
        ]
        if ops:
            self.client.batch_update_points(self.collection, update_operations=ops, wait=True)

    def _project_filter(self, project_id: int, since_ms: int | None = None) -> models.Filter:
        must: list[models.Condition] = [
            models.FieldCondition(
                key="project_id", match=models.MatchValue(value=tenant(project_id))
            )
        ]
        if since_ms is not None:
            must.append(models.FieldCondition(key="last_ts", range=models.Range(gte=since_ms)))
        return models.Filter(must=must)

    def scroll(self, project_id: int, since_ms: int, limit: int) -> Points:
        """Points of one project seen since since_ms, newest first is not guaranteed;
        at most limit points."""
        ids: list[str] = []
        vectors: list[list[float]] = []
        payloads: list[dict[str, Any]] = []
        offset: Any = None
        while len(ids) < limit:
            page, offset = self.client.scroll(
                self.collection,
                scroll_filter=self._project_filter(project_id, since_ms),
                limit=min(1000, limit - len(ids)),
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            for p in page:
                ids.append(str(p.id))
                vectors.append(list(p.vector))  # type: ignore[arg-type]
                payloads.append(dict(p.payload or {}))
            if offset is None:
                break
        arr = np.array(vectors, dtype=np.float32).reshape(len(ids), self.dim)
        return Points(ids, arr, payloads)

    def delete_older(self, cutoff_ms: int) -> None:
        """Vectors live as long as the texts (30 days, Botsight.md, 12.2)."""
        self.client.delete(
            self.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[models.FieldCondition(key="last_ts", range=models.Range(lt=cutoff_ms))]
                )
            ),
        )

    def count(self, project_id: int) -> int:
        return self.client.count(
            self.collection, count_filter=self._project_filter(project_id), exact=True
        ).count
