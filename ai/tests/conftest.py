from __future__ import annotations

import itertools
import os
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis

from botsight_ai.config import ClickHouse, Config
from botsight_ai.embedder import HashEmbedder
from botsight_ai.llm import Completion
from botsight_ai.storage import VectorStore, clickhouse

ROOT = Path(__file__).resolve().parents[2]
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.8"  # as in deploy/docker-compose.yml
QDRANT_IMAGE = "qdrant/qdrant:v1.19.1"
REDIS_IMAGE = "redis:7.4-alpine"


class Services:
    def __init__(self) -> None:
        from testcontainers.core.container import DockerContainer

        self.containers: list[Any] = []
        ch = (
            DockerContainer(CLICKHOUSE_IMAGE)
            .with_env("CLICKHOUSE_DB", "botsight")
            .with_env("CLICKHOUSE_USER", "botsight")
            .with_env("CLICKHOUSE_PASSWORD", "botsight")
            .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
            .with_exposed_ports(8123)
        )
        qd = DockerContainer(QDRANT_IMAGE).with_exposed_ports(6333)
        rd = DockerContainer(REDIS_IMAGE).with_exposed_ports(6379)
        for c in (ch, qd, rd):
            c.start()
            self.containers.append(c)
        self.ch_cfg = ClickHouse(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            user="botsight",
            password="botsight",
            database="botsight",
        )
        self.qdrant_url = f"http://{qd.get_container_host_ip()}:{qd.get_exposed_port(6333)}"
        self.redis_url = f"redis://{rd.get_container_host_ip()}:{rd.get_exposed_port(6379)}/0"
        self._wait()
        self._migrate()

    def _wait(self) -> None:
        deadline = time.time() + 120
        while True:
            try:
                httpx.get(
                    f"http://{self.ch_cfg.host}:{self.ch_cfg.port}/ping", timeout=2
                ).raise_for_status()
                clickhouse(self.ch_cfg).query("SELECT 1")
                httpx.get(f"{self.qdrant_url}/readyz", timeout=2).raise_for_status()
                redis.Redis.from_url(self.redis_url).ping()
                return
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)

    def _migrate(self) -> None:
        """The ClickHouse migrations of the repository, split on semicolons like
        golang-migrate with x-multi-statement."""
        client = clickhouse(self.ch_cfg)
        for path in sorted((ROOT / "migrations" / "clickhouse").glob("*.up.sql")):
            for stmt in path.read_text().split(";"):
                code = "\n".join(
                    line for line in stmt.splitlines() if not line.strip().startswith("--")
                )
                if code.strip():
                    client.command(stmt)

    def close(self) -> None:
        for c in self.containers:
            c.stop()


@pytest.fixture(scope="session")
def services() -> Iterator[Services]:
    try:
        svc = Services()
    except Exception as exc:  # no Docker
        if os.environ.get("CI"):
            raise
        pytest.skip(f"integration environment unavailable (Docker?): {exc}")
    yield svc
    svc.close()


_projects = itertools.count(int(time.time()) % 100_000 * 10 + 1)


@pytest.fixture
def project_id() -> int:
    return next(_projects)


@pytest.fixture
def cfg(services: Services) -> Config:
    return Config(
        clickhouse=services.ch_cfg,
        qdrant_url=services.qdrant_url,
        redis_url=services.redis_url,
        min_cluster_size=5,
        trend_min_messages=10,
        trend_min_previous_total=20,
        nl_password_secret="test-secret",
        internal_token="test-token",
    )


@pytest.fixture
def ch(cfg: Config) -> Any:
    return clickhouse(cfg.clickhouse)


@pytest.fixture
def rdb(cfg: Config) -> redis.Redis:
    return redis.Redis.from_url(cfg.redis_url)


@pytest.fixture
def embedder() -> HashEmbedder:
    return HashEmbedder(dim=64)


@pytest.fixture
def vectors(cfg: Config, embedder: HashEmbedder) -> VectorStore:
    # one collection per test run; projects isolate tests inside it
    return VectorStore(cfg.qdrant_url, embedder.dim, collection="test_messages")


class FakeLLM:
    """Returns prepared replies and records the prompts."""

    enabled = True

    def __init__(self, reply: str | Callable[[str, str], str]) -> None:
        self.reply = reply
        self.calls: list[tuple[str, str, str]] = []

    def complete(
        self, system: str, user: str, *, purpose: str, max_tokens: int = 400
    ) -> Completion:
        self.calls.append((purpose, system, user))
        text = self.reply(system, user) if callable(self.reply) else self.reply
        return Completion(text=text, prompt_tokens=100, completion_tokens=20)


def ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def new_uuid() -> str:
    return str(uuid.uuid4())


EVENT_COLUMNS = [
    "project_id",
    "bot_id",
    "event_id",
    "ts",
    "received_at",
    "type",
    "name",
    "user_id",
    "chat_id",
    "command",
    "start_param",
    "state",
    "handler",
    "state_from",
    "state_to",
    "error",
    "ok",
    "member_status",
    "handled",
]


def event_row(project_id: int, at: datetime, **f: Any) -> list[Any]:
    """A row of the events table with defaults for the columns tests do not care about."""
    values = {
        "project_id": project_id,
        "bot_id": 77,
        "event_id": new_uuid(),
        "ts": at,
        "received_at": at,
        "type": "update",
        "name": "message",
        "user_id": 1,
        "chat_id": 1,
        "command": "",
        "start_param": "",
        "state": "",
        "handler": "",
        "state_from": "",
        "state_to": "",
        "error": "",
        "ok": 1,
        "member_status": "",
        "handled": 1,
    }
    values.update(f)
    return [values[c] for c in EVENT_COLUMNS]


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)
