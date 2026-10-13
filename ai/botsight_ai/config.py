"""Settings from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _int(key: str, default: int) -> int:
    v = os.environ.get(key)
    return int(v) if v else default


def _float(key: str, default: float) -> float:
    v = os.environ.get(key)
    return float(v) if v else default


@dataclass(frozen=True)
class ClickHouse:
    host: str = "localhost"
    port: int = 8123  # HTTP interface
    user: str = "default"
    password: str = ""
    database: str = "botsight"


@dataclass(frozen=True)
class LLMConfig:
    """Any OpenAI-compatible chat API: vLLM, Ollama or a provider hosted in Russia
    (Botsight.md, 14.4). Empty base_url disables the LLM."""

    base_url: str = ""
    api_key: str = ""
    model: str = ""
    timeout: float = 60.0
    price_rub_per_1k_tokens: float = 0.0  # for the cost metric (Botsight.md, 14.1, item 8)

    @property
    def enabled(self) -> bool:
        return bool(self.base_url and self.model)


@dataclass(frozen=True)
class Config:
    clickhouse: ClickHouse = field(default_factory=ClickHouse)
    qdrant_url: str = "http://localhost:6333"
    redis_url: str = "redis://localhost:6379/0"
    kafka_brokers: str = "localhost:9094"
    kafka_group: str = "ai-worker"
    llm: LLMConfig = field(default_factory=LLMConfig)

    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    model_cache: str = ""
    embed_batch: int = 64

    daily_text_limit: int = 20_000  # per project; above it 1 in 10 texts is kept
    topics_interval: float = 3600.0
    topics_max_points: int = 20_000
    min_cluster_size: int = 8
    trend_growth: float = 3.0
    trend_min_messages: int = 20
    trend_min_previous_total: int = 50

    anomaly_z: float = 4.0
    http_addr: str = "0.0.0.0:8090"
    internal_token: str = ""  # shared with the api service
    nl_password_secret: str = ""  # derives passwords of nl_p{project_id} users

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            clickhouse=ClickHouse(
                host=_env("CLICKHOUSE_HOST", "localhost"),
                port=_int("CLICKHOUSE_HTTP_PORT", 8123),
                user=_env("CLICKHOUSE_USER", "default"),
                password=_env("CLICKHOUSE_PASSWORD"),
                database=_env("CLICKHOUSE_DATABASE", "botsight"),
            ),
            qdrant_url=_env("QDRANT_URL", "http://localhost:6333"),
            redis_url=_env("REDIS_URL", "redis://localhost:6379/0"),
            kafka_brokers=_env("KAFKA_BROKERS", "localhost:9094"),
            kafka_group=_env("AI_KAFKA_GROUP", "ai-worker"),
            llm=LLMConfig(
                base_url=_env("LLM_BASE_URL").rstrip("/"),
                api_key=_env("LLM_API_KEY"),
                model=_env("LLM_MODEL"),
                timeout=_float("LLM_TIMEOUT", 60.0),
                price_rub_per_1k_tokens=_float("LLM_PRICE_RUB_PER_1K_TOKENS", 0.0),
            ),
            embedding_model=_env(
                "AI_EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
            ),
            model_cache=_env("AI_MODEL_CACHE"),
            daily_text_limit=_int("AI_DAILY_TEXT_LIMIT", 20_000),
            topics_interval=_float("AI_TOPICS_INTERVAL_SECONDS", 3600.0),
            topics_max_points=_int("AI_TOPICS_MAX_POINTS", 20_000),
            min_cluster_size=_int("AI_MIN_CLUSTER_SIZE", 8),
            trend_growth=_float("AI_TREND_GROWTH", 3.0),
            trend_min_messages=_int("AI_TREND_MIN_MESSAGES", 20),
            anomaly_z=_float("AI_ANOMALY_Z", 4.0),
            http_addr=_env("AI_HTTP_ADDR", "0.0.0.0:8090"),
            internal_token=_env("AI_INTERNAL_TOKEN"),
            nl_password_secret=_env("AI_NL_PASSWORD_SECRET"),
        )
