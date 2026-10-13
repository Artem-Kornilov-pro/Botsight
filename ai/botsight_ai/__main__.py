"""ai-worker entry point.

python -m botsight_ai all        # everything in one process (docker compose)
python -m botsight_ai embed      # messages.text -> vectors, scale by Kafka partitions
python -m botsight_ai topics     # hourly topics and trends
python -m botsight_ai anomalies  # hourly anomaly check
python -m botsight_ai serve      # internal HTTP API: /internal/ask, /metrics
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from functools import partial
from typing import Any

import redis

from . import consumer
from .alerts import KafkaAlertSink
from .anomalies import AnomalyDetector, AnomalyScheduler
from .config import Config
from .embedder import FastEmbedder, model_dim
from .llm import build as build_llm
from .nlsql import Asker
from .server import create_app
from .storage import VectorStore, clickhouse
from .texts import TextIngestor
from .topics import TopicRunner, TopicScheduler

log = logging.getLogger("botsight_ai")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="botsight_ai")
    parser.add_argument("command", choices=["all", "embed", "topics", "anomalies", "serve"])
    args = parser.parse_args(argv)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    cfg = Config.from_env()
    rdb = redis.Redis.from_url(cfg.redis_url)
    llm = build_llm(cfg.llm)
    if not cfg.llm.enabled:
        log.warning(
            "LLM_BASE_URL/LLM_MODEL not set: topics get keyword names, "
            "anomalies template texts, natural language questions are off"
        )
    stop = threading.Event()
    threads: list[threading.Thread] = []

    def start(name: str, target: Any) -> None:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        threads.append(t)

    cmd = args.command
    if cmd in ("all", "embed"):
        embedder = FastEmbedder(cfg.embedding_model, cfg.model_cache, cfg.embed_batch)
        ingestor = TextIngestor(
            embedder,
            VectorStore(cfg.qdrant_url, embedder.dim),
            clickhouse(cfg.clickhouse),
            rdb,
            cfg.daily_text_limit,
        )
        start("embed", partial(consumer.run, cfg.kafka_brokers, cfg.kafka_group, ingestor, stop))
    if cmd in ("all", "topics"):
        runner = TopicRunner(
            cfg,
            VectorStore(cfg.qdrant_url, model_dim(cfg.embedding_model)),
            clickhouse(cfg.clickhouse),
            llm,
            KafkaAlertSink(cfg.kafka_brokers),
        )
        scheduler = TopicScheduler(runner, rdb, cfg.topics_interval)
        start("topics", partial(scheduler.loop, stop))
    if cmd in ("all", "anomalies"):
        detector = AnomalyDetector(
            clickhouse(cfg.clickhouse), llm, KafkaAlertSink(cfg.kafka_brokers), cfg.anomaly_z
        )
        start("anomalies", partial(AnomalyScheduler(detector, rdb).loop, stop))

    if cmd in ("all", "serve"):
        import uvicorn

        asker = Asker(
            llm,
            admin=lambda: clickhouse(cfg.clickhouse),
            as_user=lambda user, pw: clickhouse(cfg.clickhouse, username=user, password=pw),
            password_secret=cfg.nl_password_secret,
            database=cfg.clickhouse.database,
        )

        def ready() -> None:
            clickhouse(cfg.clickhouse).ping()
            rdb.ping()

        host, _, port = cfg.http_addr.rpartition(":")
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(asker, cfg.internal_token, ready),
                host=host or "0.0.0.0",
                port=int(port),
                log_config=None,
                access_log=False,
            )
        )
        server.run()  # returns on SIGTERM / SIGINT
    else:
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        stop.wait()

    log.info("shutting down")
    stop.set()
    for t in threads:
        t.join(timeout=30)


if __name__ == "__main__":
    main()
