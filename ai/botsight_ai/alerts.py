"""Alerts go to the Kafka topic alerts (Botsight.md, 11.1); ch-writer stores them in
ClickHouse, a notifier will deliver them to Telegram later."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from . import metrics

_NAMESPACE = uuid.UUID("0d6b8f0a-3c1e-4a8b-9b7f-2e5d1c4a9f60")


@dataclass(frozen=True)
class Alert:
    project_id: int
    kind: str  # topic_trend | anomaly
    title: str
    text: str
    details: dict[str, Any] = field(default_factory=dict)
    # Same key -> same alert_id: a repeated run replaces the alert instead of adding one.
    key: str = ""
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def alert_id(self) -> str:
        return str(uuid.uuid5(_NAMESPACE, f"{self.project_id}:{self.kind}:{self.key}"))

    def to_record(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("key")
        d["alert_id"] = self.alert_id
        d["details"] = json.dumps(self.details, ensure_ascii=False)
        return d


class AlertSink(Protocol):
    def send(self, alert: Alert) -> None: ...


class KafkaAlertSink:
    def __init__(self, brokers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all"}
        )

    def send(self, alert: Alert) -> None:
        self._producer.produce(
            "alerts",
            key=str(alert.project_id).encode(),
            value=json.dumps(alert.to_record(), ensure_ascii=False).encode(),
        )
        remaining = self._producer.flush(10)
        if remaining:
            raise RuntimeError("alert was not delivered to Kafka")
        metrics.alerts.labels(alert.kind).inc()


class ListAlertSink:
    """For tests."""

    def __init__(self) -> None:
        self.alerts: list[Alert] = []

    def send(self, alert: Alert) -> None:
        self.alerts.append(alert)
        metrics.alerts.labels(alert.kind).inc()
