"""Kafka consumer of messages.text. Instances share the topic's partitions in one consumer
group, so embedding scales with the number of partitions (Botsight.md, 11.1)."""

from __future__ import annotations

import logging
import threading

from confluent_kafka import Consumer, KafkaError

from .texts import TextIngestor, TextRecord

log = logging.getLogger(__name__)

TOPIC = "messages.text"


def run(brokers: str, group: str, ingestor: TextIngestor, stop: threading.Event) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group,
            "enable.auto.commit": False,  # commit only after vectors and points are stored
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 600_000,
        }
    )
    consumer.subscribe([TOPIC])
    log.info("consuming %s as %s", TOPIC, group)
    try:
        while not stop.is_set():
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            if not msgs:
                continue
            records: list[TextRecord] = []
            for m in msgs:
                err = m.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        log.warning("kafka error: %s", err)
                    continue
                rec = TextRecord.from_kafka(m.value() or b"")
                if rec is None:
                    log.warning("skipping malformed text record at offset %s", m.offset())
                    continue
                records.append(rec)
            while not stop.is_set():
                try:
                    ingestor.process(records)
                    break
                except Exception:
                    log.exception("failed to process %d texts, retrying", len(records))
                    stop.wait(5)
            if stop.is_set():
                break  # not committed: the next owner reads these records again
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
