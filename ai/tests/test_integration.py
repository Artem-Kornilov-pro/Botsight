"""Integration tests on real ClickHouse, Qdrant and Redis (testcontainers)."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from conftest import EVENT_COLUMNS, FakeLLM, event_row, new_uuid, ts

from botsight_ai.alerts import ListAlertSink
from botsight_ai.anomalies import AnomalyDetector
from botsight_ai.config import Config
from botsight_ai.llm import DisabledLLM
from botsight_ai.nlsql import Asker, NLSQLError
from botsight_ai.storage import VectorStore, clickhouse
from botsight_ai.texts import TextIngestor, TextRecord
from botsight_ai.topics import TopicRunner

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC).replace(microsecond=0)

VOCAB = {
    "code": ["код", "подтверждения", "смс", "приходит", "получил", "сообщение", "номер", "жду"],
    "pay": ["оплата", "карта", "платеж", "списались", "подписка", "деньги", "отклонена", "чек"],
    "lab": ["лаба", "лабораторная", "физика", "вариант", "добавьте", "химия", "методичка", "сдать"],
}


def texts_of(topic: str, n: int, seed: int) -> list[str]:
    rnd = random.Random(seed)
    out: set[str] = set()
    while len(out) < n:
        out.add(" ".join(rnd.sample(VOCAB[topic], 4)))
    return sorted(out)


def store_texts(
    ch: Any, ingestor: TextIngestor, project_id: int, items: list[tuple[str, datetime]]
) -> None:
    """Messages as they would arrive: a row in messages_text (ch-writer) and a record
    through the ingestor (messages.text consumer)."""
    records, rows = [], []
    for text, at in items:
        eid = new_uuid()
        records.append(TextRecord(project_id, eid, ts(at), text))
        rows.append([project_id, 77, eid, at, 1, "", 1, text])
    ch.insert(
        "messages_text",
        rows,
        column_names=[
            "project_id",
            "bot_id",
            "event_id",
            "ts",
            "user_id",
            "state",
            "handled",
            "text",
        ],
    )
    ingestor.process(records)


def test_texts_deduplicate_and_limit(
    cfg: Config, ch: Any, rdb: Any, embedder: Any, vectors: VectorStore, project_id: int
) -> None:
    ingestor = TextIngestor(embedder, vectors, ch, rdb, daily_limit=1000)
    at = NOW - timedelta(hours=1)
    store_texts(
        ch,
        ingestor,
        project_id,
        [("Не приходит код 1234!", at)] * 3 + [("не   приходит КОД 9999!", at)],
    )
    # four messages, one normalized text, one vector
    assert vectors.count(project_id) == 1
    n = ch.query(
        "SELECT count(), uniqExact(point_id) FROM message_points FINAL WHERE project_id = {p:UInt32}",
        parameters={"p": project_id},
    ).result_rows[0]
    assert n == (4, 1)
    # another project with the same text gets its own vector
    store_texts(ch, ingestor, project_id + 100_000, [("не приходит код", at)])
    assert vectors.count(project_id) == 1 and vectors.count(project_id + 100_000) == 1

    limited = TextIngestor(embedder, vectors, ch, rdb, daily_limit=10)
    other = project_id + 200_000
    store_texts(
        ch, limited, other, [(f"текст номер {chr(1072 + i % 32)}{i}", at) for i in range(200)]
    )
    kept = ch.query(
        "SELECT count() FROM message_points WHERE project_id = {p:UInt32}", parameters={"p": other}
    ).result_rows[0][0]
    assert 10 < kept < 60  # 10 in full, then about one in ten


def runner(cfg: Config, vectors: VectorStore, llm: Any = None) -> tuple[TopicRunner, ListAlertSink]:
    sink = ListAlertSink()
    return TopicRunner(cfg, vectors, clickhouse(cfg.clickhouse), llm or DisabledLLM(), sink), sink


def test_topics_trends_and_stable_ids(
    cfg: Config, ch: Any, rdb: Any, embedder: Any, vectors: VectorStore, project_id: int
) -> None:
    ingestor = TextIngestor(embedder, vectors, ch, rdb, daily_limit=100_000)
    this_week = lambda i: NOW - timedelta(hours=1 + i % 100)  # noqa: E731
    prev_week = lambda i: NOW - timedelta(days=8, hours=i % 100)  # noqa: E731
    code, pay, lab = texts_of("code", 40, 1), texts_of("pay", 40, 2), texts_of("lab", 40, 3)
    items = (
        [(t, this_week(i)) for i, t in enumerate(code)]
        + [(t, prev_week(i)) for i, t in enumerate(code)]  # the same complaints a week earlier
        + [(t, this_week(i)) for i, t in enumerate(pay)]  # payments: new this week
        + [(t, prev_week(i)) for i, t in enumerate(pay[:3])]
        + [(t, prev_week(i)) for i, t in enumerate(lab)]  # labs: only last week
    )
    store_texts(ch, ingestor, project_id, items)

    run, sink = runner(cfg, vectors)
    result = run.run(project_id, ts(NOW))
    assert result.status == "done"
    by_word = {}
    for t in result.topics:
        for word in ("код", "оплата", "платеж", "карта", "подтверждения", "смс"):
            if word in t.keywords:
                by_word.setdefault(word, t)
    code_topic = next(t for t in result.topics if set(t.keywords) & set(VOCAB["code"]))
    pay_topic = next(t for t in result.topics if set(t.keywords) & set(VOCAB["pay"]))
    assert code_topic.topic_id != pay_topic.topic_id
    assert code_topic.named_by == "keywords" and code_topic.name
    assert code_topic.messages_7d >= 30 and code_topic.messages_prev_7d >= 30
    assert pay_topic.messages_7d >= 30 and pay_topic.growth >= 3

    trend = [a for a in sink.alerts if a.kind == "topic_trend"]
    assert [a.details["topic_id"] for a in trend] == [pay_topic.topic_id], trend
    assert "pay" not in trend[0].title and pay_topic.name in trend[0].title

    stored = ch.query(
        "SELECT topic_id, name, messages_7d, length(example_event_ids) FROM topics FINAL "
        "WHERE project_id = {p:UInt32} AND active = 1",
        parameters={"p": project_id},
    ).result_rows
    assert {r[0] for r in stored} == {t.topic_id for t in result.topics}
    assert all(r[3] > 0 for r in stored)

    # an hour later the same data keeps the same topic ids
    again = run.run(project_id, ts(NOW) + 3_600_000)
    assert {t.topic_id for t in again.topics} == {t.topic_id for t in result.topics}
    # and the same week's trend alert has the same id (replaced, not duplicated)
    assert [a.alert_id for a in sink.alerts if a.kind == "topic_trend"][-1] == trend[0].alert_id


def test_topics_named_by_llm(
    cfg: Config, ch: Any, rdb: Any, embedder: Any, vectors: VectorStore, project_id: int
) -> None:
    ingestor = TextIngestor(embedder, vectors, ch, rdb, daily_limit=100_000)
    store_texts(
        ch,
        ingestor,
        project_id,
        [(t, NOW - timedelta(hours=2)) for t in texts_of("code", 40, 5) + texts_of("pay", 40, 6)],
    )
    llm = FakeLLM(
        lambda system, user: json.dumps(
            {
                "name": "Не приходит код" if "код" in user else "Проблемы с оплатой",
                "description": "Пользователи пишут об этом.",
            },
            ensure_ascii=False,
        )
    )
    run, _ = runner(cfg, vectors, llm)
    result = run.run(project_id, ts(NOW))
    names = {t.name for t in result.topics}
    assert {"Не приходит код", "Проблемы с оплатой"} <= names
    assert all(t.named_by == "llm" for t in result.topics)
    # the LLM saw only example texts of the topic, not other projects' data
    assert all(purpose == "topic_name" for purpose, _, _ in llm.calls)
    # named topics are not renamed on the next run
    calls = len(llm.calls)
    run.run(project_id, ts(NOW) + 3_600_000)
    assert len(llm.calls) == calls


def test_too_few_texts(
    cfg: Config, ch: Any, rdb: Any, embedder: Any, vectors: VectorStore, project_id: int
) -> None:
    ingestor = TextIngestor(embedder, vectors, ch, rdb, daily_limit=100)
    store_texts(ch, ingestor, project_id, [("привет", NOW)])
    run, _ = runner(cfg, vectors)
    assert run.run(project_id, ts(NOW)).status == "too_few"


# --- natural language questions ---------------------------------------------------------


def asker(cfg: Config, llm: Any) -> Asker:
    return Asker(
        llm,
        admin=lambda: clickhouse(cfg.clickhouse),
        as_user=lambda u, p: clickhouse(cfg.clickhouse, username=u, password=p),
        password_secret=cfg.nl_password_secret,
    )


def seed_events(ch: Any, project_id: int, n: int, command: str) -> None:
    at = NOW - timedelta(hours=3)
    ch.insert(
        "events",
        [event_row(project_id, at, user_id=i + 1, command=command) for i in range(n)],
        column_names=EVENT_COLUMNS,
    )


def test_ask_sees_only_own_project(cfg: Config, ch: Any, project_id: int) -> None:
    mine, other = project_id, project_id + 300_000
    seed_events(ch, mine, 5, "start_mine")
    seed_events(ch, other, 7, "secret_other")

    sql = {"q": ""}
    llm = FakeLLM(lambda s, u: json.dumps({"sql": sql["q"], "chart": "number", "title": "Ответ"}))
    a = asker(cfg, llm)

    sql["q"] = "SELECT count() AS n FROM events"
    res = a.ask(mine, "сколько событий?")
    assert res["rows"] == [[5]] and res["columns"] == ["n"] and res["chart"] == "number"
    # the dictionary in the prompt comes from this project only
    prompt = llm.calls[-1][2]
    assert "start_mine" in prompt and "secret_other" not in prompt

    # an LLM asking for another project gets nothing: the row policy filters rows
    sql["q"] = f"SELECT count() FROM events WHERE project_id = {other}"
    assert a.ask(mine, "а сколько у соседа?")["rows"] == [[0]]
    sql["q"] = "SELECT project_id, count() FROM events GROUP BY project_id"
    assert a.ask(mine, "по проектам")["rows"] == [[mine, 5]]

    # the service user is not limited by the policy
    total = ch.query(
        "SELECT count() FROM events WHERE project_id IN ({a:UInt32}, {b:UInt32})",
        parameters={"a": mine, "b": other},
    ).result_rows[0][0]
    assert total == 12


@pytest.mark.parametrize(
    "bad_sql",
    [
        "SELECT * FROM url('http://example.com', CSV, 'a String')",
        "SELECT count() FROM events SETTINGS max_execution_time = 1000",
        "SELECT text FROM messages_text",
        "DROP TABLE events",
    ],
)
def test_ask_rejects_unsafe_sql(cfg: Config, project_id: int, bad_sql: str) -> None:
    llm = FakeLLM(json.dumps({"sql": bad_sql, "chart": "table"}))
    with pytest.raises(NLSQLError) as err:
        asker(cfg, llm).ask(project_id, "что-нибудь")
    assert err.value.code == "invalid_sql"


def test_nl_user_is_read_only_even_without_validation(cfg: Config, project_id: int) -> None:
    """The second line of defence: what the validator might miss, ClickHouse refuses."""
    a = asker(cfg, FakeLLM("{}"))
    client = a._client(project_id)
    for sql in [
        "SELECT count() FROM messages_text",  # no grant
        "SELECT count() FROM events SETTINGS readonly = 0",  # readonly profile
        "SELECT * FROM url('http://example.com/x', CSV, 'a String')",  # no source grants
        "INSERT INTO events (project_id) VALUES (1)",
    ]:
        with pytest.raises(Exception):  # noqa: B017 - any ClickHouse refusal
            client.query(sql)


def test_ask_without_llm(cfg: Config, project_id: int) -> None:
    with pytest.raises(NLSQLError) as err:
        asker(cfg, DisabledLLM()).ask(project_id, "сколько?")
    assert err.value.code == "llm_disabled"


def test_ask_reports_query_errors(cfg: Config, project_id: int) -> None:
    llm = FakeLLM(json.dumps({"sql": "SELECT no_such_column FROM events"}))
    with pytest.raises(NLSQLError) as err:
        asker(cfg, llm).ask(project_id, "что-нибудь")
    assert err.value.code == "query_failed"


# --- anomalies ---------------------------------------------------------------------------


def seed_hour(
    ch: Any, project_id: int, hour: datetime, users: int, ok_errors: int, pay_errors: int = 0
) -> None:
    rows = [
        event_row(project_id, hour + timedelta(minutes=i % 60), user_id=i + 1) for i in range(users)
    ]
    rows += [
        event_row(
            project_id,
            hour + timedelta(minutes=i % 60),
            type="step",
            handler="bot:cb_ok",
            error="ValueError",
        )
        for i in range(ok_errors)
    ]
    rows += [
        event_row(
            project_id,
            hour + timedelta(minutes=i % 60),
            type="step",
            handler="bot:cb_pay",
            error="KeyError",
        )
        for i in range(pay_errors)
    ]
    ch.insert("events", rows, column_names=EVENT_COLUMNS)


def test_anomaly_found_and_explained(cfg: Config, ch: Any, project_id: int) -> None:
    hour = (NOW - timedelta(hours=2)).replace(minute=0, second=0)
    broken, healthy = project_id, project_id + 400_000
    for p in (broken, healthy):
        for week in range(1, 5):
            seed_hour(ch, p, hour - timedelta(days=7 * week), users=40, ok_errors=10 + week)
    seed_hour(ch, broken, hour, users=40, ok_errors=11, pay_errors=110)
    seed_hour(ch, healthy, hour, users=41, ok_errors=12)
    ch.insert(
        "events",
        [
            event_row(
                broken,
                hour - timedelta(minutes=40),
                type="lifecycle",
                name="bot_started",
                user_id=0,
            )
        ],
        column_names=EVENT_COLUMNS,
    )

    sink = ListAlertSink()
    findings = AnomalyDetector(clickhouse(cfg.clickhouse), DisabledLLM(), sink, cfg.anomaly_z).run(
        hour
    )
    mine = [f for f in findings if f.project_id in (broken, healthy)]
    assert [(f.project_id, f.metric.name) for f in mine] == [(broken, "errors")]
    f = mine[0]
    assert f.current == 121 and 11 <= f.usual <= 13
    assert (
        f.segments[0]["segment"] == "хендлер bot:cb_pay: KeyError"
        and f.segments[0]["share_of_change"] > 0.9
    )
    assert len(f.deploys) == 1 and f.deploys[0].endswith("МСК")

    alert = next(a for a in sink.alerts if a.project_id == broken)
    assert alert.kind == "anomaly" and alert.title.startswith("Ошибки: рост")
    assert "bot:cb_pay" in alert.text and "перезапускался" in alert.text

    # with an LLM the facts go to it as JSON and its sentences become the text
    llm = FakeLLM("Ошибки выросли после перезапуска бота, почти все — в cb_pay.")
    sink2 = ListAlertSink()
    AnomalyDetector(clickhouse(cfg.clickhouse), llm, sink2, cfg.anomaly_z).run(hour)
    facts = json.loads(next(u for p, _, u in llm.calls if p == "anomaly"))
    assert facts["current"] == 121 and facts["top_segments"][0]["segment"].endswith(
        "cb_pay: KeyError"
    )
    assert next(a for a in sink2.alerts if a.project_id == broken).text.startswith(
        "Ошибки выросли после"
    )


def test_new_project_without_history_is_skipped(cfg: Config, ch: Any, project_id: int) -> None:
    hour = (NOW - timedelta(hours=2)).replace(minute=0, second=0)
    seed_hour(ch, project_id, hour, users=5, ok_errors=0, pay_errors=500)
    findings = AnomalyDetector(
        clickhouse(cfg.clickhouse), DisabledLLM(), ListAlertSink(), cfg.anomaly_z
    ).run(hour)
    assert project_id not in {f.project_id for f in findings}
