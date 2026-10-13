"""Unit tests: no Docker, no model download."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import numpy as np
import pytest
from conftest import FakeLLM

from botsight_ai import clustering
from botsight_ai.anomalies import METRICS, AnomalyDetector, Finding, robust_z
from botsight_ai.config import LLMConfig
from botsight_ai.llm import LLMError, OpenAICompatibleLLM, parse_json_object
from botsight_ai.nlsql import NLSQLError, validate
from botsight_ai.normalize import normalize, point_id
from botsight_ai.server import create_app

# --- normalization ---------------------------------------------------------------------


def test_normalize_masks_personal_data_and_numbers() -> None:
    assert normalize("  Мой номер +7 (999) 123-45-67,   почта ivan@mail.ru ") == (
        "мой номер <phone> , почта <email>"
    )
    assert normalize("Код 4821 не пришёл https://t.me/x") == "код <num> не пришёл <url>"
    assert normalize("   ") == ""


def test_point_id_is_per_project_and_text() -> None:
    a = point_id(1, "не приходит код")
    assert a == point_id(1, "не приходит код")
    assert a != point_id(2, "не приходит код")
    assert a != point_id(1, "не приходит смс")


# --- clustering ------------------------------------------------------------------------


def blobs(
    k: int, per: int, dim: int = 32, spread: float = 0.08, seed: int = 0
) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    centers = rng.normal(size=(k, dim))
    x = np.concatenate(
        [c + rng.normal(scale=spread * np.linalg.norm(c), size=(per, dim)) for c in centers]
    )
    x /= np.linalg.norm(x, axis=1, keepdims=True)
    return x.astype(np.float32), np.repeat(np.arange(k), per)


def test_cluster_finds_topics_and_noise() -> None:
    x, y = blobs(3, 40)
    rng = np.random.default_rng(1)
    noise = rng.normal(size=(10, 32)).astype(np.float32)
    noise /= np.linalg.norm(noise, axis=1, keepdims=True)
    labels = clustering.cluster(np.concatenate([x, noise]), min_cluster_size=8)
    found = labels[:120]
    assert len(set(found.tolist()) - {-1}) == 3
    for t in range(3):  # each true topic maps to one cluster
        assert len(set(found[y == t].tolist()) - {-1}) == 1
    # random vectors are nearly equidistant from everything, most of them stay noise
    assert (labels[120:] == -1).sum() >= 6


def test_cluster_too_few_points_is_all_noise() -> None:
    x, _ = blobs(1, 5)
    assert (clustering.cluster(x, min_cluster_size=8) == -1).all()


def test_match_topics_keeps_ids_and_numbers_new_ones() -> None:
    x, y = blobs(3, 30)
    old = clustering.centroids(x, y)  # labels 0..2 as topic ids 10..12
    old_topics = {10 + k: v for k, v in old.items()}
    x2, y2 = blobs(4, 30)  # same seed: first 3 centers equal, 4th is new
    new = clustering.centroids(x2, y2)
    mapping = clustering.match_topics(new, old_topics, next_id=13)
    assert mapping[0] == 10 and mapping[1] == 11 and mapping[2] == 12
    assert mapping[3] == 13


def test_assign_uses_threshold() -> None:
    x, y = blobs(2, 20)
    cents = {7: clustering.centroids(x, y)[0], 9: clustering.centroids(x, y)[1]}
    assigned = clustering.assign(x, cents, threshold=0.5)
    assert (assigned[y == 0] == 7).all() and (assigned[y == 1] == 9).all()
    far = -x[:1]
    assert clustering.assign(far, cents, threshold=0.5)[0] == 0


def test_keywords_prefer_distinctive_words() -> None:
    kw = clustering.keywords(
        {
            1: ["не приходит код подтверждения", "код не приходит", "где код подтверждения"],
            2: ["не проходит оплата картой", "оплата не проходит", "карта отклонена оплата"],
        }
    )
    assert kw[1][0] == "код" and "подтверждения" in kw[1]
    assert kw[2][0] == "оплата" and "приходит" not in kw[2]


# --- LLM -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reply",
    [
        '{"name": "Код"}',
        'Вот ответ:\n```json\n{"name": "Код"}\n```',
        'Конечно! {"name": "Код"} — готово',
    ],
)
def test_parse_json_object(reply: str) -> None:
    assert parse_json_object(reply) == {"name": "Код"}


def test_parse_json_object_without_json() -> None:
    with pytest.raises(LLMError):
        parse_json_object("не знаю")


def test_openai_compatible_llm() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "привет"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 3},
            },
        )

    llm = OpenAICompatibleLLM(
        LLMConfig(base_url="http://llm", api_key="k", model="m"),
        transport=httpx.MockTransport(handler),
    )
    out = llm.complete("sys", "user", purpose="test")
    assert out.text == "привет" and out.prompt_tokens == 12 and out.completion_tokens == 3
    assert seen["auth"] == "Bearer k"
    assert seen["body"]["model"] == "m"  # type: ignore[index]

    failing = OpenAICompatibleLLM(
        LLMConfig(base_url="http://llm", model="m"),
        transport=httpx.MockTransport(lambda r: httpx.Response(500)),
    )
    with pytest.raises(LLMError):
        failing.complete("s", "u", purpose="test")


# --- NL->SQL validation ----------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM events",
        "SELECT toDate(ts) AS day, uniq(user_id) AS users FROM events WHERE type = 'update' GROUP BY day ORDER BY day;",
        "WITH firsts AS (SELECT user_id, argMin(start_param, ts) AS source FROM events WHERE command = 'start' GROUP BY user_id) SELECT source, count() FROM firsts GROUP BY source",
        "SELECT count() FROM botsight.events WHERE props_num['amount'] > 100",
        "SELECT 1 UNION ALL SELECT count() FROM events",
    ],
)
def test_validate_accepts(sql: str) -> None:
    assert validate(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "SELECT 1; DROP TABLE events",
        "INSERT INTO events SELECT * FROM events",
        "DROP TABLE events",
        "SELECT * FROM url('http://evil/x', CSV, 'a String')",
        "SELECT * FROM remote('127.0.0.1', botsight.events)",
        "SELECT count() FROM (SELECT * FROM numbers(10))",
        "SELECT file('/etc/passwd')",
        "SELECT count() FROM events SETTINGS max_execution_time = 1000",
        "SELECT 1 FORMAT TabSeparated",
        "SELECT 1 INTO OUTFILE 'x'",
        "SELECT * FROM system.users",
        "SELECT * FROM events JOIN messages_text USING event_id",
        "SELECT * FROM other_db.events",
        "SELECT getSetting('readonly')",
        "SELECT " + "1 + " * 2000 + "1",
    ],
)
def test_validate_rejects(sql: str) -> None:
    with pytest.raises(NLSQLError) as err:
        validate(sql)
    assert err.value.code == "invalid_sql"


# --- anomalies ---------------------------------------------------------------------------


def test_robust_z() -> None:
    z, med = robust_z(120, [10, 12, 9, 11])
    assert med == 10.5 and z > 20
    z, _ = robust_z(11, [10, 12, 9, 11])
    assert abs(z) < 1
    z, med = robust_z(0, [0, 0, 0, 0])  # no division by zero
    assert z == 0 and med == 0


def test_template_and_title() -> None:
    errors = next(m for m in METRICS if m.name == "errors")
    f = Finding(
        1,
        errors,
        datetime(2026, 10, 13, 11, tzinfo=UTC),
        current=120,
        usual=10,
        z=30,
        segments=[
            {
                "segment": "хендлер bot:cb_pay: KeyError",
                "current": 110,
                "usual": 1,
                "share_of_change": 0.99,
            }
        ],
        deploys=["13:20 МСК"],
    )
    text = AnomalyDetector.template(f)
    assert "14:00 МСК" in text and "120" in text and "bot:cb_pay" in text and "13:20 МСК" in text
    assert AnomalyDetector.title(f) == "Ошибки: рост в 12 раз"

    users = next(m for m in METRICS if m.name == "active_users")
    drop = Finding(1, users, f.hour, current=40, usual=100, z=-6)
    assert AnomalyDetector.title(drop) == "Активные пользователи: на 60% меньше обычного"


# --- internal API -------------------------------------------------------------------------


class StubAsker:
    def ask(self, project_id: int, question: str) -> dict[str, object]:
        if question == "boom":
            raise NLSQLError("invalid_sql", "nope")
        return {"project_id": project_id, "question": question}


def test_server_checks_token_and_maps_errors() -> None:
    from fastapi.testclient import TestClient

    app = create_app(StubAsker(), "secret", ready=lambda: None)  # type: ignore[arg-type]
    c = TestClient(app)
    body = {"project_id": 3, "question": "сколько?"}
    assert c.post("/internal/ask", json=body).status_code == 401
    assert (
        c.post("/internal/ask", json=body, headers={"X-Internal-Token": "wrong"}).status_code == 401
    )
    ok = c.post("/internal/ask", json=body, headers={"X-Internal-Token": "secret"})
    assert ok.status_code == 200 and ok.json()["project_id"] == 3
    bad = c.post(
        "/internal/ask", json={**body, "question": "boom"}, headers={"X-Internal-Token": "secret"}
    )
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_sql"
    assert (
        c.post(
            "/internal/ask",
            json={"project_id": 0, "question": "x"},
            headers={"X-Internal-Token": "secret"},
        ).status_code
        == 422
    )
    assert c.get("/healthz").status_code == 200 and c.get("/readyz").status_code == 200
    assert "botsight_ai_ask_total" in c.get("/metrics").text or c.get("/metrics").status_code == 200

    no_token = TestClient(create_app(StubAsker(), "", ready=lambda: None))  # type: ignore[arg-type]
    assert no_token.post("/internal/ask", json=body).status_code == 401  # unset token locks the API


def test_fake_llm_records_calls() -> None:
    llm = FakeLLM('{"a": 1}')
    assert llm.complete("s", "u", purpose="p").text == '{"a": 1}'
    assert llm.calls == [("p", "s", "u")]
