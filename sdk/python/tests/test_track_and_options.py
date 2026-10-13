from __future__ import annotations

import functools
import math
import re

import pytest
from aiogram.exceptions import (
    ClientDecodeError,
    RestartingTelegram,
    TelegramBadRequest,
    TelegramEntityTooLarge,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.methods import SendMessage

import botsight
from botsight import Botsight
from botsight._names import handler_name
from botsight._uuid import uuid7
from botsight.events import normalize_error

from .conftest import API_KEY, drain


class Form(StatesGroup):
    name = State()
    group = State()


def make_client(**options: object) -> Botsight:
    return Botsight(API_KEY, bot_id=42, **options)  # type: ignore[arg-type]


def test_track_valid_event() -> None:
    client = make_client()
    client.track(
        "lab_generated",
        user_id=5,
        props={
            "lab": "lab2",
            "queue_ms": 340,
            "ratio": 0.5,
            "pro": True,
            "missing": None,
            "obj": ["a"],
            "nan": math.nan,
            "long": "x" * 5000,
        },
    )
    [ev] = drain(client)
    assert ev["type"] == "custom" and ev["name"] == "lab_generated"
    assert ev["user_id"] == 5 and ev["chat_id"] == 0 and ev["bot_id"] == 42
    assert ev["props"] == {
        "lab": "lab2",
        "queue_ms": 340,
        "ratio": 0.5,
        "pro": "true",
        "obj": "['a']",
        "long": "x" * 1024,
    }


@pytest.mark.parametrize("name", ["", "Purchase", "1st", "with space", "x" * 65, None, 42])
def test_track_invalid_name_is_dropped_without_raising(name: object) -> None:
    client = make_client()
    client.track(name, user_id=1)  # type: ignore[arg-type]
    assert drain(client) == []


def test_track_limits_props_count() -> None:
    client = make_client()
    client.track("many", props={f"k{i}": i for i in range(50)})
    [ev] = drain(client)
    assert len(ev["props"]) == 32


def test_track_bad_props_and_ids_do_not_raise() -> None:
    client = make_client()
    client.track("ok", user_id="oops", props=["not", "a", "mapping"])  # type: ignore[arg-type]
    [ev] = drain(client)
    assert ev["user_id"] == 0 and ev["props"] == {}


def test_module_level_track_without_setup_is_noop() -> None:
    botsight.track("purchase", user_id=1)


def test_queue_overflow_counts_dropped() -> None:
    client = make_client(max_queue=2)
    for _ in range(5):
        client.track("evt", user_id=1)
    assert len(drain(client)) == 2
    assert client.dropped == 3


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"batch_size": 0}, ValueError),
        ({"batch_size": 501}, ValueError),
        ({"flush_interval": 0}, ValueError),
        ({"max_queue": 0}, ValueError),
        ({"sample_rate": 1.5}, ValueError),
        ({"endpoint": "ftp://x"}, ValueError),
        ({"redact": "nope"}, TypeError),
        ({"pseudonymize_user_ids": True}, ValueError),
        ({"skip_text_in_states": [42]}, TypeError),
    ],
)
def test_invalid_options_fail_fast(options: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        make_client(**options)


def test_api_key_required() -> None:
    with pytest.raises(ValueError):
        Botsight("")


def test_skip_text_in_states_accepts_states_and_groups() -> None:
    client = make_client(skip_text_in_states=[Form, "Other:step"])
    assert client.options.skip_text_in_states == {"Form:name", "Form:group", "Other:step"}
    client = make_client(skip_text_in_states=Form.name)
    assert client.options.skip_text_in_states == {"Form:name"}


def test_endpoint_trailing_slash() -> None:
    assert (
        make_client(endpoint="http://localhost:8080/").options.endpoint == "http://localhost:8080"
    )


async def plain(x: int) -> None: ...


class ClassHandler:
    async def method(self) -> None: ...

    async def __call__(self) -> None: ...


def test_handler_name_variants() -> None:
    mod = __name__
    assert handler_name(plain) == f"{mod}:plain"
    assert handler_name(functools.partial(plain, 1)) == f"{mod}:plain"
    assert handler_name(ClassHandler().method) == f"{mod}:ClassHandler.method"
    assert handler_name(ClassHandler) == f"{mod}:ClassHandler"
    assert handler_name(ClassHandler()) == f"{mod}:ClassHandler"


def test_uuid7_format_and_order() -> None:
    a, b = uuid7(1_700_000_000_000), uuid7(1_700_000_000_001)
    pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    assert re.fullmatch(pattern, a) and re.fullmatch(pattern, b)
    assert a < b
    assert a.replace("-", "")[:12] == f"{1_700_000_000_000:012x}"


METHOD = SendMessage(chat_id=1, text="x")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (
            TelegramForbiddenError(METHOD, "Forbidden: bot was blocked by the user"),
            (403, "bot_blocked"),
        ),
        (
            TelegramForbiddenError(METHOD, "Forbidden: user is deactivated"),
            (403, "user_deactivated"),
        ),
        (
            TelegramForbiddenError(METHOD, "Forbidden: bot can't initiate conversation"),
            (403, "forbidden"),
        ),
        (TelegramRetryAfter(METHOD, "Too Many Requests", retry_after=5), (429, "rate_limited")),
        (TelegramBadRequest(METHOD, "Bad Request: message is not modified"), (400, "not_modified")),
        (TelegramBadRequest(METHOD, "Bad Request: chat not found"), (400, "bad_request")),
        (RestartingTelegram(METHOD, "restart"), (500, "server_error")),
        (TelegramEntityTooLarge(METHOD, "too large"), (413, "entity_too_large")),
        (TelegramNetworkError(METHOD, "timeout"), (0, "network")),
        (ClientDecodeError("bad json", ValueError(), "{"), (0, "decode_error")),
        (RuntimeError("other"), (0, "unknown")),
    ],
)
def test_normalize_error(exc: BaseException, expected: tuple[int, str]) -> None:
    assert normalize_error(exc) == expected
