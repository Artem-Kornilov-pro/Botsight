from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
from pathlib import Path
from typing import Any

import pytest
from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from botsight import Botsight, __version__, setup, transport

from .conftest import API_KEY, BOT_ID, Collector, make_bot, message_update

TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
UUID7_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
# Shared with the collector tests, section 9.5 of Botsight.md
FIXTURE = Path(__file__).resolve().parents[3] / "docs" / "contract" / "batch_v1.json"


class Form(StatesGroup):
    name = State()


def client_for(collector: Collector, **options: Any) -> Botsight:
    options.setdefault("flush_interval", 0.05)
    return Botsight(API_KEY, bot_id=BOT_ID, endpoint=collector.url, **options)


async def run(client: Botsight, n: int, **track_kwargs: Any) -> None:
    await client.start()
    for i in range(n):
        client.track("evt", user_id=i + 1, **track_kwargs)
    await client.close()


async def test_end_to_end_batch_matches_contract(collector: Collector) -> None:
    dp = Dispatcher()
    bot = make_bot()

    @dp.message(CommandStart())
    async def start(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.name)
        await message.answer("hi")

    client = setup(dp, bot, api_key=API_KEY, endpoint=collector.url, flush_interval=0.05)
    await dp.emit_startup()
    await dp.feed_update(bot, message_update("/start ch_tech"))
    client.track("purchase", user_id=1, props={"amount": 990, "plan": "pro"})
    await dp.emit_shutdown()

    [request] = collector.requests
    headers = request["headers"]
    assert headers["Authorization"] == f"Bearer {API_KEY}"
    assert headers["Content-Encoding"] == "gzip"
    assert headers["Content-Type"] == "application/json"

    batch = request["batch"]
    assert batch["schema_version"] == 1
    assert UUID7_RE.fullmatch(batch["batch_id"])
    assert TS_RE.fullmatch(batch["sent_at"])
    assert batch["sdk"] == {
        "name": "botsight-python",
        "version": __version__,
        "framework": "aiogram",
        "framework_version": batch["sdk"]["framework_version"],
    }
    assert batch["dropped"] == 0

    kinds = [(e["type"], e["name"]) for e in batch["events"]]
    assert kinds == [
        ("lifecycle", "bot_started"),
        ("outgoing", "sendMessage"),
        ("step", "message"),
        ("update", "message"),
        ("custom", "purchase"),
        ("lifecycle", "bot_stopped"),
    ]
    ids = [e["event_id"] for e in batch["events"]]
    assert len(set(ids)) == len(ids) and all(UUID7_RE.fullmatch(i) for i in ids)
    assert all(TS_RE.fullmatch(e["ts"]) for e in batch["events"])

    # Every event carries exactly the keys of the shared contract fixture.
    fixture = json.loads(FIXTURE.read_text())
    expected_keys = {(e["type"], e["name"]): set(e) for e in fixture["events"]}
    for ev in batch["events"]:
        key = (ev["type"], ev["name"])
        if key in expected_keys:
            assert set(ev) == expected_keys[key], key
    assert set(batch) == set(fixture)


async def test_retry_resends_the_same_batch(collector: Collector) -> None:
    collector.respond(503, 409)
    client = client_for(collector)
    await client.start()
    client.track("evt", user_id=1)
    await asyncio.sleep(0.5)
    await client.close()
    first, second, third = collector.requests[:3]
    assert [r["status"] for r in collector.requests[:3]] == [503, 409, 202]
    assert first["batch"] == second["batch"] == third["batch"]


async def test_retry_after_is_respected(collector: Collector) -> None:
    collector.respond((429, {"Retry-After": "0"}))
    client = client_for(collector)
    await client.start()
    client.track("evt", user_id=1)
    await asyncio.sleep(0.3)
    await client.close()
    assert [r["status"] for r in collector.requests][:2] == [429, 202]


async def test_413_splits_batch(collector: Collector) -> None:
    collector.respond(413)
    client = client_for(collector, flush_interval=0.5)
    await run(client, 10)
    statuses = [r["status"] for r in collector.requests]
    assert statuses[0] == 413
    sizes = [len(r["batch"]["events"]) for r in collector.requests[1:3]]
    assert sum(sizes) == len(collector.requests[0]["batch"]["events"])
    assert len({r["batch"]["batch_id"] for r in collector.requests}) == len(collector.requests)
    names = [e["name"] for e in collector.events]
    assert names.count("evt") == 10


async def test_400_drops_batch_and_reports_it(collector: Collector) -> None:
    collector.respond(400)
    client = client_for(collector)
    await client.start()
    client.track("evt", user_id=1)
    await asyncio.sleep(0.3)
    client.track("evt", user_id=2)
    await client.close()
    lost = collector.requests[0]["batch"]
    later = collector.accepted
    assert later and later[0]["dropped"] == len(lost["events"]) + lost["dropped"]


async def test_401_stops_sdk(collector: Collector) -> None:
    collector.respond(401)
    client = client_for(collector)
    await client.start()
    client.track("evt", user_id=1)
    await asyncio.sleep(0.3)
    client.track("evt", user_id=2)
    await client.close()
    assert [r["status"] for r in collector.requests] == [401]
    assert client._queue.empty()


async def test_queue_overflow_reported_in_batch(collector: Collector) -> None:
    client = client_for(collector, max_queue=3)
    for i in range(10):
        client.track("evt", user_id=i + 1)  # sender not started yet
    assert client.dropped == 7
    await client.start()
    await client.close()
    # bot_started/bot_stopped may also hit the full queue; nothing is lost silently
    reported = sum(b["dropped"] for b in collector.accepted)
    assert reported + len(collector.events) == 10 + 2
    assert client.dropped == 0


async def test_batch_size_limit(collector: Collector) -> None:
    client = client_for(collector, batch_size=4, flush_interval=0.5)
    await run(client, 10)
    assert all(len(b["events"]) <= 4 for b in collector.accepted)
    assert sum(1 for e in collector.events if e["name"] == "evt") == 10


async def test_batches_stay_under_byte_limit(collector: Collector) -> None:
    client = client_for(collector, flush_interval=0.5)
    await run(client, 200, props={f"k{i}": "я" * 1024 for i in range(7)})
    assert len(collector.accepted) >= 2
    assert all(r["size"] <= 2 * 1024 * 1024 for r in collector.requests)
    assert sum(1 for e in collector.events if e["name"] == "evt") == 200


async def test_oversized_event_text_is_truncated(collector: Collector) -> None:
    dp = Dispatcher()
    bot = make_bot()

    @dp.message()
    async def any_message(message: Message) -> None:
        pass

    setup(dp, bot, api_key=API_KEY, endpoint=collector.url, capture_text=True, flush_interval=0.05)
    await dp.emit_startup()
    await dp.feed_update(bot, message_update("😀" * 4096))
    await dp.emit_shutdown()
    [update] = [e for e in collector.events if e["type"] == "update"]
    size = len(json.dumps(update, ensure_ascii=False, separators=(",", ":")).encode())
    assert size <= transport.MAX_EVENT_BYTES
    assert 0 < len(update["text"]) < 4096
    assert update["text_len"] == 4096


async def test_oversized_custom_event_is_dropped(collector: Collector) -> None:
    client = client_for(collector)
    await client.start()
    client.track("huge", user_id=1, props={f"k{i}": "я" * 1024 for i in range(32)})
    client.track("small", user_id=1)
    await client.close()
    names = [e["name"] for e in collector.events]
    assert "huge" not in names and "small" in names
    assert sum(b["dropped"] for b in collector.accepted) == 1


async def test_pseudonymization(collector: Collector) -> None:
    secret = "project-secret"
    client = client_for(collector, pseudonymize_user_ids=True, pseudonymize_secret=secret)
    await client.start()
    client.track("evt", user_id=123456789, chat_id=123456789)
    client.track("grp", user_id=123456789, chat_id=-100500)
    await client.close()
    digest = hmac.new(secret.encode(), b"123456789", hashlib.sha256).digest()
    expected = int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF
    evt, grp = [e for e in collector.events if e["type"] == "custom"]
    assert evt["user_id"] == expected and evt["chat_id"] == expected
    assert grp["user_id"] == expected and grp["chat_id"] == -100500


async def test_redact_applied(collector: Collector) -> None:
    dp = Dispatcher()
    bot = make_bot()
    setup(
        dp,
        bot,
        api_key=API_KEY,
        endpoint=collector.url,
        capture_text=True,
        redact=lambda t: re.sub(r"\d", "#", t),
        flush_interval=0.05,
    )
    await dp.emit_startup()
    await dp.feed_update(bot, message_update("code 1234"))
    await dp.emit_shutdown()
    [update] = [e for e in collector.events if e["type"] == "update"]
    assert update["text"] == "code ####"


async def test_restart_after_close(collector: Collector) -> None:
    client = client_for(collector)
    await run(client, 1)
    await run(client, 1)
    assert sum(1 for e in collector.events if e["name"] == "evt") == 2


@pytest.mark.parametrize("value", ["5", "0.5", "abc", None, "-3", "100000"])
def test_retry_after_parsing(value: str | None) -> None:
    parsed = transport._retry_after(value)
    if value in ("abc", None):
        assert parsed is None
    else:
        assert parsed is not None and 0 <= parsed <= transport.MAX_RETRY_AFTER
