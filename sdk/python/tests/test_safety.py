"""The SDK must never break the bot (section 6, principle 2)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from botsight import events, setup

from .conftest import API_KEY, drain, make_bot, message_update, of_type


class Form(StatesGroup):
    name = State()


def bot_with_handler(storage: Any = None, **options: Any) -> tuple[Dispatcher, Any, Any, list]:
    dp = Dispatcher(storage=storage or MemoryStorage())
    bot = make_bot()
    calls: list[str] = []

    @dp.message(CommandStart())
    async def start(message: Message, state: FSMContext) -> None:
        calls.append("start")
        await state.set_state(Form.name)
        await message.answer("hi")

    client = setup(dp, bot, api_key=API_KEY, **options)
    return dp, bot, client, calls


async def test_broken_event_builders_do_not_affect_the_bot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(**kwargs: Any) -> Any:
        raise RuntimeError("sdk bug")

    monkeypatch.setattr(events, "update_event", broken)
    monkeypatch.setattr(events, "step_event", broken)
    monkeypatch.setattr(events, "outgoing_event", broken)
    dp, bot, client, calls = bot_with_handler()
    await dp.feed_update(bot, message_update("/start"))
    assert calls == ["start"]
    assert drain(client) == []


class FlakyStorage(MemoryStorage):
    """get_state works before the handler and fails afterwards."""

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    async def get_state(self, key: StorageKey) -> str | None:
        self.reads += 1
        if self.reads > 1:
            raise ConnectionError("redis is down")
        return await super().get_state(key)


async def test_fsm_storage_failure_after_handler() -> None:
    dp, bot, client, calls = bot_with_handler(storage=FlakyStorage())
    await dp.feed_update(bot, message_update("/start"))
    assert calls == ["start"]
    [step] = of_type(drain(client), "step")
    # no fake "stuck on step" when the state could not be read
    assert step["state_from"] == "" and step["state_to"] == ""


async def test_full_queue_does_not_block_or_raise() -> None:
    dp, bot, client, calls = bot_with_handler(max_queue=1)
    for i in range(1, 51):
        await dp.feed_update(bot, message_update("/start", update_id=i, user_id=i))
    assert len(calls) == 50
    assert client._queue.qsize() == 1
    assert client.dropped > 0


async def test_collector_unreachable_bot_keeps_working() -> None:
    # nothing listens on port 9 (discard); connection is refused or times out
    dp, bot, client, calls = bot_with_handler(endpoint="http://127.0.0.1:9", flush_interval=0.05)
    await client.start()
    for i in range(1, 21):
        await dp.feed_update(bot, message_update("/start", update_id=i, user_id=i))
    await asyncio.sleep(0.2)
    started = time.monotonic()
    await client.close()
    assert time.monotonic() - started < 6
    assert len(calls) == 20


async def test_collector_returns_garbage(collector: Any) -> None:
    collector.respond((200, {}), (500, {}), (418, {}))
    dp, bot, client, calls = bot_with_handler(endpoint=collector.url, flush_interval=0.05)
    await client.start()
    await dp.feed_update(bot, message_update("/start"))
    await asyncio.sleep(0.3)
    await client.close()
    assert calls == ["start"]


async def test_redact_failure_drops_only_the_text(collector: Any) -> None:
    def redact(text: str) -> str:
        raise RuntimeError("bad redact")

    dp, bot, client, _calls = bot_with_handler(
        endpoint=collector.url, capture_text=True, redact=redact
    )
    await client.start()
    await dp.feed_update(bot, message_update("/start"))
    await client.close()
    [update] = [e for e in collector.events if e["type"] == "update"]
    assert "text" not in update
    assert update["text_len"] == len("/start")


async def test_start_and_close_never_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    _dp, _bot, client, _ = bot_with_handler()

    def boom() -> None:
        raise RuntimeError("cannot start")

    monkeypatch.setattr(client._sender, "start", boom)
    await client.start()
    await client.close()
