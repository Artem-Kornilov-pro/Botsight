"""Hot-path overhead per update (section 8.8): target < 50 us.

Measures the SDK hooks on a real dispatcher against the same dispatcher without the SDK.
Run with ``pytest -m benchmark -s``.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

from botsight import setup

from .conftest import API_KEY, drain, make_bot, message_update

N = 3000
TARGET_US = 50.0


def build(with_sdk: bool) -> tuple[Dispatcher, Any, Any]:
    dp = Dispatcher()
    bot = make_bot()

    @dp.message(CommandStart())
    async def start(message: Message) -> None:
        pass

    client = setup(dp, bot, api_key=API_KEY, max_queue=N * 4) if with_sdk else None
    return dp, bot, client


async def per_update_us(dp: Dispatcher, bot: Any, client: Any) -> float:
    updates = [message_update("/start ch_tech", update_id=i, user_id=i) for i in range(1, N + 1)]
    for u in updates[:200]:  # warm-up
        await dp.feed_update(bot, u)
    best = float("inf")
    for _ in range(3):
        if client is not None:
            drain(client)
        started = time.perf_counter()
        for u in updates:
            await dp.feed_update(bot, u)
        best = min(best, (time.perf_counter() - started) / N * 1e6)
    return best


@pytest.mark.benchmark
async def test_update_overhead() -> None:
    base = await per_update_us(*build(with_sdk=False))
    with_sdk = await per_update_us(*build(with_sdk=True))
    overhead = with_sdk - base
    print(
        f"\nper update: base {base:.1f} us, with SDK {with_sdk:.1f} us, "
        f"overhead {overhead:.1f} us (target < {TARGET_US} us)"
    )
    assert overhead < TARGET_US
