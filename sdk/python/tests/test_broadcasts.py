from __future__ import annotations

import asyncio
import gzip
import json
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from aiogram import Dispatcher
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.methods import SendMessage, TelegramMethod
from aiohttp import web
from aiohttp.test_utils import TestServer

from botsight import Botsight, broadcast, setup
from botsight.broadcast import TokenBucket

from .conftest import API_KEY, make_bot


@dataclass
class Server:
    """Fake collector with the broadcast endpoints and /v1/batch."""

    url: str = ""
    leases: list[Any] = field(default_factory=list)  # answers to lease: dict, 204 or 401
    reports: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)

    @property
    def results(self) -> dict[int, tuple[str, str]]:
        return {
            r["chat_id"]: (r["status"], r["error"]) for rep in self.reports for r in rep["results"]
        }


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    state = Server()

    async def lease(request: web.Request) -> web.Response:
        state.keys.append(request.headers.get("Authorization", ""))
        answer = state.leases.pop(0) if state.leases else 204
        if isinstance(answer, int):
            return web.Response(status=answer)
        return web.json_response(answer)

    async def report(request: web.Request) -> web.Response:
        state.reports.append(await request.json())
        return web.json_response({"updated": 1})

    async def batch(request: web.Request) -> web.Response:
        raw = await request.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        state.events.extend(json.loads(raw)["events"])
        return web.json_response({"accepted": 1}, status=202)

    app = web.Application()
    app.router.add_post("/v1/broadcasts/lease", lease)
    app.router.add_post("/v1/broadcasts/report", report)
    app.router.add_post("/v1/batch", batch)
    srv = TestServer(app)
    await srv.start_server()
    state.url = str(srv.make_url("")).rstrip("/")
    try:
        yield state
    finally:
        await srv.close()


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(broadcast, "LEASE_MARGIN", 0.0)
    monkeypatch.setattr(broadcast, "ERROR_PAUSE", 0.05)


def lease(chats: list[int], seconds: float = 60, text: str = "Скидка <b>20%</b>") -> dict[str, Any]:
    return {
        "broadcast_id": 7,
        "text": text,
        "parse_mode": "HTML",
        "chat_ids": chats,
        "lease_seconds": seconds,
    }


class Telegram:
    """Bot API answers: chat 3 blocked the bot, chat 4 does not exist, chat 5 hits 429 once."""

    def __init__(self) -> None:
        self.sent: list[SendMessage] = []
        self.limited = False

    def __call__(self, method: TelegramMethod[Any]) -> Any:
        assert isinstance(method, SendMessage)
        if method.chat_id == 3:
            raise TelegramForbiddenError(method, "Forbidden: bot was blocked by the user")
        if method.chat_id == 4:
            raise TelegramBadRequest(method, "Bad Request: chat not found")
        if method.chat_id == 5 and not self.limited:
            self.limited = True
            raise TelegramRetryAfter(method, "Too Many Requests: retry after 1", retry_after=0)
        self.sent.append(method)
        return True


async def wait_for(cond: Any, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.01)


def start_bot(server: Server, tg: Telegram, **options: Any) -> Botsight:
    options.setdefault("broadcast_rate", 30)
    options.setdefault("broadcast_poll_interval", 1)
    bot = make_bot(tg)
    return setup(
        Dispatcher(),
        bot,
        api_key=API_KEY,
        endpoint=server.url,
        broadcasts=True,
        flush_interval=0.05,
        **options,
    )


async def test_broadcast_is_sent_and_reported(server: Server) -> None:
    server.leases = [lease([1, 2, 3, 4, 5, 6])]
    tg = Telegram()
    client = start_bot(server, tg)
    await client.start()
    await wait_for(lambda: len(server.results) == 6)
    await client.close()

    assert server.results == {
        1: ("sent", ""),
        2: ("sent", ""),
        3: ("blocked", "bot_blocked"),
        4: ("failed", "bad_request"),
        5: ("sent", ""),
        6: ("sent", ""),
    }
    assert [m.chat_id for m in tg.sent] == [1, 2, 5, 6]  # 5 retried after 429
    assert tg.sent[0].text == "Скидка <b>20%</b>" and tg.sent[0].parse_mode == "HTML"
    assert server.keys[0] == f"Bearer {API_KEY}"
    received = [e for e in server.events if e["name"] == "broadcast_received"]
    assert sorted(e["user_id"] for e in received) == [1, 2, 5, 6]
    assert received[0]["props"] == {"broadcast_id": 7}
    # the sends themselves are outgoing events of the bot, blocks included
    blocked = [
        e for e in server.events if e["type"] == "outgoing" and e.get("error") == "bot_blocked"
    ]
    assert [e["chat_id"] for e in blocked] == [3]


async def test_work_stops_before_the_lease_ends(server: Server) -> None:
    server.leases = [lease(list(range(10, 60)), seconds=0.4)]
    tg = Telegram()
    client = start_bot(server, tg, broadcast_rate=10)
    await client.start()
    await wait_for(lambda: server.reports)
    await asyncio.sleep(0.3)
    await client.close()
    reported = server.results
    # about 4 messages fit into 0.4 s at 10 a second; the rest goes back to the queue
    assert 2 <= len(reported) <= 6
    assert set(reported) == {m.chat_id for m in tg.sent}


async def test_shutdown_reports_what_was_sent(server: Server) -> None:
    server.leases = [lease(list(range(100, 200)))]
    tg = Telegram()
    client = start_bot(server, tg, broadcast_rate=20)
    await client.start()
    await wait_for(lambda: len(tg.sent) >= 3)
    await client.close()  # mid-lease
    assert set(server.results) == {m.chat_id for m in tg.sent}
    assert len(tg.sent) < 100


async def test_invalid_key_stops_broadcasts(server: Server) -> None:
    server.leases = [401, lease([1])]
    client = start_bot(server, Telegram())
    await client.start()
    await asyncio.sleep(0.2)
    assert client._broadcaster is not None and client._broadcaster._task is not None
    assert client._broadcaster._task.done()
    await client.close()
    assert len(server.keys) == 1


async def test_token_bucket_paces_and_pauses() -> None:
    bucket = TokenBucket(100)
    started = time.monotonic()
    for _ in range(21):
        await bucket.take()
    assert 0.18 <= time.monotonic() - started < 0.6
    bucket.pause(0.2)
    paused = time.monotonic()
    await bucket.take()
    assert time.monotonic() - paused >= 0.19


def test_options() -> None:
    with pytest.raises(ValueError, match="broadcast_rate"):
        Botsight(API_KEY, broadcast_rate=31)
    with pytest.raises(ValueError, match="pseudonymize"):
        Botsight(API_KEY, broadcasts=True, pseudonymize_user_ids=True, pseudonymize_secret="s")
    client = setup(Dispatcher(), make_bot(), api_key=API_KEY)
    assert client._broadcaster is None  # off by default
