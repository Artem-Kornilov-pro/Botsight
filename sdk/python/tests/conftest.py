from __future__ import annotations

import gzip
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pytest
from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.methods import GetMe, SendMessage, TelegramMethod
from aiogram.types import (
    CallbackQuery,
    Chat,
    ChatMemberBanned,
    ChatMemberMember,
    ChatMemberUpdated,
    Message,
    Update,
    User,
)
from aiohttp import web
from aiohttp.test_utils import TestServer

import botsight
from botsight import Botsight, transport

BOT_ID = 42
TOKEN = f"{BOT_ID}:TEST-token"
API_KEY = "bs_live_ab12cd34_secret"


class FakeSession(BaseSession):
    """Bot API session without network. ``responder(method)`` returns a result or raises."""

    def __init__(self, responder: Callable[[TelegramMethod[Any]], Any] | None = None) -> None:
        super().__init__()
        self.requests: list[TelegramMethod[Any]] = []
        self.responder = responder

    async def close(self) -> None:
        pass

    async def make_request(
        self, bot: Bot, method: TelegramMethod[Any], timeout: int | None = None
    ) -> Any:
        self.requests.append(method)
        if self.responder is not None:
            return self.responder(method)
        if isinstance(method, GetMe):
            return User(id=BOT_ID, is_bot=True, first_name="Bot", username="shop_bot")
        if isinstance(method, SendMessage):
            return Message(
                message_id=1,
                date=datetime.now(),
                chat=Chat(
                    id=method.chat_id if isinstance(method.chat_id, int) else -1, type="private"
                ),
                text=method.text,
            )
        return True

    async def stream_content(self, *args: Any, **kwargs: Any) -> AsyncIterator[bytes]:
        yield b""


def make_bot(responder: Callable[[TelegramMethod[Any]], Any] | None = None) -> Bot:
    return Bot(TOKEN, session=FakeSession(responder))


def user(
    user_id: int = 1, *, lang: str = "ru", username: str | None = "u", premium: bool | None = None
) -> User:
    return User(
        id=user_id,
        is_bot=False,
        first_name="U",
        language_code=lang,
        username=username,
        is_premium=premium,
    )


def message_update(
    text: str | None = "hi",
    *,
    user_id: int = 1,
    update_id: int = 1,
    chat_id: int | None = None,
    caption: str | None = None,
    **user_kwargs: Any,
) -> Update:
    u = user(user_id, **user_kwargs)
    chat = Chat(
        id=chat_id if chat_id is not None else user_id,
        type="private" if chat_id is None or chat_id > 0 else "supergroup",
    )
    msg = Message(
        message_id=update_id,
        date=datetime.now(),
        chat=chat,
        from_user=u,
        text=text,
        caption=caption,
    )
    return Update(update_id=update_id, message=msg)


def callback_update(data: str, *, user_id: int = 1, update_id: int = 1) -> Update:
    u = user(user_id)
    msg = Message(
        message_id=1, date=datetime.now(), chat=Chat(id=user_id, type="private"), text="menu"
    )
    cb = CallbackQuery(id=str(update_id), from_user=u, chat_instance="x", data=data, message=msg)
    return Update(update_id=update_id, callback_query=cb)


def member_update(*, kicked: bool = True, user_id: int = 1, update_id: int = 1) -> Update:
    u = user(user_id)
    bot_user = User(id=BOT_ID, is_bot=True, first_name="Bot")
    old = ChatMemberMember(user=bot_user)
    new = ChatMemberBanned(user=bot_user, until_date=0) if kicked else old
    event = ChatMemberUpdated(
        chat=Chat(id=user_id, type="private"),
        from_user=u,
        date=datetime.now(),
        old_chat_member=old,
        new_chat_member=new,
    )
    return Update(update_id=update_id, my_chat_member=event)


def drain(client: Botsight) -> list[dict[str, Any]]:
    out = []
    while not client._queue.empty():
        out.append(client._queue.get_nowait())
    return out


def of_type(evs: list[dict[str, Any]], type_: str) -> list[dict[str, Any]]:
    return [e for e in evs if e["type"] == type_]


@pytest.fixture(autouse=True)
def _reset_default_client() -> None:
    botsight._default_client = None


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transport, "BACKOFF_BASE", 0.01)
    monkeypatch.setattr(transport, "BACKOFF_CAP", 0.05)
    monkeypatch.setattr(transport, "BREAKER_PAUSE", 0.05)


@dataclass
class Collector:
    """Mock collector: records batches and answers with scripted responses."""

    url: str = ""
    batches: list[dict[str, Any]] = field(default_factory=list)
    requests: list[dict[str, Any]] = field(default_factory=list)
    script: list[tuple[int, dict[str, str]]] = field(default_factory=list)

    def respond(self, *statuses: int | tuple[int, dict[str, str]]) -> None:
        for s in statuses:
            self.script.append(s if isinstance(s, tuple) else (s, {}))

    @property
    def events(self) -> list[dict[str, Any]]:
        return [e for b in self.accepted for e in b["events"]]

    @property
    def accepted(self) -> list[dict[str, Any]]:
        return [r["batch"] for r in self.requests if r["status"] == 202]


@pytest.fixture
async def collector() -> AsyncIterator[Collector]:
    state = Collector()

    async def handle(request: web.Request) -> web.Response:
        # aiohttp transparently decompresses Content-Encoding: gzip
        raw = await request.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        body = json.loads(raw)
        status, headers = state.script.pop(0) if state.script else (202, {})
        state.requests.append(
            {
                "status": status,
                "headers": dict(request.headers),
                "batch": body,
                "size": len(raw),
            }
        )
        state.batches.append(body)
        if status == 202:
            return web.json_response(
                {"accepted": len(body["events"]), "duplicate": False}, status=202
            )
        return web.Response(status=status, headers=headers)

    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.router.add_post("/v1/batch", handle)
    server = TestServer(app)
    await server.start_server()
    state.url = str(server.make_url("")).rstrip("/")
    try:
        yield state
    finally:
        await server.close()
