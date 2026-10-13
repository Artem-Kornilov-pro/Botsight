"""Event builders (section 9).

Hot-path builders return plain dicts with ``ts`` as a float. ``event_id``, the RFC 3339
timestamp, pseudonymization and ``redact`` are applied later, off the hot path, in
:mod:`botsight.transport`.
"""

from __future__ import annotations

import math
import re
import sys
import time
from collections.abc import Mapping
from enum import Enum
from typing import Any

from aiogram import __version__ as aiogram_version
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramAPIError,
    TelegramBadRequest,
    TelegramConflictError,
    TelegramEntityTooLarge,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiogram.types import CallbackQuery, Chat, ChatMemberUpdated, Message, User

from ._log import log_limited
from ._version import __version__
from .options import Options

Event = dict[str, Any]

MAX_START_PARAM = 64
MAX_TEXT = 4096
MAX_CUSTOM_NAME = 64
MAX_PROPS = 32
MAX_PROP_KEY = 64
MAX_PROP_STR = 1024

_CUSTOM_NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,63}")

SDK_INFO = {
    "name": "botsight-python",
    "version": __version__,
    "framework": "aiogram",
    "framework_version": aiogram_version,
}


def _str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _base(type_: str, name: str, bot_id: int, user_id: int, chat_id: int) -> Event:
    return {
        "type": type_,
        "name": name,
        "ts": time.time(),
        "bot_id": bot_id,
        "user_id": user_id,
        "chat_id": chat_id,
    }


def update_event(
    *,
    name: str,
    event: Any,
    bot_id: int,
    user: User | None,
    chat: Chat | None,
    state: str | None,
    handled: bool,
    duration_ms: int,
    options: Options,
) -> Event:
    ev = _base("update", name, bot_id, user.id if user else 0, chat.id if chat else 0)
    ev["chat_type"] = _str(chat.type) if chat else ""
    ev["state"] = state or ""
    ev["text_len"] = 0

    if isinstance(event, Message):
        ev["content_type"] = _str(event.content_type)
        text = event.text or event.caption
        if text:
            ev["text_len"] = len(text)
            if event.text and text.startswith("/"):
                _parse_command(text, ev)
            if options.capture_text and ev["state"] not in options.skip_text_in_states:
                ev["text"] = text[:MAX_TEXT]
    elif isinstance(event, CallbackQuery):
        if event.data:
            ev["callback_data"] = event.data
    elif isinstance(event, ChatMemberUpdated):
        # "kicked" in a private chat means the user blocked the bot (section 8.6)
        ev["member_status"] = _str(event.new_chat_member.status)

    if user is not None:
        ev["user"] = {
            "lang": user.language_code or "",
            "is_premium": bool(user.is_premium),
            "has_username": bool(user.username),
        }
    ev["handled"] = handled
    ev["duration_ms"] = duration_ms
    return ev


def _parse_command(text: str, ev: Event) -> None:
    parts = text.split(maxsplit=1)
    command = parts[0][1:].split("@", 1)[0].lower()
    if not command:
        return
    ev["command"] = command[:64]
    if command == "start" and len(parts) > 1:
        ev["start_param"] = parts[1].strip()[:MAX_START_PARAM]


def step_event(
    *,
    name: str,
    bot_id: int,
    user_id: int,
    chat_id: int,
    handler: str,
    state_from: str | None,
    state_to: str | None,
    duration_ms: int,
    error: str | None,
) -> Event:
    ev = _base("step", name, bot_id, user_id, chat_id)
    ev["handler"] = handler
    ev["state_from"] = state_from or ""
    ev["state_to"] = state_to or ""
    ev["duration_ms"] = duration_ms
    ev["error"] = error
    return ev


def outgoing_event(
    *,
    name: str,
    bot_id: int,
    chat_id: int,
    duration_ms: int,
    exc: BaseException | None,
) -> Event:
    ev = _base("outgoing", name, bot_id, 0, chat_id)
    if exc is None:
        ev["ok"] = True
        ev["error_code"] = 0
        ev["error"] = None
    else:
        code, error = normalize_error(exc)
        ev["ok"] = False
        ev["error_code"] = code
        ev["error"] = error
    ev["duration_ms"] = duration_ms
    return ev


def normalize_error(exc: BaseException) -> tuple[int, str]:
    """Map an exception from a Bot API call to ``(code, error)``, table in section 8.5."""
    description = (getattr(exc, "message", None) or str(exc)).lower()
    if isinstance(exc, TelegramForbiddenError):
        if "bot was blocked by the user" in description:
            return 403, "bot_blocked"
        if "user is deactivated" in description:
            return 403, "user_deactivated"
        return 403, "forbidden"
    if isinstance(exc, TelegramRetryAfter):
        return 429, "rate_limited"
    if isinstance(exc, TelegramMigrateToChat):
        return 400, "migrated"
    if isinstance(exc, TelegramBadRequest):
        if "message is not modified" in description:
            return 400, "not_modified"
        return 400, "bad_request"
    if isinstance(exc, TelegramUnauthorizedError):
        return 401, "unauthorized"
    if isinstance(exc, TelegramNotFound):
        return 404, "not_found"
    if isinstance(exc, TelegramConflictError):
        return 409, "conflict"
    if isinstance(exc, TelegramEntityTooLarge):  # subclass of TelegramNetworkError
        return 413, "entity_too_large"
    if isinstance(exc, TelegramServerError):
        return 500, "server_error"
    if isinstance(exc, TelegramNetworkError):
        return 0, "network"
    if isinstance(exc, ClientDecodeError):
        return 0, "decode_error"
    if isinstance(exc, TelegramAPIError):
        return 0, "api_error"
    return 0, "unknown"


def custom_event(
    *,
    name: str,
    bot_id: int,
    user_id: int,
    chat_id: int,
    props: Mapping[str, Any] | None,
) -> Event | None:
    """Manual event (section 8.7). Returns ``None`` if the name is invalid."""
    if not isinstance(name, str) or not _CUSTOM_NAME_RE.fullmatch(name):
        log_limited(
            "track-name",
            "botsight: track() name must be snake_case up to %d chars, got %r; event dropped",
            MAX_CUSTOM_NAME,
            name,
        )
        return None
    ev = _base("custom", name, bot_id, user_id, chat_id)
    ev["props"] = clean_props(props)
    return ev


def clean_props(props: Mapping[str, Any] | None) -> dict[str, str | int | float]:
    """Numbers stay numbers (sums and percentiles), everything else becomes a string."""
    if props is None:
        return {}
    if not isinstance(props, Mapping):
        log_limited("track-props", "botsight: track() props must be a mapping; props dropped")
        return {}
    out: dict[str, str | int | float] = {}
    for key, value in props.items():
        if len(out) >= MAX_PROPS:
            log_limited(
                "track-props-count", "botsight: track() keeps only the first %d props", MAX_PROPS
            )
            break
        if not isinstance(key, str) or not key or len(key) > MAX_PROP_KEY or value is None:
            continue
        if isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif isinstance(value, int):
            out[key] = value
        elif isinstance(value, float):
            if math.isfinite(value):
                out[key] = value
        else:
            out[key] = (value if isinstance(value, str) else _str(value))[:MAX_PROP_STR]
    return out


def lifecycle_event(name: str, bot_id: int) -> Event:
    ev = _base("lifecycle", name, bot_id, 0, 0)
    ev["props"] = {
        "sdk_version": __version__,
        "framework": "aiogram",
        "framework_version": aiogram_version,
        "python_version": "{}.{}.{}".format(*sys.version_info[:3]),
    }
    return ev
