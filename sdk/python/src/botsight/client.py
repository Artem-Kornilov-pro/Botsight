from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from aiogram.fsm.state import State, StatesGroup

from . import events
from ._log import log_limited, logger
from .options import Options
from .transport import Sender

_SAMPLE_SPACE = 1 << 64


class Botsight:
    """Botsight client: an in-memory event queue plus a background sender.

    Usually created by :func:`botsight.setup`. Every public method is safe to call
    from bot code: it never raises and never blocks.
    """

    def __init__(
        self,
        api_key: str,
        *,
        bot_id: int = 0,
        endpoint: str = "https://ingest.botsight.ru",
        capture_text: bool = False,
        skip_text_in_states: Iterable[str | State | type[StatesGroup]] = (),
        redact: Callable[[str], str] | None = None,
        pseudonymize_user_ids: bool = False,
        pseudonymize_secret: str | bytes | None = None,
        track_fsm: bool = True,
        track_blocks: bool = True,
        fallback_handlers: Iterable[str | Callable[..., Any]] = (),
        batch_size: int = 500,
        flush_interval: float = 2.0,
        max_queue: int = 10_000,
        sample_rate: float = 1.0,
        enabled: bool = True,
    ) -> None:
        self.options = Options.build(
            endpoint=endpoint,
            capture_text=capture_text,
            skip_text_in_states=skip_text_in_states,
            redact=redact,
            pseudonymize_user_ids=pseudonymize_user_ids,
            pseudonymize_secret=pseudonymize_secret,
            track_fsm=track_fsm,
            track_blocks=track_blocks,
            fallback_handlers=fallback_handlers,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue=max_queue,
            sample_rate=sample_rate,
            enabled=enabled,
        )
        if self.options.enabled and (not isinstance(api_key, str) or not api_key):
            raise ValueError("botsight: api_key is required (get it in the dashboard)")
        self.bot_id = bot_id
        self._api_key = api_key
        self._queue: asyncio.Queue[events.Event] = asyncio.Queue(maxsize=self.options.max_queue)
        self._dropped = 0
        self._active = self.options.enabled
        self._sender = Sender(self)
        self._sample_threshold = int(self.options.sample_rate * _SAMPLE_SPACE)

    # --- public API -------------------------------------------------------------------

    def track(
        self,
        name: str,
        user_id: int | None = None,
        props: Mapping[str, Any] | None = None,
        *,
        chat_id: int | None = None,
    ) -> None:
        """Record a business event, e.g. ``track("purchase", user_id, {"amount": 990})``.

        ``name`` is snake_case up to 64 chars, up to 32 props, strings up to 1024 chars.
        Invalid input is logged and dropped, never raised.
        """
        try:
            if not self._active:
                return
            uid = _as_id(user_id)
            cid = _as_id(chat_id)
            if not self._sampled(uid or cid):
                return
            ev = events.custom_event(
                name=name, bot_id=self.bot_id, user_id=uid, chat_id=cid, props=props
            )
            if ev is not None:
                self._emit(ev)
        except Exception:
            log_limited("track", "botsight: track() failed", exc_info=True)

    @property
    def dropped(self) -> int:
        """Events lost so far and not yet reported to the server."""
        return self._dropped

    async def start(self) -> None:
        """Start background delivery. Registered on ``dp.startup`` by ``setup()``."""
        if not self._active:
            return
        try:
            self._emit(events.lifecycle_event("bot_started", self.bot_id))
            self._sender.start()
        except Exception:
            log_limited("start", "botsight: failed to start", exc_info=True)

    async def close(self) -> None:
        """Flush the queue (up to 5 s) and stop. Registered on ``dp.shutdown``."""
        try:
            if self._active:
                self._emit(events.lifecycle_event("bot_stopped", self.bot_id))
            await self._sender.close()
        except Exception:
            log_limited("close", "botsight: failed to close", exc_info=True)

    # --- internal, used by middlewares and the sender ---------------------------------

    def _emit(self, ev: events.Event) -> None:
        try:
            self._queue.put_nowait(ev)
        except asyncio.QueueFull:
            self._dropped += 1

    def _sampled(self, key: int) -> bool:
        """Sampling by a stable hash of the user, so funnels stay whole."""
        if self._sample_threshold >= _SAMPLE_SPACE or key == 0:
            return True
        digest = hashlib.blake2b(key.to_bytes(8, "big", signed=True), digest_size=8).digest()
        return int.from_bytes(digest, "big") < self._sample_threshold

    def _take_dropped(self) -> int:
        dropped, self._dropped = self._dropped, 0
        return dropped

    def _add_dropped(self, count: int) -> None:
        self._dropped += count

    def _deactivate(self, reason: str) -> None:
        if self._active:
            self._active = False
            logger.log(logging.ERROR, "botsight: delivery stopped until restart: %s", reason)


def _as_id(value: Any) -> int:
    """Accept an int id or an object with ``.id`` (``User``, ``Chat``)."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    inner = getattr(value, "id", None)
    if isinstance(inner, int) and not isinstance(inner, bool):
        return inner
    return 0
