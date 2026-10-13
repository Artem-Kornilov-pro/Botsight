"""aiogram hooks (sections 8.3-8.6).

Rule for every hook: the bot's own call runs exactly as without the SDK, the bot's
exceptions pass through untouched, and any failure of the SDK itself is logged and
swallowed.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from aiogram import Bot, Router
from aiogram.client.session.middlewares.base import (
    BaseRequestMiddleware,
    NextRequestMiddlewareType,
)
from aiogram.dispatcher.event.bases import UNHANDLED, CancelHandler, SkipHandler
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.methods import GetUpdates, TelegramMethod
from aiogram.methods.base import Response, TelegramType
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message, PreCheckoutQuery, Update

from . import events
from ._log import log_limited
from ._names import handler_name

if TYPE_CHECKING:
    from aiogram.fsm.context import FSMContext

    from .client import Botsight

CONTEXT_KEY = "botsight_update_ctx"

_STEP_NAMES: dict[type, str] = {
    Message: "message",
    CallbackQuery: "callback_query",
    PreCheckoutQuery: "pre_checkout_query",
}


class UpdateContext:
    """Shared between the outer update hook and inner hooks of the same update."""

    __slots__ = ("fallback", "sdk_handled")

    def __init__(self) -> None:
        self.fallback = False
        self.sdk_handled = False


def _ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


class UpdateMiddleware(BaseMiddleware):
    """Outer middleware on ``dp.update``: every incoming update, before filters."""

    def __init__(self, client: Botsight) -> None:
        self._client = client

    async def __call__(
        self,
        handler: Callable[[Any, dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        if not self._client._active:
            return await handler(event, data)
        ctx = UpdateContext()
        data[CONTEXT_KEY] = ctx
        started = time.perf_counter()
        result: Any = UNHANDLED
        failed = False
        try:
            result = await handler(event, data)
            return result
        except BaseException:
            failed = True  # a handler was found but crashed
            raise
        finally:
            try:
                self._record(event, data, ctx, failed or result is not UNHANDLED, _ms(started))
            except Exception:
                log_limited("hook-update", "botsight: failed to record update", exc_info=True)

    def _record(
        self,
        update: Update,
        data: dict[str, Any],
        ctx: UpdateContext,
        handled: bool,
        duration_ms: int,
    ) -> None:
        client = self._client
        user = data.get("event_from_user")
        chat = data.get("event_chat")
        if not client._sampled(user.id if user else chat.id if chat else 0):
            return
        bot = data.get("bot")
        client._emit(
            events.update_event(
                name=update.event_type,
                event=update.event,
                bot_id=bot.id if bot is not None else client.bot_id,
                user=user,
                chat=chat,
                state=data.get("raw_state"),
                handled=(handled or ctx.sdk_handled) and not ctx.fallback,
                duration_ms=duration_ms,
                options=client.options,
            )
        )


class StepMiddleware(BaseMiddleware):
    """Inner middleware on message/callback_query/pre_checkout_query: runs after filters,
    so it knows which handler fired and how the FSM state changed."""

    def __init__(self, client: Botsight) -> None:
        self._client = client

    async def __call__(
        self,
        handler: Callable[[Any, dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        if not self._client._active:
            return await handler(event, data)
        before = data.get("raw_state")  # already read by aiogram, free
        started = time.perf_counter()
        error: str | None = None
        skipped = False
        try:
            return await handler(event, data)
        except (SkipHandler, CancelHandler):
            skipped = True  # the handler declined the event: not a step
            raise
        except Exception as exc:
            error = type(exc).__name__
            raise  # never swallow the bot's exception
        finally:
            if not skipped:
                try:
                    await self._record(event, data, before, _ms(started), error)
                except Exception:
                    log_limited("hook-step", "botsight: failed to record step", exc_info=True)

    async def _record(
        self,
        event: Any,
        data: dict[str, Any],
        before: str | None,
        duration_ms: int,
        error: str | None,
    ) -> None:
        client = self._client
        handler_obj = data.get("handler")
        name = handler_name(handler_obj.callback) if handler_obj is not None else ""
        if name in client.options.fallback_handlers:
            ctx = data.get(CONTEXT_KEY)
            if isinstance(ctx, UpdateContext):
                ctx.fallback = True

        user = data.get("event_from_user")
        chat = data.get("event_chat")
        user_id = user.id if user else 0
        chat_id = chat.id if chat else 0
        if not client._sampled(user_id or chat_id):
            return

        state_from: str | None = None
        state_to: str | None = None
        if client.options.track_fsm:
            state_from = before
            fsm: FSMContext | None = data.get("state")
            if fsm is None:
                state_to = before
            else:
                # One extra storage read per handled event (section 8.4); disable with
                # track_fsm=False. If it fails, report no FSM data rather than a fake
                # "stuck on step" (state_from == state_to).
                try:
                    state_to = await fsm.get_state()
                except Exception:
                    log_limited("fsm-read", "botsight: failed to read FSM state", exc_info=True)
                    state_from = None

        bot = data.get("bot")
        client._emit(
            events.step_event(
                name=_STEP_NAMES.get(type(event), type(event).__name__.lower()),
                bot_id=bot.id if bot is not None else client.bot_id,
                user_id=user_id,
                chat_id=chat_id,
                handler=name,
                state_from=state_from,
                state_to=state_to,
                duration_ms=duration_ms,
                error=error,
            )
        )


class OutgoingMiddleware(BaseRequestMiddleware):
    """Request middleware of the bot session: every Bot API call except getUpdates.
    Reads only the method name and ``chat_id``, never message or file contents."""

    def __init__(self, client: Botsight) -> None:
        self._client = client

    async def __call__(
        self,
        make_request: NextRequestMiddlewareType[TelegramType],
        bot: Bot,
        method: TelegramMethod[TelegramType],
    ) -> Response[TelegramType]:
        if not self._client._active or isinstance(method, GetUpdates):
            return await make_request(bot, method)
        started = time.perf_counter()
        try:
            response = await make_request(bot, method)
        except Exception as exc:
            self._record(bot, method, _ms(started), exc)
            raise
        self._record(bot, method, _ms(started), None)
        return response

    def _record(
        self, bot: Bot, method: TelegramMethod[Any], duration_ms: int, exc: BaseException | None
    ) -> None:
        try:
            client = self._client
            chat_id = getattr(method, "chat_id", None)
            # chat_id may be "@channel"; the schema field is int64
            if not isinstance(chat_id, int) or isinstance(chat_id, bool):
                chat_id = 0
            if not client._sampled(chat_id):
                return
            client._emit(
                events.outgoing_event(
                    name=method.__api_method__,
                    bot_id=bot.id,
                    chat_id=chat_id,
                    duration_ms=duration_ms,
                    exc=exc,
                )
            )
        except Exception:
            log_limited("hook-outgoing", "botsight: failed to record request", exc_info=True)


def blocks_router() -> Router:
    """Subscribes the bot to ``my_chat_member`` (section 8.6).

    aiogram requests from Telegram only update types that have handlers, so an empty
    handler is enough; the update itself is recorded by :class:`UpdateMiddleware`.
    The handler raises ``SkipHandler``, so it never shadows the bot's own
    ``my_chat_member`` handlers, whatever the router order.
    """
    router = Router(name="botsight")

    @router.my_chat_member()
    async def botsight_my_chat_member(
        event: ChatMemberUpdated, botsight_update_ctx: UpdateContext | None = None
    ) -> None:
        if botsight_update_ctx is not None:
            botsight_update_ctx.sdk_handled = True
        raise SkipHandler

    return router
