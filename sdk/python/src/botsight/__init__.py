"""Botsight: analytics for Telegram bots in two lines of code.

from botsight import setup

dp.include_router(router)
setup(dp, bot, api_key=os.environ["BOTSIGHT_KEY"])
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from aiogram import Bot, Dispatcher

from ._log import logger
from ._version import __version__
from .client import Botsight
from .middlewares import OutgoingMiddleware, StepMiddleware, UpdateMiddleware, blocks_router

__all__ = ["Botsight", "__version__", "setup", "track"]

_default_client: Botsight | None = None


def setup(dp: Dispatcher, bot: Bot, api_key: str, **options: Any) -> Botsight:
    """Connect Botsight to an aiogram 3 dispatcher. Call it before ``start_polling``.

    Options are listed in section 8.9 of the docs and in :class:`botsight.Botsight`.
    Invalid options raise here, at startup; after that the SDK never raises into bot code.
    """
    global _default_client

    existing = dp.workflow_data.get("botsight")
    if isinstance(existing, Botsight):
        logger.warning("botsight: setup() called twice for one dispatcher, reusing the client")
        return existing

    client = Botsight(api_key, bot_id=bot.id, **options)
    dp["botsight"] = client  # handlers get it by declaring a `botsight` argument
    _default_client = client
    if not client.options.enabled:
        return client

    dp.update.outer_middleware(UpdateMiddleware(client))
    steps = StepMiddleware(client)
    dp.message.middleware(steps)
    dp.callback_query.middleware(steps)
    dp.pre_checkout_query.middleware(steps)
    bot.session.middleware(OutgoingMiddleware(client))
    if client.options.track_blocks:
        dp.include_router(blocks_router())

    dp.startup.register(client.start)
    dp.shutdown.register(client.close)
    return client


def track(
    name: str,
    user_id: int | None = None,
    props: Mapping[str, Any] | None = None,
    *,
    chat_id: int | None = None,
) -> None:
    """``Botsight.track`` on the client created by the last ``setup()`` call.

    Does nothing if ``setup()`` was not called. Never raises.
    """
    if _default_client is not None:
        _default_client.track(name, user_id, props, chat_id=chat_id)
