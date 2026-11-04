"""Broadcasts to a segment (section 22): the dashboard picks the users, the SDK sends.

The bot token never leaves the bot: the collector only gives out recipients. A background
task leases a slice of them, sends the message within the Telegram limits and reports
every result, so the dashboard shows who got it, who blocked the bot and what failed.

Rules that keep a user from getting the message twice:

* a lease belongs to one bot instance; other replicas get other recipients;
* work stops before the lease runs out, unreported recipients go back to the queue;
* on shutdown the task finishes the current message and reports what it sent.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from typing import TYPE_CHECKING, Any

import aiohttp
from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)

from ._log import log_limited, logger
from .events import SDK_INFO, normalize_error

if TYPE_CHECKING:
    from .client import Botsight

LEASE_PATH = "/v1/broadcasts/lease"
REPORT_PATH = "/v1/broadcasts/report"
LEASE_SIZE = 100
REPORT_EVERY = 50
# stop this long before the lease ends: the report must reach the server in time
LEASE_MARGIN = 15.0
SEND_ATTEMPTS = 3
REQUEST_TIMEOUT = 10.0
SHUTDOWN_TIMEOUT = 5.0
ERROR_PAUSE = 10.0


class TokenBucket:
    """Paces sends to ``rate`` a second. A 429 from Telegram pauses the whole bucket:
    the limit belongs to the bot, not to one chat."""

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self._tokens = 1.0
        self._updated = time.monotonic()
        self._paused_until = 0.0

    async def take(self) -> None:
        while True:
            now = time.monotonic()
            if now < self._paused_until:
                await asyncio.sleep(self._paused_until - now)
                continue
            self._tokens = min(1.0, self._tokens + (now - self._updated) * self.rate)
            self._updated = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            await asyncio.sleep((1.0 - self._tokens) / self.rate)

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, time.monotonic() + seconds)
        self._tokens = 0.0


class Broadcaster:
    def __init__(self, client: Botsight, bot: Bot) -> None:
        self._client = client
        self._bot = bot
        self._options = client.options
        self._bucket = TokenBucket(self._options.broadcast_rate)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._session: aiohttp.ClientSession | None = None

    # --- lifecycle --------------------------------------------------------------------

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(self._run(), name="botsight-broadcasts")

    async def close(self) -> None:
        task = self._task
        if task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(task, SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning("botsight: broadcast task did not stop in time")
        except Exception:
            log_limited("broadcast-close", "botsight: broadcast task failed", exc_info=True)
        finally:
            self._task = None
            if self._session is not None:
                await self._session.close()
                self._session = None

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                lease = await self._lease()
            except _Stop:
                return
            except Exception:
                log_limited(
                    "broadcast-lease", "botsight: cannot lease broadcast recipients", exc_info=True
                )
                await self._sleep(ERROR_PAUSE)
                continue
            if lease is None:
                # jitter: replicas restarted together should not poll in step
                await self._sleep(self._options.broadcast_poll_interval * random.uniform(0.8, 1.2))
                continue
            try:
                await self._process(lease)
            except Exception:
                log_limited("broadcast", "botsight: broadcast failed", exc_info=True)
                await self._sleep(ERROR_PAUSE)

    # --- one lease --------------------------------------------------------------------

    async def _process(self, lease: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(lease["lease_seconds"]) - LEASE_MARGIN)
        broadcast_id = int(lease["broadcast_id"])
        text = str(lease["text"])
        parse_mode = lease.get("parse_mode") or None
        results: list[dict[str, Any]] = []
        try:
            for chat_id in lease["chat_ids"]:
                if self._stopping.is_set() or loop.time() >= deadline:
                    break  # the rest goes back to the queue when the lease ends
                status, error = await self._send(int(chat_id), text, parse_mode, deadline)
                if status is None:
                    continue
                results.append({"chat_id": chat_id, "status": status, "error": error})
                if status == "sent":
                    self._client.track(
                        "broadcast_received",
                        user_id=int(chat_id),
                        props={"broadcast_id": broadcast_id},
                    )
                if len(results) >= REPORT_EVERY:
                    await self._report(broadcast_id, results)
                    results = []
        finally:
            if results:
                await self._report(broadcast_id, results)

    async def _send(
        self, chat_id: int, text: str, parse_mode: str | None, deadline: float
    ) -> tuple[str | None, str]:
        """``(status, error)``; status None leaves the recipient for a later lease."""
        loop = asyncio.get_running_loop()
        for attempt in range(SEND_ATTEMPTS):
            await self._bucket.take()
            try:
                await self._bot.send_message(chat_id, text, parse_mode=parse_mode)
                return "sent", ""
            except TelegramRetryAfter as exc:
                if loop.time() + exc.retry_after >= deadline:
                    return None, ""
                self._bucket.pause(exc.retry_after)
            except TelegramForbiddenError as exc:
                return "blocked", normalize_error(exc)[1]
            except TelegramBadRequest as exc:
                return "failed", normalize_error(exc)[1]
            except (TelegramNetworkError, TelegramServerError):
                await self._sleep(min(2.0**attempt, max(0.0, deadline - loop.time())))
            except Exception as exc:
                return "failed", normalize_error(exc)[1]
            if self._stopping.is_set():
                return None, ""
        return None, ""  # Telegram kept failing: try again with a later lease

    # --- collector ----------------------------------------------------------------------

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                headers={
                    "Authorization": f"Bearer {self._client._api_key}",
                    "User-Agent": f"{SDK_INFO['name']}/{SDK_INFO['version']}",
                },
            )
        return self._session

    async def _lease(self) -> dict[str, Any] | None:
        url = self._options.endpoint + LEASE_PATH
        async with self._http().post(url, json={"limit": LEASE_SIZE}) as resp:
            if resp.status == 204:
                return None
            if resp.status == 401:
                logger.error("botsight: broadcasts stopped: invalid or revoked api_key (401)")
                raise _Stop
            if resp.status != 200:
                raise RuntimeError(f"lease answered {resp.status}")
            data: dict[str, Any] = await resp.json()
            return data

    async def _report(self, broadcast_id: int, results: list[dict[str, Any]]) -> None:
        """A report is retried a few times: a lost one would let the lease expire and the
        recipients get the message again. Repeating a report is harmless."""
        url = self._options.endpoint + REPORT_PATH
        body = {"broadcast_id": broadcast_id, "results": results}
        for attempt in range(3):
            try:
                async with self._http().post(url, json=body) as resp:
                    if resp.status == 200:
                        return
                    if 400 <= resp.status < 500:
                        log_limited(
                            "broadcast-report", "botsight: report rejected (%d)", resp.status
                        )
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                pass
            await asyncio.sleep(0.5 * 2**attempt)
        log_limited("broadcast-report", "botsight: broadcast results not reported")

    async def _sleep(self, delay: float) -> None:
        """Sleep that ends early on shutdown."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), delay)


class _Stop(Exception):
    """The key is wrong: broadcasts stay off until restart."""
