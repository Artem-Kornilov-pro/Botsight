"""Background delivery of events to the collector (section 8.8)."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import hmac
import json
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aiohttp

from ._log import log_limited, logger
from ._uuid import uuid7
from .events import MAX_TEXT, SDK_INFO, Event

if TYPE_CHECKING:
    from .client import Botsight

SCHEMA_VERSION = 1
BATCH_PATH = "/v1/batch"

# Collector limits are 2 MiB per batch after decompression and 16 KiB per event
# (section 10.1). Keep a margin for the batch envelope.
MAX_BATCH_BYTES = 2 * 1024 * 1024 - 64 * 1024
MAX_EVENT_BYTES = 16 * 1024

BACKOFF_BASE = 0.5
BACKOFF_CAP = 60.0
MAX_BATCH_AGE = 600.0  # give up on a batch after 10 minutes of retries
MAX_RETRY_AFTER = 600.0
BREAKER_THRESHOLD = 5
BREAKER_PAUSE = 30.0
REQUEST_TIMEOUT = 10.0
SHUTDOWN_TIMEOUT = 5.0
RESTART_DELAY = 1.0

_JSON_SEPARATORS = (",", ":")


@dataclass
class Batch:
    """Immutable after creation: a retry re-sends the same ``batch_id`` and events,
    which is what server-side deduplication relies on (section 10.4)."""

    batch_id: str
    events: list[bytes]
    dropped: int
    body: bytes
    created: float


class Sender:
    def __init__(self, client: Botsight) -> None:
        self._client = client
        self._options = client.options
        self._url = self._options.endpoint + BATCH_PATH
        self._pending: deque[Batch] = deque()
        self._failures = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping: asyncio.Event | None = None
        self._stop_waiter: asyncio.Task[Any] | None = None
        self._session: aiohttp.ClientSession | None = None
        self._pseudonyms: dict[int, int] = {}

    # --- lifecycle --------------------------------------------------------------------

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(self._run(), name="botsight-sender")

    async def close(self) -> None:
        task, stopping = self._task, self._stopping
        if task is None or stopping is None:
            return
        stopping.set()
        try:
            await asyncio.wait_for(task, SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            lost = self._client._queue.qsize() + sum(len(b.events) for b in self._pending)
            logger.warning("botsight: shutdown timeout, %d events not delivered", lost)
        except Exception:
            log_limited("sender-close", "botsight: sender failed on shutdown", exc_info=True)
        finally:
            self._task = None
            if self._stop_waiter is not None:
                self._stop_waiter.cancel()
                self._stop_waiter = None
            if self._session is not None:
                await self._session.close()
                self._session = None

    # --- main loop --------------------------------------------------------------------

    async def _run(self) -> None:
        assert self._stopping is not None
        self._stop_waiter = asyncio.ensure_future(self._stopping.wait())
        while True:
            try:
                if await self._iteration():
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                log_limited("sender", "botsight: sender error, restarting", exc_info=True)
                await self._sleep(RESTART_DELAY)

    async def _iteration(self) -> bool:
        """One collect-and-send round. Returns True when the sender should exit."""
        assert self._stopping is not None
        if not self._client._active:
            return True
        if not self._pending:
            raw = await self._collect()
            if not raw:
                return self._stopping.is_set()
            dropped = self._client._take_dropped()
            self._pending.extend(await asyncio.to_thread(self._build, raw, dropped))
        while self._pending and self._client._active:
            await self._deliver(self._pending.popleft())
        return False

    async def _collect(self) -> list[Event]:
        """Up to ``batch_size`` events, waiting at most ``flush_interval`` after the first."""
        assert self._stopping is not None
        queue = self._client._queue
        first = await self._next(None)
        if first is None:
            return []
        items = [first]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._options.flush_interval
        while len(items) < self._options.batch_size:
            try:
                items.append(queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass
            remaining = deadline - loop.time()
            if remaining <= 0 or self._stopping.is_set():
                break
            ev = await self._next(remaining)
            if ev is None:
                break
            items.append(ev)
        return items

    async def _next(self, timeout: float | None) -> Event | None:
        """Next event, or None on timeout or when stopping with an empty queue."""
        queue = self._client._queue
        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        assert self._stop_waiter is not None
        if self._stop_waiter.done():
            return None
        getter = asyncio.ensure_future(queue.get())
        done, _ = await asyncio.wait(
            {getter, self._stop_waiter}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if getter in done:
            return getter.result()
        # Cancelling a pending Queue.get() never loses an item: it is only removed
        # from the queue after the getter resumes.
        getter.cancel()
        return None

    # --- building batches (runs in a worker thread) -----------------------------------

    def _build(self, raw: list[Event], dropped: int) -> list[Batch]:
        encoded: list[bytes] = []
        for ev in raw:
            data = self._finalize(ev)
            if data is None:
                dropped += 1
            else:
                encoded.append(data)

        batches: list[Batch] = []
        chunk: list[bytes] = []
        size = 0
        for data in encoded:
            if chunk and (
                len(chunk) >= self._options.batch_size or size + len(data) + 1 > MAX_BATCH_BYTES
            ):
                batches.append(self._make_batch(chunk, dropped))
                dropped = 0
                chunk, size = [], 0
            chunk.append(data)
            size += len(data) + 1
        if chunk:
            batches.append(self._make_batch(chunk, dropped))
        elif dropped:
            self._client._add_dropped(dropped)  # nothing to carry it, report next time
        return batches

    def _finalize(self, ev: Event) -> bytes | None:
        try:
            unix_ms = int(ev["ts"] * 1000)
            out: Event = {"event_id": uuid7(unix_ms)}
            out.update(ev)
            out["ts"] = _format_ts(unix_ms)
            if self._options.pseudonymize_user_ids:
                if out["user_id"]:
                    out["user_id"] = self._pseudonym(out["user_id"])
                if out["chat_id"] > 0:  # private chat id equals the user id
                    out["chat_id"] = self._pseudonym(out["chat_id"])
            text = out.get("text")
            if text is not None and self._options.redact is not None:
                try:
                    out["text"] = str(self._options.redact(text))[:MAX_TEXT]
                except Exception:
                    del out["text"]
                    log_limited("redact", "botsight: redact() failed, text not sent", exc_info=True)
            data = _dumps(out)
            if len(data) > MAX_EVENT_BYTES and "text" in out:
                excess = len(data) - MAX_EVENT_BYTES + 64
                text_bytes = out["text"].encode()
                out["text"] = text_bytes[: max(0, len(text_bytes) - excess)].decode(
                    "utf-8", "ignore"
                )
                data = _dumps(out)
                if len(data) > MAX_EVENT_BYTES:
                    del out["text"]
                    data = _dumps(out)
            if len(data) > MAX_EVENT_BYTES:
                log_limited(
                    "event-size",
                    "botsight: event %r over %d bytes dropped",
                    out.get("name"),
                    MAX_EVENT_BYTES,
                )
                return None
            return data
        except Exception:
            log_limited("finalize", "botsight: failed to serialize event", exc_info=True)
            return None

    def _pseudonym(self, value: int) -> int:
        """HMAC-SHA256 with the project secret, first 8 bytes, top bit cleared to fit int64."""
        cached = self._pseudonyms.get(value)
        if cached is not None:
            return cached
        assert self._options.pseudonymize_secret is not None
        digest = hmac.new(
            self._options.pseudonymize_secret, str(value).encode(), hashlib.sha256
        ).digest()
        result = int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF
        if len(self._pseudonyms) > 100_000:
            self._pseudonyms.clear()
        self._pseudonyms[value] = result
        return result

    def _make_batch(self, events: list[bytes], dropped: int) -> Batch:
        batch_id = uuid7()
        envelope = {
            "schema_version": SCHEMA_VERSION,
            "batch_id": batch_id,
            "sent_at": _format_ts(time.time_ns() // 1_000_000),
            "sdk": SDK_INFO,
            "dropped": dropped,
        }
        head = _dumps(envelope)[:-1] + b',"events":['
        body = gzip.compress(head + b",".join(events) + b"]}", compresslevel=6)
        return Batch(batch_id, events, dropped, body, time.monotonic())

    # --- delivery ---------------------------------------------------------------------

    async def _deliver(self, batch: Batch) -> None:
        assert self._stopping is not None
        attempt = 0
        while True:
            status, retry_after = await self._post(batch)
            if status is not None and 200 <= status < 300:
                self._failures = 0
                return
            if status == 401:
                self._client._deactivate("invalid or revoked api_key (401)")
                return
            if status == 413:
                await self._split(batch)
                return
            if status is not None and 400 <= status < 500 and status not in (408, 409, 429):
                log_limited("http-4xx", "botsight: collector rejected batch (%d), dropped", status)
                self._lose(batch)
                return

            # 409 (same batch in progress), 408, 429, 5xx, timeout, network: retry
            self._failures += 1
            if self._stopping.is_set() or time.monotonic() - batch.created > MAX_BATCH_AGE:
                self._lose(batch)
                return
            if retry_after is not None:
                delay = retry_after
            else:
                delay = random.uniform(0, min(BACKOFF_CAP, BACKOFF_BASE * 2**attempt))
            if self._failures >= BREAKER_THRESHOLD:
                delay = max(delay, BREAKER_PAUSE)
                log_limited("breaker", "botsight: collector unavailable, pausing delivery")
            attempt += 1
            await self._sleep(delay)

    async def _post(self, batch: Batch) -> tuple[int | None, float | None]:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                headers={
                    "Authorization": f"Bearer {self._client._api_key}",
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                    "User-Agent": f"{SDK_INFO['name']}/{SDK_INFO['version']}",
                },
            )
        try:
            async with self._session.post(self._url, data=batch.body) as resp:
                await resp.read()
                return resp.status, _retry_after(resp.headers.get("Retry-After"))
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            log_limited("http-network", "botsight: cannot reach collector: %r", exc)
            return None, None

    async def _split(self, batch: Batch) -> None:
        if len(batch.events) <= 1:
            log_limited("http-413", "botsight: single event rejected as too large, dropped")
            self._lose(batch)
            return
        mid = len(batch.events) // 2
        first = await asyncio.to_thread(self._make_batch, batch.events[:mid], batch.dropped)
        second = await asyncio.to_thread(self._make_batch, batch.events[mid:], 0)
        self._pending.appendleft(second)
        self._pending.appendleft(first)

    def _lose(self, batch: Batch) -> None:
        self._client._add_dropped(len(batch.events) + batch.dropped)

    async def _sleep(self, delay: float) -> None:
        """Sleep that ends early on shutdown."""
        assert self._stop_waiter is not None
        await asyncio.wait({self._stop_waiter}, timeout=delay)


def _dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=_JSON_SEPARATORS).encode()


def _format_ts(unix_ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(unix_ms // 1000)) + (
        f".{unix_ms % 1000:03d}Z"
    )


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return min(max(float(value), 0.0), MAX_RETRY_AFTER)
    except ValueError:
        return None
