from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from aiogram.fsm.state import State, StatesGroup

from ._names import handler_name

DEFAULT_ENDPOINT = "https://ingest.botsight.ru"
MAX_BATCH_SIZE = 500  # collector limit, section 10.1


@dataclass(frozen=True)
class Options:
    """SDK settings, section 8.9. Invalid values fail fast in ``setup()``."""

    endpoint: str = DEFAULT_ENDPOINT
    capture_text: bool = False
    skip_text_in_states: frozenset[str] = field(default_factory=frozenset)
    redact: Callable[[str], str] | None = None
    pseudonymize_user_ids: bool = False
    pseudonymize_secret: bytes | None = None
    track_fsm: bool = True
    track_blocks: bool = True
    fallback_handlers: frozenset[str] = field(default_factory=frozenset)
    batch_size: int = MAX_BATCH_SIZE
    flush_interval: float = 2.0
    max_queue: int = 10_000
    sample_rate: float = 1.0
    enabled: bool = True

    @classmethod
    def build(
        cls,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        capture_text: bool = False,
        skip_text_in_states: Iterable[str | State | type[StatesGroup]] = (),
        redact: Callable[[str], str] | None = None,
        pseudonymize_user_ids: bool = False,
        pseudonymize_secret: str | bytes | None = None,
        track_fsm: bool = True,
        track_blocks: bool = True,
        fallback_handlers: Iterable[str | Callable[..., Any]] = (),
        batch_size: int = MAX_BATCH_SIZE,
        flush_interval: float = 2.0,
        max_queue: int = 10_000,
        sample_rate: float = 1.0,
        enabled: bool = True,
    ) -> Options:
        if not isinstance(endpoint, str) or not endpoint.startswith(("https://", "http://")):
            raise ValueError("botsight: endpoint must be an http(s) URL")
        if redact is not None and not callable(redact):
            raise TypeError("botsight: redact must be a callable (text) -> text")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_BATCH_SIZE
        ):
            raise ValueError(f"botsight: batch_size must be in 1..{MAX_BATCH_SIZE}")
        if not isinstance(flush_interval, (int, float)) or not 0 < flush_interval <= 60:
            raise ValueError("botsight: flush_interval must be in (0, 60] seconds")
        if isinstance(max_queue, bool) or not isinstance(max_queue, int) or max_queue < 1:
            raise ValueError("botsight: max_queue must be a positive int")
        if (
            not isinstance(sample_rate, (int, float))
            or math.isnan(sample_rate)
            or not 0 <= sample_rate <= 1
        ):
            raise ValueError("botsight: sample_rate must be in [0, 1]")

        secret: bytes | None = None
        if pseudonymize_user_ids:
            if isinstance(pseudonymize_secret, str):
                secret = pseudonymize_secret.encode()
            elif isinstance(pseudonymize_secret, bytes):
                secret = pseudonymize_secret
            if not secret:
                raise ValueError(
                    "botsight: pseudonymize_user_ids=True requires pseudonymize_secret "
                    "(keep it stable: changing it changes every pseudonym)"
                )

        return cls(
            endpoint=endpoint.rstrip("/"),
            capture_text=bool(capture_text),
            skip_text_in_states=frozenset(_state_names(skip_text_in_states)),
            redact=redact,
            pseudonymize_user_ids=bool(pseudonymize_user_ids),
            pseudonymize_secret=secret,
            track_fsm=bool(track_fsm),
            track_blocks=bool(track_blocks),
            fallback_handlers=frozenset(_handler_names(fallback_handlers)),
            batch_size=batch_size,
            flush_interval=float(flush_interval),
            max_queue=max_queue,
            sample_rate=float(sample_rate),
            enabled=bool(enabled),
        )


def _state_names(items: Iterable[str | State | type[StatesGroup]]) -> Iterable[str]:
    if isinstance(items, (str, State)) or (
        isinstance(items, type) and issubclass(items, StatesGroup)
    ):
        items = [items]
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, State):
            if item.state:
                yield item.state
        elif isinstance(item, type) and issubclass(item, StatesGroup):
            yield from item.__all_states_names__
        else:
            raise TypeError(
                f"botsight: skip_text_in_states accepts str, State or StatesGroup, got {item!r}"
            )


def _handler_names(items: Iterable[str | Callable[..., Any]]) -> Iterable[str]:
    if isinstance(items, str) or callable(items):
        items = [items]
    for item in items:
        if isinstance(item, str):
            yield item
        elif callable(item):
            yield handler_name(item)
        else:
            raise TypeError(
                f"botsight: fallback_handlers accepts handlers or 'module:qualname', got {item!r}"
            )
