from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger("botsight")

LOG_INTERVAL = 60.0

_last_logged: dict[str, float] = {}


def log_limited(
    key: str, msg: str, *args: Any, level: int = logging.WARNING, exc_info: bool = False
) -> None:
    """Log SDK problems at most once per minute per key, so a broken SDK never floods bot logs."""
    now = time.monotonic()
    last = _last_logged.get(key)
    if last is not None and now - last < LOG_INTERVAL:
        return
    _last_logged[key] = now
    logger.log(level, msg, *args, exc_info=exc_info)
