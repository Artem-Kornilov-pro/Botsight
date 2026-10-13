from __future__ import annotations

import os
import time


def uuid7(unix_ms: int | None = None) -> str:
    """UUIDv7 (RFC 9562) as a string.

    Own implementation instead of ``uuid.uuid7()`` (Python 3.14+): it works on 3.10+
    and takes the event time, so ``event_id`` is ordered by when the event happened,
    not by when the batch was built.
    """
    if unix_ms is None:
        unix_ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")
    value = (
        (unix_ms & 0xFFFF_FFFF_FFFF) << 80
        | 0x7 << 76
        | ((rand >> 62) & 0xFFF) << 64
        | 0b10 << 62
        | (rand & 0x3FFF_FFFF_FFFF_FFFF)
    )
    h = f"{value:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
