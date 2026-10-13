"""Text normalization before embedding (Botsight.md, 14.1, item 2).

Personal details and numbers are replaced with tokens, so "мой номер +7 999 123-45-67"
and "мой номер 8 912 000 11 22" become one text with one vector.
"""

from __future__ import annotations

import re
import uuid

_URL = re.compile(r"https?://\S+|www\.\S+")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+")
_PHONE = re.compile(r"(?<!\w)\+?\d[\d\s()\-]{8,}\d")
_NUMBER = re.compile(r"\d+([.,]\d+)?")
_SPACE = re.compile(r"\s+")

MAX_CHARS = 512  # longer texts are cut: the model reads ~128 tokens anyway

# Point ids are derived from the text, so the same text of a project always maps to the
# same vector (one embedding per unique text).
_NAMESPACE = uuid.UUID("6f1c2b7e-7d1a-4f0e-9a51-0b3c5f2d8e41")


def normalize(text: str) -> str:
    s = text.lower()
    s = _URL.sub(" <url> ", s)
    s = _EMAIL.sub(" <email> ", s)
    s = _PHONE.sub(" <phone> ", s)
    s = _NUMBER.sub(" <num> ", s)
    s = _SPACE.sub(" ", s).strip()
    return s[:MAX_CHARS]


def point_id(project_id: int, normalized: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{project_id}:{normalized}"))
