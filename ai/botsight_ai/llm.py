"""LLM behind one interface so the provider can be replaced (Botsight.md, 14.4).

Texts of users are personal data: the LLM must be hosted in Russia or on our own
servers. Any OpenAI-compatible chat API works: vLLM, Ollama, compatible providers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from . import metrics
from .config import LLMConfig


class LLMError(Exception):
    pass


class LLMDisabled(LLMError):
    pass


@dataclass(frozen=True)
class Completion:
    text: str
    prompt_tokens: int = 0
    completion_tokens: int = 0


class LLM(Protocol):
    enabled: bool

    def complete(
        self, system: str, user: str, *, purpose: str, max_tokens: int = 400
    ) -> Completion: ...


class DisabledLLM:
    enabled = False

    def complete(
        self, system: str, user: str, *, purpose: str, max_tokens: int = 400
    ) -> Completion:
        raise LLMDisabled("LLM is not configured")


class OpenAICompatibleLLM:
    enabled = True

    def __init__(self, cfg: LLMConfig, transport: httpx.BaseTransport | None = None) -> None:
        headers = {"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {}
        self._client = httpx.Client(
            base_url=cfg.base_url, timeout=cfg.timeout, headers=headers, transport=transport
        )
        self._model = cfg.model
        self._price = cfg.price_rub_per_1k_tokens

    def complete(
        self, system: str, user: str, *, purpose: str, max_tokens: int = 400
    ) -> Completion:
        try:
            resp = self._client.post(
                "/chat/completions",
                json={
                    "model": self._model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.1,
                },
            )
            resp.raise_for_status()
            body = resp.json()
            text = body["choices"][0]["message"]["content"] or ""
        except (httpx.HTTPError, KeyError, IndexError, ValueError) as exc:
            metrics.llm_calls.labels(purpose, "error").inc()
            raise LLMError(f"LLM request failed: {exc}") from exc
        usage = body.get("usage") or {}
        result = Completion(
            text=text,
            prompt_tokens=int(usage.get("prompt_tokens") or 0),
            completion_tokens=int(usage.get("completion_tokens") or 0),
        )
        metrics.llm_calls.labels(purpose, "ok").inc()
        metrics.llm_tokens.labels(purpose, "prompt").inc(result.prompt_tokens)
        metrics.llm_tokens.labels(purpose, "completion").inc(result.completion_tokens)
        tokens = result.prompt_tokens + result.completion_tokens
        metrics.llm_cost_rub.labels(purpose).inc(tokens / 1000 * self._price)
        return result


def build(cfg: LLMConfig) -> LLM:
    return OpenAICompatibleLLM(cfg) if cfg.enabled else DisabledLLM()


_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def parse_json_object(text: str) -> dict[str, Any]:
    """The first JSON object in a reply; models like to wrap it in code fences or prose."""
    m = _FENCE.search(text)
    candidates = [m.group(1)] if m else []
    candidates.append(text)
    decoder = json.JSONDecoder()
    for cand in candidates:
        for start in (i for i, ch in enumerate(cand) if ch == "{"):
            try:
                obj, _ = decoder.raw_decode(cand[start:])
            except ValueError:
                continue
            if isinstance(obj, dict):
                return obj
    raise LLMError("no JSON object in the LLM reply")
