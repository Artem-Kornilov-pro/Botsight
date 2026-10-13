"""Text embeddings. The model runs locally on CPU: texts never leave the infrastructure
(Botsight.md, 14.1, item 3)."""

from __future__ import annotations

import hashlib
import time
from typing import Protocol

import numpy as np
import numpy.typing as npt

from . import metrics

Vectors = npt.NDArray[np.float32]


class Embedder(Protocol):
    dim: int

    def embed(self, texts: list[str]) -> Vectors:
        """Unit-length vectors, one row per text."""
        ...


def _unit(v: Vectors) -> Vectors:
    norms = np.linalg.norm(v, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return (v / norms).astype(np.float32)


def model_dim(model: str) -> int:
    """Vector size of a fastembed model without loading it (topics only read vectors)."""
    from fastembed import TextEmbedding

    for m in TextEmbedding.list_supported_models():
        if m["model"].lower() == model.lower():
            return int(m["dim"])
    raise ValueError(f"unknown embedding model {model}")


class FastEmbedder:
    """ONNX model via fastembed: no PyTorch, small image, fast on CPU.

    The default paraphrase-multilingual-MiniLM-L12-v2 (384 dimensions, 220 MB) handles
    Russian well; on short support messages it separated topics with ARI 0.78, the
    768-dimensional mpnet model reached 0.87 at 3.5x the size (AI_EMBEDDING_MODEL)."""

    def __init__(self, model: str, cache_dir: str | None = None, batch_size: int = 64) -> None:
        from fastembed import TextEmbedding

        self._model = TextEmbedding(model, cache_dir=cache_dir or None)
        self._batch = batch_size
        self.dim = int(self.embed(["dimension probe"]).shape[1])

    def embed(self, texts: list[str]) -> Vectors:
        started = time.perf_counter()
        vectors = np.array(list(self._model.embed(texts, batch_size=self._batch)), dtype=np.float32)
        metrics.embed_seconds.inc(time.perf_counter() - started)
        return _unit(vectors)


class HashEmbedder:
    """Deterministic bag-of-words vectors for tests: texts sharing words are close."""

    def __init__(self, dim: int = 64) -> None:
        self.dim = dim

    def embed(self, texts: list[str]) -> Vectors:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            for word in text.split():
                h = int.from_bytes(hashlib.blake2b(word.encode(), digest_size=8).digest(), "big")
                out[i, h % self.dim] += 1.0
        return _unit(out)
