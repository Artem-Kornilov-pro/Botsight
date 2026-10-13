"""Pure functions of topic modelling: clustering, stable topic ids, keywords."""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence

import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.decomposition import PCA

from .embedder import Vectors

# Above this many points vectors are reduced with PCA first: HDBSCAN on 384 dimensions
# gets slow, and on a few thousand points the full vectors cluster best.
PCA_ABOVE = 5000
PCA_COMPONENTS = 50


def cluster(vectors: Vectors, min_cluster_size: int) -> np.ndarray:
    """HDBSCAN labels, -1 is noise (Botsight.md, 14.1, item 5): the number of topics is not
    known in advance and one-off messages stay out of every topic. The minimum cluster size
    grows with the data so that a large project does not get hundreds of micro-topics."""
    n = len(vectors)
    mcs = max(min_cluster_size, n // 400)
    if n < 2 * mcs:
        return np.full(n, -1)
    x = vectors
    if n > PCA_ABOVE:
        x = PCA(n_components=PCA_COMPONENTS, random_state=0).fit_transform(vectors)
    labels: np.ndarray = HDBSCAN(min_cluster_size=mcs, min_samples=3, copy=True).fit(x).labels_
    return labels


def centroids(vectors: Vectors, labels: np.ndarray) -> dict[int, np.ndarray]:
    out: dict[int, np.ndarray] = {}
    for label in sorted(set(labels.tolist()) - {-1}):
        c = vectors[labels == label].mean(axis=0)
        out[label] = c / (np.linalg.norm(c) or 1.0)
    return out


def match_topics(
    new: Mapping[int, np.ndarray],
    old: Mapping[int, np.ndarray],
    next_id: int,
    threshold: float = 0.8,
) -> dict[int, int]:
    """Cluster label -> topic id. A cluster close to a previous topic keeps its id, so a
    topic keeps its name and history between hourly runs; the closest pairs win first."""
    pairs = sorted(
        ((float(cv @ ov), label, tid) for label, cv in new.items() for tid, ov in old.items()),
        reverse=True,
    )
    result: dict[int, int] = {}
    used: set[int] = set()
    for sim, label, tid in pairs:
        if sim < threshold:
            break
        if label in result or tid in used:
            continue
        result[label] = tid
        used.add(tid)
    for label in sorted(new):
        if label not in result:
            result[label] = next_id
            next_id += 1
    return result


def assign(
    vectors: Vectors, topic_centroids: Mapping[int, np.ndarray], threshold: float
) -> np.ndarray:
    """Nearest topic for every vector, 0 if none is similar enough (noise and old texts)."""
    out = np.zeros(len(vectors), dtype=np.int64)
    if not topic_centroids or len(vectors) == 0:
        return out
    ids = np.array(list(topic_centroids))
    mat = np.stack([topic_centroids[t] for t in ids])
    sims = vectors @ mat.T
    best = sims.argmax(axis=1)
    ok = sims[np.arange(len(vectors)), best] >= threshold
    out[ok] = ids[best[ok]]
    return out


def nearest(vectors: Vectors, centroid: np.ndarray, k: int) -> np.ndarray:
    """Indices of the k vectors closest to the centroid."""
    sims = vectors @ centroid
    k = min(k, len(sims))
    return np.argsort(-sims)[:k]


_WORD = re.compile(r"[a-zа-яё]{3,}")
STOPWORDS = frozenset(
    [
        "это",
        "как",
        "так",
        "что",
        "все",
        "она",
        "они",
        "его",
        "ему",
        "для",
        "или",
        "при",
        "над",
        "под",
        "без",
        "про",
        "через",
        "уже",
        "еще",
        "ещё",
        "тоже",
        "только",
        "когда",
        "где",
        "кто",
        "чем",
        "там",
        "тут",
        "вот",
        "был",
        "была",
        "были",
        "быть",
        "есть",
        "нет",
        "нету",
        "даже",
        "очень",
        "можно",
        "нужно",
        "надо",
        "мне",
        "меня",
        "мой",
        "моя",
        "мои",
        "вас",
        "вам",
        "ваш",
        "наш",
        "нас",
        "the",
        "and",
        "you",
        "for",
        "are",
        "not",
        "with",
        "пожалуйста",
        "здравствуйте",
        "привет",
        "спасибо",
        "добрый",
        "день",
        "почему",
        "какой",
        "какая",
        "какие",
        "срочно",
        "помогите",
        "помоги",
        "подскажите",
        "после",
        "час",
        "блин",
        "вообще",
        "просто",
    ]
)


MIN_CONTENT_WORDS = 2  # shorter texts ("привет", "ок", "как дела") stay out of topics


def content_words(text: str) -> int:
    """Meaningful words: small talk has almost none and would glue unrelated short
    questions into one cluster."""
    return sum(1 for w in _WORD.findall(text.lower()) if w not in STOPWORDS)


def keywords(texts_by_topic: Mapping[int, Sequence[str]], top: int = 5) -> dict[int, list[str]]:
    """c-TF-IDF (class-based TF-IDF): words frequent in one topic and rare in the others.
    Names topics when no LLM is configured."""
    counts: dict[int, Counter[str]] = {}
    for tid, texts in texts_by_topic.items():
        c: Counter[str] = Counter()
        for t in texts:
            c.update(w for w in _WORD.findall(t.lower()) if w not in STOPWORDS)
        counts[tid] = c
    total: Counter[str] = Counter()
    for c in counts.values():
        total.update(c)
    if not total:
        return {tid: [] for tid in texts_by_topic}
    avg_words = sum(total.values()) / max(1, len(counts))
    out: dict[int, list[str]] = {}
    for tid, c in counts.items():
        size = sum(c.values()) or 1
        scored = sorted(
            ((n / size * math.log(1 + avg_words / total[w]), w) for w, n in c.items()),
            reverse=True,
        )
        out[tid] = [w for _, w in scored[:top]]
    return out
