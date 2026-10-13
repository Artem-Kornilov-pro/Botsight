"""The real embedding model on Russian support messages: topics must separate.
Downloads ~220 MB; run with `pytest -m model` (CI caches the model)."""

from __future__ import annotations

import os
import random

import numpy as np
import pytest
from sklearn.metrics import adjusted_rand_score

from botsight_ai import clustering
from botsight_ai.config import Config
from botsight_ai.embedder import FastEmbedder, model_dim

pytestmark = pytest.mark.model

TOPICS = {
    "code": [
        "не приходит код",
        "код подтверждения не пришел",
        "смс с кодом не приходит",
        "где мой код?",
        "жду код уже 10 минут",
        "не получил код",
        "код не приходит на телефон",
        "почему нет кода подтверждения",
    ],
    "pay": [
        "не могу оплатить",
        "оплата не проходит",
        "карта отклонена при оплате",
        "ошибка при оплате картой",
        "деньги списались а подписки нет",
        "как оплатить подписку",
        "не проходит платеж",
        "оплатил но доступа нет",
    ],
    "lab": [
        "добавьте лабу по физике",
        "нужна лабораторная по химии",
        "когда будет лаба 5",
        "хочу лабу по программированию",
        "сделайте лабораторную по сетям",
        "есть ли лаба по матану",
        "добавьте лабораторную номер 3",
        "нет моей лабы в списке",
    ],
    "bug": [
        "бот не отвечает",
        "бот завис",
        "ничего не происходит после нажатия",
        "кнопка не работает",
        "бот молчит",
        "ошибка при генерации",
        "бот сломался",
        "не работает меню",
    ],
}
NOISE = [
    "привет",
    "спасибо",
    "ок",
    "кто ты",
    "погода завтра",
    "ахаха",
    "лол",
    "здравствуйте",
    "как дела",
    "что умеешь",
]


def test_real_model_separates_topics() -> None:
    model = Config().embedding_model
    emb = FastEmbedder(model, os.environ.get("AI_MODEL_CACHE"))
    assert emb.dim == model_dim(model) == 384

    rnd = random.Random(1)
    texts, labels = [], []
    for i, base in enumerate(TOPICS.values()):
        for b in base:
            for t in [
                b,
                *(
                    b + s
                    for s in rnd.sample(
                        ["!", " пожалуйста", " срочно", "??", " помогите", " уже час"], 2
                    )
                ),
            ]:
                texts.append(t)
                labels.append(i)
    texts += NOISE
    labels += [-1] * len(NOISE)

    # as in TopicRunner: texts without content words are not clustered
    topical = np.array([clustering.content_words(t) >= clustering.MIN_CONTENT_WORDS for t in texts])
    pred = np.full(len(texts), -1)
    pred[topical] = clustering.cluster(
        emb.embed([t for t, k in zip(texts, topical, strict=True) if k]), min_cluster_size=8
    )
    truth = np.array(labels)

    # noise as singletons on both sides
    def singletons(a: np.ndarray) -> np.ndarray:
        a = a.copy()
        nxt = a.max() + 1
        for i in np.where(a < 0)[0]:
            a[i] = nxt
            nxt += 1
        return a

    ari = adjusted_rand_score(singletons(truth), singletons(pred))
    assert ari >= 0.7, f"ARI {ari:.2f}"
    # Small talk may form its own "greetings" topic, but never mixes into real topics.
    topic_labels = set(pred[: -len(NOISE)].tolist()) - {-1}
    assert not topic_labels & set(pred[-len(NOISE) :].tolist())
