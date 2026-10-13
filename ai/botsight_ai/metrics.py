"""Prometheus metrics, including the cost of the module (Botsight.md, 14.1, item 8):
embedding CPU seconds and LLM tokens and rubles per 1000 messages are derived from
these counters in Grafana. No project_id labels (cardinality)."""

from __future__ import annotations

from prometheus_client import Counter, Histogram

texts = Counter(
    "botsight_ai_texts_total",
    "Texts from messages.text by outcome: embedded, duplicate, sampled_out, empty.",
    ["result"],
)
embed_seconds = Counter(
    "botsight_ai_embed_seconds_total", "Wall time spent computing embeddings (CPU bound)."
)
llm_calls = Counter(
    "botsight_ai_llm_calls_total", "LLM requests by purpose and result.", ["purpose", "result"]
)
llm_tokens = Counter(
    "botsight_ai_llm_tokens_total", "LLM tokens by purpose and kind.", ["purpose", "kind"]
)
llm_cost_rub = Counter("botsight_ai_llm_cost_rub_total", "LLM cost in rubles.", ["purpose"])
topic_runs = Counter("botsight_ai_topic_runs_total", "Topic clustering runs.", ["result"])
topic_run_seconds = Histogram(
    "botsight_ai_topic_run_seconds",
    "Duration of one project's clustering run.",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
ask = Counter("botsight_ai_ask_total", "Natural language questions by result.", ["result"])
anomalies = Counter("botsight_ai_anomalies_total", "Anomalies found by metric.", ["metric"])
alerts = Counter("botsight_ai_alerts_total", "Alerts produced by kind.", ["kind"])
