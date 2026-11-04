#!/usr/bin/env python3
"""Grafana dashboards of Botsight as code (Botsight.md, 17).

    python3 deploy/grafana/dashboards.py

writes deploy/grafana/dashboards/*.json; Grafana provisions them read-only, so a change
is a change of this file. Panels are described compactly here: the JSON is long and
repetitive, a reviewer reads this file instead.
"""

import json
import pathlib

OUT = pathlib.Path(__file__).parent / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}

_ids = iter(range(1, 10_000))


def target(expr, legend="", ref="A"):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}


def timeseries(title, targets, unit="short", w=12, h=8, desc="", threshold=None, stack=False, min0=True):
    """Lines by default: thin, a legend under the chart, one tooltip for all series."""
    defaults = {
        "unit": unit,
        "color": {"mode": "palette-classic"},
        "custom": {
            "lineWidth": 2,
            "fillOpacity": 10 if stack else 0,
            "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none"},
            "thresholdsStyle": {"mode": "line" if threshold is not None else "off"},
        },
    }
    if min0:
        defaults["min"] = 0
    if threshold is not None:
        # the SLO target as a line on the chart
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": threshold}]}
    return {
        "id": next(_ids),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"w": w, "h": h},
        "targets": [target(e, l, chr(65 + i)) for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def stat(title, expr, unit="short", w=4, h=4, desc="", steps=None, decimals=None):
    """One number: the form when the data is a single current value."""
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": steps or [{"color": "text", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "id": next(_ids),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"w": w, "h": h},
        "targets": [target(expr)],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "none", "textMode": "value"},
    }


def row(title):
    return {"id": next(_ids), "type": "row", "title": title, "collapsed": False, "gridPos": {"w": 24, "h": 1}, "panels": []}


def layout(panels):
    """Left to right, top to bottom on Grafana's 24-column grid."""
    x = y = line = 0
    for p in panels:
        g = p["gridPos"]
        if x + g["w"] > 24 or p["type"] == "row":
            x, y = 0, y + line
            line = 0
        g["x"], g["y"] = x, y
        x += g["w"]
        line = max(line, g["h"])
        if p["type"] == "row":
            x, y, line = 0, y + g["h"], 0
    return panels


def dashboard(uid, title, panels, desc):
    return {
        "uid": uid,
        "title": title,
        "description": desc,
        "tags": ["botsight"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": "10s",
        "time": {"from": "now-1h", "to": "now"},
        "panels": layout(panels),
    }


def q(p, metric, by="", rng="5m"):
    """A quantile of a histogram; metric may carry a selector: name{table="events"}."""
    name, _, selector = metric.partition("{")
    selector = "{" + selector if selector else ""
    group = f" by (le{', ' + by if by else ''})"
    return f"histogram_quantile({p}, sum{group} (rate({name}_bucket{selector}[{rng}])))"


GOOD_BAD = lambda bad: [{"color": "green", "value": None}, {"color": "red", "value": bad}]

pipeline = dashboard("botsight-pipeline", "Botsight — приём и конвейер", [
    stat("Батчей в секунду", "sum(rate(botsight_ingest_requests_total[1m]))", "reqps", decimals=1),
    stat("Событий в секунду", 'sum(rate(botsight_ingest_events_total{result="accepted"}[1m]))', "short", decimals=0),
    stat("p99 приёма", q(0.99, "botsight_ingest_duration_seconds"), "s", steps=GOOD_BAD(0.05), desc="цель < 50 мс"),
    stat("Свежесть p95", "botsight:freshness_p95:5m", "s", steps=GOOD_BAD(10), desc="события: от записи в Kafka до вставки в ClickHouse; цель < 10 с"),
    stat("Лаг ch-writer", 'sum(botsight_consumer_lag{topic="events.raw"})', "short", steps=GOOD_BAD(100000)),
    stat("Доля 5xx", "botsight:ingest_error_ratio:rate5m", "percentunit", steps=GOOD_BAD(0.001), decimals=2),
    row("Приём: collector"),
    timeseries("Батчи по кодам ответа", [("sum by (code) (rate(botsight_ingest_requests_total[1m]))", "{{code}}")], "reqps", stack=True),
    timeseries("Время приёма батча", [(q(0.5, "botsight_ingest_duration_seconds", rng="1m"), "p50"), (q(0.95, "botsight_ingest_duration_seconds", rng="1m"), "p95"), (q(0.99, "botsight_ingest_duration_seconds", rng="1m"), "p99")], "s", threshold=0.05, desc="включая подтверждение Kafka; линия — цель p99 50 мс"),
    timeseries("События", [('sum by (result) (rate(botsight_ingest_events_total[1m]))', "{{result}}")], "short", stack=True, desc="принятые и отклонённые валидацией"),
    timeseries("Отказы и повторы", [
        ("sum(rate(botsight_ratelimited_total[1m]))", "лимит проекта (429)"),
        ("sum(rate(botsight_batch_duplicates_total[1m]))", "повтор батча"),
        ("sum(rate(botsight_ingest_overloaded_total[1m]))", "перегрузка (503)"),
        ("sum(rate(botsight_kafka_produce_errors_total[1m]))", "ошибка Kafka"),
    ], "reqps"),
    timeseries("Кэш ключей", [('sum by (result) (rate(botsight_key_cache_total[1m]))', "{{result}}")], "reqps", desc="промах ходит в PostgreSQL"),
    row("Конвейер: Kafka → ClickHouse"),
    timeseries("Лаг потребителя", [('sum by (topic) (botsight_consumer_lag)', "{{topic}}")], "short", desc="записи, которые ch-writer ещё не закоммитил"),
    timeseries("Свежесть событий", [(q(p, 'botsight_ch_freshness_seconds{table="events"}', rng="1m"), f"p{int(p * 100)}") for p in (0.5, 0.95, 0.99)], "s", threshold=10, desc="возраст самой старой записи пачки events в момент вставки; линия — цель p95 10 с"),
    timeseries("Вставки в ClickHouse", [('sum by (table) (rate(botsight_ch_insert_rows_total[1m]))', "{{table}}")], "rowsps"),
    timeseries("Время вставки p95", [(q(0.95, "botsight_ch_insert_duration_seconds", "table", "1m"), "{{table}}")], "s"),
    timeseries("Ошибки вставки и DLQ", [
        ('sum by (table) (rate(botsight_ch_insert_errors_total[5m]))', "ошибки вставки {{table}}"),
        ('sum by (topic) (increase(botsight_dlq_total[5m]))', "в DLQ {{topic}}"),
    ], "short", desc="ошибки вставки повторяются; DLQ — записи, которые не разобрать"),
    timeseries("Партиции на паузе", [("sum(botsight_ch_paused_partitions)", "на паузе")], "short", desc="backpressure, пока ClickHouse недоступен"),
    row("Антифрод, AI, прокси"),
    timeseries("Антифрод: прогоны", [('sum by (result) (increase(botsight_antifraud_runs_total[5m]))', "{{result}}"), ("max(botsight_antifraud_run_seconds)", "длительность, с")], "short", w=8),
    timeseries("AI: вызовы LLM", [('sum by (purpose, result) (increase(botsight_ai_llm_calls_total[5m]))', "{{purpose}} {{result}}")], "short", w=8),
    timeseries("Прокси Bot API", [('sum by (kind) (rate(botsight_proxy_requests_total[1m]))', "{{kind}}")], "reqps", w=8),
], "Путь события SDK → collector → Kafka → ch-writer → ClickHouse (Botsight.md, 7.3)")

api = dashboard("botsight-api", "Botsight — API и аналитика", [
    stat("Запросов в секунду", "sum(rate(botsight_api_requests_total[1m]))", "reqps", decimals=1),
    stat("Аналитика p95", q(0.95, "botsight_query_duration_seconds"), "s", steps=GOOD_BAD(1), desc="цель < 1 с"),
    # an hour, not 5 minutes: with no dashboard traffic 0/0 is not "0 % hits"
    stat("Попадания в кэш, час", 'sum(increase(botsight_query_cache_total{result="hit"}[1h])) / sum(increase(botsight_query_cache_total[1h]))', "percentunit", decimals=0),
    stat("Потоков /live", "sum(botsight_live_streams)", "short"),
    stat("Доля 5xx", '(sum(rate(botsight_api_requests_total{code=~"5.."}[5m])) or vector(0)) / clamp_min(sum(rate(botsight_api_requests_total[5m])), 1e-9)', "percentunit", steps=GOOD_BAD(0.01), decimals=2),
    stat("Отказы 429", '(sum(rate(botsight_api_requests_total{code="429"}[5m])) or vector(0))', "reqps", decimals=2, desc="лимиты входа, вопросов AI и потоков"),
    timeseries("Запросы по маршрутам, топ 10", [("topk(10, sum by (route) (rate(botsight_api_requests_total[1m])))", "{{route}}")], "reqps"),
    timeseries("p95 по маршрутам, топ 10", [("topk(10, " + q(0.95, "http_request_duration_seconds", "route", "5m") + ")", "{{route}}")], "s"),
    timeseries("Аналитика: p95 по видам запросов", [(q(0.95, "botsight_query_duration_seconds", "kind", "5m"), "{{kind}}")], "s", threshold=1, desc="время в ClickHouse, без попаданий в кэш; линия — цель 1 с"),
    timeseries("Кэш по видам запросов", [('sum by (kind, result) (rate(botsight_query_cache_total[5m]))', "{{kind}} {{result}}")], "reqps"),
    timeseries("Ответы по кодам", [('sum by (code) (rate(botsight_api_requests_total[1m]))', "{{code}}")], "reqps", stack=True),
    timeseries("Потоки /live", [("sum(botsight_live_streams)", "открыто")], "short"),
], "Сервис api: управление, аналитика из ClickHouse, живая лента (Botsight.md, 13.3)")

# Error budget over 30 days: 0.1 % of batch requests may fail
# "or vector(0)": with no 5xx at all there is no series, and the budget is untouched, not unknown
ERRORS_30D = '(sum(increase(botsight_ingest_requests_total{code=~"5.."}[30d])) or vector(0))'
TOTAL_30D = "sum(increase(botsight_ingest_requests_total[30d]))"
slo = dashboard("botsight-slo", "Botsight — SLO", [
    stat("Доступность приёма, 30 дней", f"1 - {ERRORS_30D} / clamp_min({TOTAL_30D}, 1)", "percentunit", w=6, h=5, decimals=3,
         steps=[{"color": "red", "value": None}, {"color": "green", "value": 0.999}], desc="цель 99,9 % батчей без 5xx"),
    stat("Остаток бюджета ошибок", f"1 - {ERRORS_30D} / clamp_min(0.001 * {TOTAL_30D}, 1e-9)", "percentunit", w=6, h=5, decimals=1,
         steps=[{"color": "red", "value": None}, {"color": "orange", "value": 0.25}, {"color": "green", "value": 0.5}], desc="бюджет 0,1 % запросов за 30 дней"),
    stat("p99 приёма", "botsight:ingest_latency_p99:5m", "s", w=4, h=5, steps=GOOD_BAD(0.05), desc="цель < 50 мс"),
    stat("Свежесть p95", "botsight:freshness_p95:5m", "s", w=4, h=5, steps=GOOD_BAD(10), desc="цель < 10 с"),
    stat("Дашборд p95", "botsight:query_latency_p95:5m", "s", w=4, h=5, steps=GOOD_BAD(1), desc="цель < 1 с"),
    timeseries("Скорость сгорания бюджета", [
        ("botsight:ingest_error_ratio:rate1h / 0.001", "за 1 час"),
        ("botsight:ingest_error_ratio:rate6h / 0.001", "за 6 часов"),
    ], "short", w=24, threshold=14.4, desc="1 — бюджет тратится ровно за 30 дней; 14,4 за час — срочный алерт (линия), 6 за 6 часов — задача"),
    timeseries("p99 приёма против цели", [("botsight:ingest_latency_p99:5m", "p99")], "s", threshold=0.05),
    timeseries("Свежесть p95 против цели", [("botsight:freshness_p95:5m", "p95")], "s", threshold=10),
], "Цели Botsight.md, 17.2 и бюджет ошибок; алерты — deploy/prometheus/rules.yml")

if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for d in (pipeline, api, slo):
        path = OUT / f"{d['uid']}.json"
        path.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(path)
