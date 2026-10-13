# ai-worker

AI-модуль Botsight (раздел 14 [Botsight.md](../Botsight.md)):

- **темы сообщений и тренды** — сообщения пользователей группируются в темы с названиями, о росте темы приходит алерт;
- **вопросы на естественном языке** — вопрос превращается в SQL, который выполняется под отдельным пользователем ClickHouse с доступом только к строкам проекта;
- **аномалии** — резкий рост ошибок и блокировок или падение активности с объяснением, какие хендлеры, ошибки или перезапуски бота за этим стоят.

Модуль работает только для проектов с включённым сбором текстов (`capture_text`). Без LLM темы называются ключевыми словами, аномалии объясняются шаблоном, а вопросы на естественном языке недоступны.

## Как устроено

```
messages.text (Kafka) → embed → Qdrant (векторы) + ClickHouse message_points
                                      ↓ раз в час
                              topics → ClickHouse topics, point_topics; алерты → Kafka alerts
events (ClickHouse) → anomalies (раз в час) → алерты → Kafka alerts → ch-writer → ClickHouse alerts
api POST /v1/projects/{id}/ask → serve /internal/ask → LLM → проверка SQL → ClickHouse (nl_p{id})
```

| Команда | Что делает | Как масштабируется |
|---|---|---|
| `embed` | читает `messages.text`, нормализует тексты, считает эмбеддинги | партиции Kafka |
| `topics` | раз в час кластеризует тексты каждого проекта с новыми сообщениями | блокировка проекта в Redis |
| `anomalies` | раз в час ищет аномалии сразу по всем проектам | блокировка часа в Redis |
| `serve` | внутренний HTTP API для `api`, `/metrics` | без состояния |
| `all` | всё в одном процессе (docker compose) | — |

## Локальный запуск

В составе всего стека, из корня репозитория:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Чтобы темы считались раз в минуту, а не раз в час, и появились тексты для них:

```bash
AI_TOPICS_INTERVAL_SECONDS=60 docker compose -f deploy/docker-compose.yml up -d --build
cd backend && go run ./cmd/loadgen -projects 2 -users 300 -rate 1000 -duration 20s -texts
```

Через минуту-две темы видны в `GET /v1/projects/{id}/topics` (войдите под аккаунтом, который напечатал `loadgen`).

LLM подключается переменными, например Ollama на хосте:

```bash
LLM_BASE_URL=http://host.docker.internal:11434/v1 LLM_MODEL=qwen2.5:7b \
  docker compose -f deploy/docker-compose.yml up -d ai-worker
```

Тексты пользователей — персональные данные: в продакшене LLM должна быть размещена в РФ или на своём сервере (§14.4).

## Разработка

```bash
cd ai
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check . && mypy
pytest                       # unit и интеграционные (нужен Docker: ClickHouse, Qdrant, Redis)
pytest -m model              # настоящая модель эмбеддингов на русских фразах (~220 МБ)
```

## Настройки

| Переменная | По умолчанию | Описание |
|---|---|---|
| `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE` | `localhost`, `8123`, `default`, пусто, `botsight` | ClickHouse по HTTP. Пользователю нужно управление доступом: модуль создаёт пользователей `nl_p{project_id}` |
| `QDRANT_URL` | `http://localhost:6333` | Qdrant |
| `REDIS_URL` | `redis://localhost:6379/0` | блокировки и дневные лимиты |
| `KAFKA_BROKERS` | `localhost:9094` | `messages.text` и `alerts` |
| `AI_KAFKA_GROUP` | `ai-worker` | consumer group |
| `AI_EMBEDDING_MODEL` | `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` | модель fastembed; смена размерности требует новой коллекции Qdrant |
| `AI_MODEL_CACHE` | кэш fastembed | папка с моделью (в образе — `/models`) |
| `AI_DAILY_TEXT_LIMIT` | `20000` | текстов в день на проект, сверх — каждый десятый |
| `AI_TOPICS_INTERVAL_SECONDS` | `3600` | как часто пересчитывать темы |
| `AI_TOPICS_MAX_POINTS` | `20000` | максимум текстов в одной кластеризации |
| `AI_MIN_CLUSTER_SIZE` | `8` | минимальный размер темы |
| `AI_TREND_GROWTH`, `AI_TREND_MIN_MESSAGES` | `3`, `20` | порог роста доли темы и минимальный объём для алерта |
| `AI_ANOMALY_Z` | `4` | порог робастного z-score |
| `AI_HTTP_ADDR` | `0.0.0.0:8090` | адрес внутреннего API |
| `AI_INTERNAL_TOKEN` | — | общий с `api` токен; без него `/internal/ask` закрыт |
| `AI_NL_PASSWORD_SECRET` | — | секрет, из которого выводятся пароли пользователей `nl_p*` |
| `LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY` | пусто | OpenAI-совместимый API; пусто — без LLM |
| `LLM_TIMEOUT` | `60` | секунд на ответ LLM |
| `LLM_PRICE_RUB_PER_1K_TOKENS` | `0` | цена для метрики стоимости |

## Лицензия

[PolyForm Noncommercial 1.0.0](https://github.com/Artem-Kornilov-pro/Botsight/blob/main/LICENSE).
