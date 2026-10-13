# backend

Go-модуль с сервисами Botsight:

| Сервис | Что делает | Документация |
|---|---|---|
| `api` | control plane дашборда на PostgreSQL: регистрация и вход, проекты, участники, API-ключи, сохранённые воронки, расходы на рекламу | разделы 12.4, 13.3, 16.3 [Botsight.md](../Botsight.md), [api/openapi.yaml](api/openapi.yaml) |
| `collector` | принимает батчи событий от SDK (`POST /v1/batch`) и пишет их в Kafka | раздел 10 |
| `chwriter` | переносит события из Kafka в ClickHouse пачками, без потерь и дублей | раздел 11 |
| `loadgen` | генератор нагрузки в формате SDK со сверкой результата; `-texts` добавляет тексты сообщений для AI-модуля | раздел 18 |

AI-модуль (`ai-worker`) написан на Python и лежит в [`../ai`](../ai/README.md).

## Локальный запуск

Нужен Docker.

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Команду запускайте из корня репозитория. Поднимутся PostgreSQL, Redis, Mailpit, Kafka и ClickHouse, применятся миграции из `migrations/postgres/` и `migrations/clickhouse/`, запустятся `api`, `collector` и `chwriter`.

| Адрес | Что там |
|---|---|
| http://localhost:8080 | API (`/healthz`, `/readyz`, `/v1/...`) |
| http://localhost:8081 | collector: `POST /v1/batch` для SDK, `/metrics` |
| http://localhost:8082 | ch-writer: `/readyz`, `/metrics` |
| http://localhost:8025 | Mailpit: письма подтверждения почты и сброса пароля |
| http://localhost:8123 | ClickHouse (HTTP), база `botsight`, пользователь и пароль `botsight` |
| localhost:9094 | Kafka |
| localhost:5432 | PostgreSQL, база `botsight`, пользователь и пароль `botsight` |
| localhost:6379 | Redis |

Пример: регистрация и создание проекта.

```bash
curl -c jar -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"password123"}' localhost:8080/v1/auth/register
# в ответе csrf_token — его нужно передавать в X-CSRF-Token
curl -b jar -H 'Content-Type: application/json' -H 'X-CSRF-Token: <csrf_token>' \
  -d '{"name":"labbot"}' localhost:8080/v1/projects
```

Подключить бота: создайте проект, возьмите `api_key.key` из ответа и передайте его в SDK вместе с адресом collector:

```python
setup(dp, bot, api_key="bs_live_...", endpoint="http://localhost:8081")
```

Через секунду-две события видны в ClickHouse:

```bash
curl "localhost:8123/?user=botsight&password=botsight&database=botsight" \
  --data-binary "SELECT type, name, count() FROM events GROUP BY type, name FORMAT PrettyCompact"
```

Вход через Telegram включается переменной `TELEGRAM_BOT_TOKEN` (токен бота, к которому привязан Login Widget):

```bash
TELEGRAM_BOT_TOKEN=123:ABC docker compose -f deploy/docker-compose.yml up --build
```

## Тесты

```bash
cd backend
go test -race ./...
```

Интеграционные тесты сами поднимают PostgreSQL, Redis, Kafka и ClickHouse через testcontainers, нужен запущенный Docker. Без Docker они пропускаются локально и падают в CI. Среди них — тест падения ch-writer между вставкой в ClickHouse и фиксацией смещения в Kafka: после рестарта дублей нет.

## Нагрузка

При запущенном docker compose:

```bash
cd backend
go run ./cmd/loadgen -projects 20 -users 1000 -rate 20000 -duration 30s -journal /tmp/journal.txt
```

`loadgen` создаёт аккаунт и проекты через `api`, отправляет батчи в формате SDK (`-rate 0` — без ограничения, `-duplicates 0.02` — 2 % батчей повторно) и печатает пропускную способность, задержки и SQL для сверки с ClickHouse. В журнал пишутся `event_id` принятых событий.

По умолчанию у проекта лимит 2000 событий в секунду. Для замера предельной пропускной способности поднимите его:

```bash
INGEST_RATE=10000000 INGEST_BURST=10000000 docker compose -f deploy/docker-compose.yml up -d collector
```

## Настройки `api`

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | — | строка подключения к PostgreSQL, обязательна |
| `REDIS_URL` | — | `redis://host:6379/0`, обязательна |
| `API_ADDR` | `:8080` | адрес HTTP-сервера |
| `DASHBOARD_URL` | `http://localhost:5173` | адрес дашборда: ссылки в письмах и разрешённый источник CORS |
| `CORS_ORIGINS` | `DASHBOARD_URL` | дополнительные источники CORS через запятую |
| `COOKIE_SECURE` | `true` | `false` только для локального http |
| `SESSION_TTL` | `720h` | сессия живёт столько с последнего запроса |
| `TRUST_PROXY` | `false` | брать IP клиента из `X-Forwarded-For` / `X-Real-IP` (за балансировщиком) |
| `TELEGRAM_BOT_TOKEN` | пусто | токен бота для Telegram Login Widget; пусто — вход через Telegram выключен |
| `SMTP_ADDR` | пусто | `host:port` SMTP-сервера; пусто — письма пишутся в лог |
| `SMTP_FROM` | `Botsight <no-reply@botsight.ru>` | отправитель писем |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | пусто | авторизация на SMTP-сервере |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `CLICKHOUSE_ADDR`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | пусто, `botsight`, `default`, пусто | ClickHouse для тем и алертов; без адреса эти эндпоинты отвечают `503` |
| `AI_URL` | пусто | адрес `ai-worker` для `POST /ask`; пусто — вопросы выключены |
| `AI_INTERNAL_TOKEN` | пусто | общий с `ai-worker` токен |

## Настройки `collector`

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL`, `REDIS_URL` | — | обязательны: ключи при промахе кэша; дедупликация, лимиты, отзыв ключей |
| `KAFKA_BROKERS` | — | брокеры через запятую, обязательна |
| `COLLECTOR_ADDR` | `:8081` | адрес HTTP-сервера |
| `INGEST_RATE` | `2000` | событий в секунду на проект |
| `INGEST_BURST` | `20000` | запас токен-бакета проекта, не меньше 500 |
| `INGEST_MAX_INFLIGHT` | `512` | одновременных батчей на инстанс, сверх — `503 overloaded` |
| `KEY_CACHE_TTL` | `60s` | сколько ключ живёт в кэше |
| `KAFKA_CREATE_TOPICS` | `true` | создать недостающие топики при старте |
| `KAFKA_REPLICATION_FACTOR` | `1` | фактор репликации создаваемых топиков (3 в продакшене) |

## Настройки `chwriter`

| Переменная | По умолчанию | Описание |
|---|---|---|
| `REDIS_URL` | — | планы пачек для дедупликации после падения, обязательна |
| `KAFKA_BROKERS` | — | обязательна |
| `CLICKHOUSE_ADDR` | — | `host:9000`, нативный протокол, обязательна |
| `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | `botsight`, `default`, пусто | доступ к ClickHouse |
| `CHWRITER_ADDR` | `:8082` | `/healthz`, `/readyz`, `/metrics` |
| `CHWRITER_GROUP` | `ch-writer` | consumer group |
| `CHWRITER_BATCH_ROWS` | `50000` | строк в одной вставке |
| `CHWRITER_FLUSH_INTERVAL` | `1s` | максимальный возраст записи в буфере |
| `KAFKA_CREATE_TOPICS`, `KAFKA_REPLICATION_FACTOR` | `true`, `1` | как у collector |

## Миграции

Файлы `migrations/postgres/` и `migrations/clickhouse/` — `NNNNNN_name.up.sql` и `.down.sql` в формате [golang-migrate](https://github.com/golang-migrate/migrate). Новая миграция — следующий номер. В docker compose их применяют контейнеры `migrate` и `migrate-clickhouse`, тесты применяют все `*.up.sql` по порядку.

В миграциях ClickHouse операторы разделяются точкой с запятой, поэтому в комментариях и строках её быть не должно.
