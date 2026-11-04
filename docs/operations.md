# Эксплуатация Botsight 0.1.0

Как запустить стенд, за чем следить, что делать по алертам и чего не хватает до продакшена. Как система устроена — в [architecture.md](architecture.md), переменные окружения всех сервисов — в [backend/README.md](../backend/README.md) и [ai/README.md](../ai/README.md).

## Стенд в docker compose

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

Поднимаются:
- **хранилища:** PostgreSQL, Redis, Kafka (KRaft, один брокер), ClickHouse, Qdrant;
- **миграции:** контейнеры `migrate` и `migrate-clickhouse` применяют их и завершаются;
- **сервисы:** `api`, `collector`, `chwriter`, `antifraud`, `botproxy`, `ai-worker`;
- **наблюдаемость:** Prometheus, Grafana, Jaeger;
- **почта:** Mailpit — перехватчик писем для локальной проверки.

`toxiproxy` запускается только с профилем `chaos`. Дашборд в compose не входит: он запускается через `npm run dev` (см. [web/README.md](../web/README.md)).

| Адрес | Сервис |
|---|---|
| http://localhost:5173 | дашборд (`cd web && npm run dev`) |
| http://localhost:8080 | `api` |
| http://localhost:8081 | `collector`: `POST /v1/batch` |
| http://localhost:8082 | `chwriter` |
| http://localhost:8083 | `antifraud` |
| http://localhost:8084 | `botproxy`: Bot API по адресу `/k/<api_key>` |
| http://localhost:8090 | `ai-worker`: внутренний API, `/metrics` |
| http://localhost:8025 | Mailpit |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana: анонимный просмотр; вход admin / botsight |
| http://localhost:16686 | Jaeger |
| http://localhost:8123, :9000 | ClickHouse, база `botsight`, пользователь `botsight` / `botsight` |
| localhost:9094 | Kafka |
| localhost:5432 | PostgreSQL, `botsight` / `botsight` |
| localhost:6379 | Redis |
| localhost:6333 | Qdrant |

Все порты открыты только на `127.0.0.1`.

У каждого Go-сервиса есть три служебных эндпоинта:
- `/healthz` — процесс жив;
- `/readyz` — доступны зависимости; collector при остановке сначала перестаёт быть ready и только потом завершается;
- `/metrics`.

### Частые операции

```bash
# демо-данные: аккаунт, проекты, трафик ботов, ботофермы, тексты для AI
cd backend && go run ./cmd/loadgen -projects 3 -users 500 -rate 3000 -duration 30s -texts -fraud 0.2

# поднять лимит проекта для замера пропускной способности
INGEST_RATE=10000000 INGEST_BURST=10000000 docker compose -f deploy/docker-compose.yml up -d collector

# трассировать каждый батч / выключить трассировку
TRACE_RATIO=1 docker compose -f deploy/docker-compose.yml up -d collector
OTEL_EXPORTER_OTLP_ENDPOINT= docker compose -f deploy/docker-compose.yml up -d collector chwriter

# хаос-тесты (≈6 минут)
docker compose -f deploy/docker-compose.yml --profile chaos up -d --build api collector chwriter toxiproxy
loadtest/chaos.sh

# остановить всё; -v удаляет и данные
docker compose -f deploy/docker-compose.yml down
```

## Наблюдаемость

### Метрики и дашборды

Prometheus снимает `/metrics` всех сервисов (`deploy/prometheus/prometheus.yml`). Дашборды Grafana описаны кодом в `deploy/grafana/dashboards.py`, а JSON генерируется из него. Grafana подключает их только на чтение, поэтому изменение дашборда — это изменение кода.

| Дашборд | Что на нём |
|---|---|
| Приём и конвейер | батчи и события в секунду, p99 приёма, свежесть, лаг ch-writer, доля 5xx; ответы collector по кодам, отказы (429, 503, дубли, ошибки Kafka), кэш ключей; лаг по топикам, вставки и их время, DLQ, партиции на паузе; трейсы батчей; антифрод, AI, прокси |
| API и аналитика | запросы и ошибки api, время аналитических запросов по видам, попадания в кэш, потоки `/live` |
| SLO | доступность приёма за 30 дней, остаток бюджета ошибок, скорость его сгорания за 1 и 6 часов, цели по задержке и свежести |

Основные метрики:

| Метрика | Сервис | Смысл |
|---|---|---|
| `botsight_ingest_requests_total{code}` | collector | ответы на `/v1/batch` |
| `botsight_ingest_duration_seconds` | collector | время обработки батча, включая подтверждение Kafka |
| `botsight_ingest_events_total{result}` | collector | события: принятые и отклонённые валидацией |
| `botsight_kafka_produce_errors_total` | collector | батчи, которые не удалось записать в Kafka |
| `botsight_consumer_lag{topic,partition}` | chwriter | записи, ещё не закоммиченные ch-writer |
| `botsight_ch_freshness_seconds{table}` | chwriter | возраст самой старой записи пачки в момент вставки |
| `botsight_ch_insert_rows_total`, `botsight_ch_insert_duration_seconds` | chwriter | вставки в ClickHouse |
| `botsight_dlq_total{topic}` | chwriter | записи в DLQ |
| `botsight_query_duration_seconds{kind}` | api | время аналитических запросов |

### SLO и алерты

Правила — `deploy/prometheus/rules.yml`, тесты на них — `rules_test.yml` (`promtool test rules` в CI). Цели (§17.2):
- 99,9 % батчей принимаются без 5xx за 30 дней;
- p99 приёма < 50 мс;
- свежесть p95 < 10 с;
- p95 аналитики < 1 с.

Алерты на бюджет ошибок построены по схеме Google SRE: две скорости сгорания, каждая проверяется на двух окнах.

| Алерт | Важность | Когда | Что проверить |
|---|---|---|---|
| `IngestErrorBudgetFastBurn` | page | доля 5xx > 1,44 % за 1 ч и за 5 мин | ответы по кодам на дашборде: `503 unavailable` — Kafka или Redis (логи collector, `/readyz`); `overloaded` — не хватает инстансов (`INGEST_MAX_INFLIGHT`) |
| `IngestErrorBudgetSlowBurn` | ticket | доля 5xx > 0,6 % за 6 ч и за 30 мин | то же, без спешки |
| `IngestLatencyHigh` | ticket | p99 приёма > 50 мс 10 мин | спан `kafka produce` в трейсах: фиксация транзакций Kafka; CPU collector |
| `DataNotFresh` | page | p95 свежести > 10 с 5 мин | лаг потребителя, время вставки, партиции на паузе — жив ли ClickHouse |
| `ConsumerLagGrowing` | page | лаг `events.raw` > 100 000 и растёт 10 мин | ошибки вставки в логах ch-writer; не хватает инстансов ch-writer (не больше числа партиций, 24) |
| `DeadLetters` | ticket | в DLQ есть записи за 15 мин | заголовок `error` записей `events.dlq`: обычно несовместимая схема события |
| `DashboardSlow` | ticket | p95 аналитики > 1 с 15 мин | время запросов по `kind`, нагрузка ClickHouse |
| `ServiceDown` | page | `up == 0` 2 мин | контейнер и его логи |

Alertmanager в стенд не входит: алерты видны в Prometheus (http://localhost:9090/alerts). Чтобы получать уведомления, подключите Alertmanager к Prometheus.

### Трассировка

Батч — один трейс OpenTelemetry (§17.3):
1. `POST /v1/batch` в collector с дочерними `redis gate`, `build records` и `kafka produce`;
2. спаны `deliver <таблица>` в ch-writer — по одному на каждую пачку, где оказались записи батча. Каждый длится от записи в Kafka до коммита смещения и отмечает события «пачка взята», «вставлено», «закоммичено».

Трейс отвечает на вопрос «почему этот батч шёл до ClickHouse так долго». Трассируется доля батчей (`OTEL_TRACES_SAMPLER_ARG`, в compose 5 %). Трейсы смотрят в Jaeger или в Grafana: панель «Трейсы батчей» на дашборде конвейера и Explore → Jaeger. Explore доступен только после входа.

Пример из стенда при 2000 событий/с. Collector отвечает за 25 мс, из них 18–22 мс уходит на фиксацию транзакции Kafka. Записи батча расходятся примерно по 20 партициям, и батч целиком оказывается в ClickHouse через 1,3 с. Почти всё это время записи ждут таймера сброса ch-writer (`CHWRITER_FLUSH_INTERVAL`, 1 с).

### Логи

Все Go-сервисы пишут JSON в stdout (`log/slog`), уровень задаёт `LOG_LEVEL`. Строки, записанные в контексте запроса или батча, содержат `trace_id`, по нему находится трейс.

## Безопасность в 0.1.0

Реализовано:

- **Пароли** хранятся как argon2id. Сессии — cookie `HttpOnly`, `Secure` (в продакшене) и `SameSite=Lax`, изменяющие запросы требуют CSRF-токен.
- **API-ключ** показывается один раз, хранится только SHA-256. Отзыв действует на всех инстансах collector сразу: событие отзыва приходит через Redis pub/sub.
- **Лимиты** на вход, регистрацию, вход через Telegram и сброс пароля по IP и по почте (§19.5). Лимит приёма — на проект.
- **Приём:** лимит тела до и после распаковки (защита от zip-бомб) и лимиты размера событий.
- **Изоляция проектов:** `project_id` берётся только из ключа или сессии, запросы к ClickHouse параметризованы, SQL от LLM выполняется под пользователем с row policy на один проект.
- **botproxy:** SSRF закрыт проверкой IP в диалере, вебхуки по http и на частные адреса по умолчанию запрещены.
- **Минимизация данных:** тексты сообщений по умолчанию не собираются. Есть псевдонимизация `user_id` через HMAC с секретом владельца бота.

## Ограничения 0.1.0 и что нужно до продакшена

Версия 0.1.0 — первый рабочий выпуск для самостоятельного развёртывания и демонстрации. Версия 0.x означает, что протокол и API могут меняться.

**Развёртывание:**
- [ ] Есть только docker compose для одной машины: один брокер Kafka (`KAFKA_REPLICATION_FACTOR=1`), одна реплика ClickHouse, Redis и PostgreSQL без реплик. Нужны манифесты для кластера и фактор репликации 3.
- [ ] Пароли в `deploy/docker-compose.yml` (`botsight`, `dev-nl-password-secret`, admin Grafana) — только для локального стенда. Их нужно заменить секретами из менеджера секретов.
- [ ] Нет TLS: его должен завершать балансировщик перед `api`, `collector` и `botproxy`. Также нужно `COOKIE_SECURE=true` и `TRUST_PROXY=true` за балансировщиком.
- [ ] Нет резервного копирования PostgreSQL и ClickHouse.
- [ ] Нет Alertmanager и маршрутизации алертов.

**Продукт и данные:**
- [ ] Удаление данных пользователя по запросу (§19.2, 152-ФЗ) спроектировано, но не реализовано.
- [ ] Хостинга `https://ingest.botsight.ru`, указанного в SDK как адрес по умолчанию, пока нет. При самостоятельном развёртывании адрес collector передаётся в SDK явно (`endpoint=...`, `WithEndpoint(...)`).
- [ ] Python SDK не опубликован в PyPI. Он ставится из git по тегу (см. [README](../README.md#установка-sdk)).
- [ ] Протокол SDK → collector — JSON + gzip. Переход на Protobuf + zstd (§10.5) поменяет контракт, и это будет новая `schema_version`.

**Проверено только на одной машине:**
- [ ] Цели производительности §18.2 замерены на ноутбуке, где генератор и все сервисы делят ядра. Замер на стенде с отдельными машинами не проводился.

## Обновление

- **Миграции** — файлы golang-migrate в `migrations/postgres/` и `migrations/clickhouse/`, применяются по порядку. В compose их применяют контейнеры `migrate` и `migrate-clickhouse` при каждом запуске, уже применённые пропускаются.
- **collector** останавливается без потерь:
  1. `/readyz` начинает отвечать 503;
  2. collector ждёт 2 секунды, пока балансировщик это заметит;
  3. запросы в обработке завершаются вместе с подтверждением Kafka.
- **ch-writer при остановке** досылает буферы (до 10 секунд) и выходит из consumer group. Незакоммиченные записи перечитает следующий владелец партиции без дублей.
- **antifraud после перезапуска** перечитывает 24 часа `events.raw` (`ANTIFRAUD_REPLAY`) и так восстанавливает состояние в памяти.
