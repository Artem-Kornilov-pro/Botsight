# backend

Go-модуль с сервисами Botsight. Сейчас реализован `api` — control plane дашборда на PostgreSQL: регистрация и вход, проекты, участники, API-ключи, сохранённые воронки и расходы на рекламу. Подробно — в разделах 12.4, 13.3 и 16.3 [Botsight.md](../Botsight.md), эндпоинты — в [api/openapi.yaml](api/openapi.yaml).

## Локальный запуск

Нужен Docker.

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Команду запускайте из корня репозитория. Поднимутся PostgreSQL, Redis и Mailpit, применятся миграции из `migrations/postgres/`, запустится `api`.

| Адрес | Что там |
|---|---|
| http://localhost:8080 | API (`/healthz`, `/readyz`, `/v1/...`) |
| http://localhost:8025 | Mailpit: письма подтверждения почты и сброса пароля |
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

Вход через Telegram включается переменной `TELEGRAM_BOT_TOKEN` (токен бота, к которому привязан Login Widget):

```bash
TELEGRAM_BOT_TOKEN=123:ABC docker compose -f deploy/docker-compose.yml up --build
```

## Тесты

```bash
cd backend
go test -race ./...
```

Интеграционные тесты сами поднимают PostgreSQL и Redis через testcontainers, нужен запущенный Docker. Без Docker они пропускаются локально и падают в CI.

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

## Миграции

Файлы `migrations/postgres/NNNNNN_name.up.sql` и `.down.sql` в формате [golang-migrate](https://github.com/golang-migrate/migrate). Новая миграция — следующий номер. В docker compose их применяет контейнер `migrate`, тесты применяют все `*.up.sql` по порядку.
