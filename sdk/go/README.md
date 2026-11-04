# Botsight Go SDK

Аналитика Telegram-ботов на Go: откуда приходят пользователи, кто блокирует бота, какие хендлеры медленные и падают, какие ошибки возвращает Bot API. SDK работает внутри процесса бота и отправляет только события: токен бота не покидает процесс. Если Botsight недоступен, бот работает как обычно.

```bash
go get github.com/Artem-Kornilov-pro/Botsight/sdk/go@v0.1.0
```

Модуль лежит в подкаталоге репозитория, поэтому его версии — теги вида `sdk/go/v0.1.0`. Нужен Go 1.24 или новее.

Hosted-версии Botsight пока нет. При самостоятельном развёртывании ([быстрый старт](https://github.com/Artem-Kornilov-pro/Botsight#быстрый-старт)) передайте адрес своего collector:

```go
bs, _ := botsight.New(os.Getenv("BOTSIGHT_KEY"), gotelegrambot.Framework(), botsight.WithEndpoint("http://localhost:8081"))
```

## go-telegram/bot — три опции

```go
bs, _ := botsight.New(os.Getenv("BOTSIGHT_KEY"), gotelegrambot.Framework())
defer bs.Close(context.Background())

b, _ := bot.New(token,
	bot.WithHTTPClient(time.Minute, bs.HTTPClient()),        // вызовы Bot API и их ошибки
	bot.WithMiddlewares(gotelegrambot.Middleware(bs)),       // апдейты и время обработки
	bot.WithDefaultHandler(gotelegrambot.Fallback(bs, nil)), // апдейты, которые никто не взял
)
b.RegisterHandler(bot.HandlerTypeMessageText, "/start", bot.MatchTypePrefix,
	gotelegrambot.Named("start", startHandler))              // имя для медленных и падающих хендлеров
```

Middleware видит каждый апдейт, и при long polling, и при вебхуках. Паника хендлера записывается как ошибка шага и пробрасывается дальше, как без Botsight.

## Любой другой фреймворк

```go
client := bs.HTTPClient()      // или bs.Transport(свой RoundTripper)
// апдейты из getUpdates запишутся сами; для вебхуков или своего времени обработки:
bs.Update(botID, rawUpdateJSON, handled, time.Since(start))
bs.Step(botID, "message", "handlers.start", userID, chatID, time.Since(start), err)
```

## Что собирается

| Событие | Откуда |
|---|---|
| `update` | апдейт: тип, команда и метка `/start <метка>`, `callback_data`, длина текста (не сам текст), язык; обработан ли, время обработки |
| `step` | хендлер, отмеченный `Named`: имя, время, ошибка (тип ошибки, а не её текст) |
| `outgoing` | каждый вызов Bot API, кроме `getUpdates`: метод, `chat_id`, время, ошибка (`bot_blocked`, `rate_limited`, `bad_request`, … — как в Python SDK) |
| `lifecycle` | `bot_started` с версиями SDK, фреймворка и Go — отметка деплоя на графиках |
| `custom` | `bs.Track("purchase", userID, map[string]any{"amount": 990})` |

## Как устроено

- **Транспорт.** `http.RoundTripper` передаёт запрос в Telegram потоком: из тела копируются только первые 4 КиБ, чтобы найти `chat_id`, когда в ответе с ошибкой нет `Message`. Ответ читается, разбирается и возвращается боту без изменений. Файлы (`/file/...`) не трогаются.
- **Дубли.** После перезапуска Telegram отдаёт неподтверждённые апдейты повторно. Окно дедупликации по `update_id` (как анти-replay окно IPsec, 40 байт на бота) не даёт посчитать их дважды; сами апдейты до бота доходят всегда.
- **Доставка.** Очередь ограничена (`WithQueueSize`, по умолчанию 10 000): `Track` и транспорт никогда не ждут. Переполнение считается и уходит в поле `dropped` следующего батча, `bs.Dropped()` показывает потери. Батчи сжимаются gzip и повторяются с тем же `batch_id` (сервер примет один раз) с экспоненциальной паузой с джиттером и с учётом `Retry-After`. `401` останавливает отправку до перезапуска, `Close(ctx)` досылает очередь.
- **Без зависимостей.** Ядро использует только стандартную библиотеку; `go-telegram/bot` нужен лишь пакету `gotelegrambot`.

## Тесты

```bash
go test -race ./...
```

Ядро проверяется против фейковых Telegram и collector: дедупликация, `chat_id` из multipart при ошибке, переполнение очереди и отчёт о потерях, 401, отсутствие токена в батчах. Адаптер проверяется на настоящем `go-telegram/bot`: long polling, именованный хендлер, необработанный апдейт, паника.

## Версии

Версия SDK — `botsight.Version`, изменения — в [CHANGELOG.md](https://github.com/Artem-Kornilov-pro/Botsight/blob/main/CHANGELOG.md).

## Лицензия

[PolyForm Noncommercial 1.0.0](https://github.com/Artem-Kornilov-pro/Botsight/blob/main/LICENSE). Для коммерческого использования нужно отдельное соглашение с автором.
