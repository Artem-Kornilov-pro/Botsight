# botsight

Python SDK [Botsight](https://github.com/Artem-Kornilov-pro/Botsight) — аналитика Telegram-ботов на aiogram 3 в две строки кода.

## Установка и подключение

```bash
pip install botsight
```

```python
import os
from botsight import setup

dp.include_router(router)
setup(dp, bot, api_key=os.environ["BOTSIGHT_KEY"])
await dp.start_polling(bot)
```

`setup()` нужно вызвать до `start_polling`. Хендлеры бота менять не нужно. Требуется Python 3.10+ и aiogram 3.10+, других зависимостей нет.

## Что собирается автоматически

| Событие | Откуда |
|---|---|
| `update` | каждый входящий апдейт: тип, команда, метка deep link `/start <метка>`, `callback_data`, длина текста, состояние FSM, обработал ли его какой-нибудь хендлер, время обработки |
| `step` | каждый сработавший хендлер `message`, `callback_query`, `pre_checkout_query`: имя хендлера, переход состояния FSM, исключение, время |
| `outgoing` | каждый запрос к Bot API, кроме `getUpdates`: метод, `chat_id`, длительность, ошибка (`bot_blocked`, `rate_limited`, `bad_request`, …) |
| `lifecycle` | `bot_started` и `bot_stopped` — отметки деплоев |

Тексты сообщений по умолчанию **не отправляются**: только тип, длина, команда и `callback_data`. Содержимое отправленных сообщений и файлов SDK не читает никогда.

## Ручные события

```python
from botsight import Botsight


@router.callback_query(F.data == "pay")
async def pay(cb: CallbackQuery, botsight: Botsight):
    ...
    botsight.track("purchase", user_id=cb.from_user.id, props={"amount": 990, "plan": "pro"})
```

Клиент попадает в хендлер через аргумент `botsight`. Вне хендлеров можно вызвать `botsight.track(...)` на уровне модуля: он использует клиент из последнего `setup()`.

Правила: имя в `snake_case` до 64 символов, не больше 32 свойств, строки до 1024 символов. Числа остаются числами (по ним считаются суммы и перцентили), `bool` превращается в `"true"`/`"false"`, остальное — в строку. Неверные данные пишутся в лог и отбрасываются, исключений нет.

## Параметры

| Параметр | По умолчанию | Описание |
|---|---|---|
| `api_key` | — | ключ проекта из дашборда |
| `endpoint` | `https://ingest.botsight.ru` | адрес приёма (для self-hosted) |
| `capture_text` | `False` | отправлять тексты сообщений (до 4096 символов) |
| `skip_text_in_states` | пусто | состояния, в которых текст не отправляется даже при `capture_text=True`. Можно передать строки `"Form:name"`, объекты `State` или целый `StatesGroup` |
| `redact` | `None` | функция `(text) -> text` для своего маскирования. Вызывается в фоновом потоке. Если она упала, текст не отправляется |
| `pseudonymize_user_ids` | `False` | заменять `user_id` (и `chat_id` личных чатов) на HMAC-SHA256 |
| `pseudonymize_secret` | — | секрет для HMAC, обязателен при `pseudonymize_user_ids=True`. Его нельзя менять: вместе с ним сменятся все псевдонимы |
| `track_fsm` | `True` | отслеживать переходы состояний: одно чтение состояния из хранилища FSM после каждого хендлера |
| `track_blocks` | `True` | подписаться на `my_chat_member`, чтобы сразу видеть блокировки |
| `fallback_handlers` | пусто | запасные хендлеры («не понял»). Апдейт, который попал в них, считается необработанным. Можно передать сами функции или строки `"module:qualname"` |
| `batch_size` | `500` | максимум событий в батче (не больше 500) |
| `flush_interval` | `2.0` | максимум секунд между отправками |
| `max_queue` | `10_000` | лимит очереди в памяти. Сверх него события выбрасываются, их число видно в дашборде |
| `sample_rate` | `1.0` | доля пользователей, по которым собираются события. Выборка по хешу `user_id`, воронки остаются целыми |
| `enabled` | `True` | `False` выключает SDK, например в тестах. `track()` при этом ничего не делает |

Неверные параметры вызывают исключение сразу в `setup()`, при запуске бота.

## Гарантии

- SDK никогда не бросает исключения в код бота и не глотает исключения самого бота.
- На пути апдейта SDK только собирает словарь и кладёт его в очередь. Сеть, сериализация и сжатие работают в фоне.
- Если Botsight недоступен, бот работает как обычно. События копятся в очереди до `max_queue` и досылаются с ретраями до 10 минут.
- При остановке бота SDK досылает очередь не дольше 5 секунд.

## Боты на вебхуках

При `start_polling` aiogram сам запрашивает нужные типы апдейтов. На вебхуках `allowed_updates` задаёте вы, и для отслеживания блокировок в нём должен быть `my_chat_member`:

```python
await bot.set_webhook(url, allowed_updates=dp.resolve_used_update_types())
```

`setup()` должен быть вызван до `resolve_used_update_types()`.

## Ограничения

- Outer-middleware бота на `dp.update`, зарегистрированные до `setup()`, срабатывают раньше SDK. Если такой middleware не пропускает апдейт дальше (например, антиспам), SDK этот апдейт не увидит. Если middleware зарегистрирован после `setup()` и вернул результат без вызова хендлера, апдейт засчитается как обработанный.
- `chat_id` вида `@channel` в исходящих запросах передаётся как `0`.

## Разработка

```bash
cd sdk/python
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                  # тесты
pytest -m benchmark     # бенчмарк накладных расходов
ruff check . && ruff format --check . && mypy
```

## Лицензия

[PolyForm Noncommercial 1.0.0](https://github.com/Artem-Kornilov-pro/Botsight/blob/main/LICENSE). Для коммерческого использования нужно отдельное соглашение с автором.
