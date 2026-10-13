from __future__ import annotations

from typing import Any

import pytest
from aiogram import Dispatcher, F, Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.methods import GetUpdates, SendMessage, TelegramMethod
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message

from botsight import Botsight, setup
from botsight._names import handler_name

from .conftest import (
    API_KEY,
    BOT_ID,
    callback_update,
    drain,
    make_bot,
    member_update,
    message_update,
    of_type,
)


class Form(StatesGroup):
    name = State()
    group = State()


def build_bot(responder: Any = None, **options: Any) -> tuple[Dispatcher, Any, Botsight, Router]:
    dp = Dispatcher()
    bot = make_bot(responder)
    router = Router()

    @router.message(CommandStart())
    async def cmd_start(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.name)
        await message.answer("Как вас зовут?")

    @router.message(StateFilter(Form.name), F.text.regexp(r"^[А-Яа-яA-Za-z]+ [А-Яа-яA-Za-z]+$"))
    async def got_name(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.group)

    @router.message(StateFilter(Form.name))
    async def bad_name(message: Message) -> None:
        await message.answer("Введите имя и фамилию")

    @router.message(Command("boom"))
    async def boom(message: Message) -> None:
        raise ValueError("handler crashed")

    @router.callback_query(F.data.startswith("lab:"))
    async def cb_lab(cb: CallbackQuery) -> None:
        pass

    dp.include_router(router)
    client = setup(dp, bot, api_key=API_KEY, **options)
    return dp, bot, client, router


async def test_start_with_deep_link() -> None:
    dp, bot, client, _ = build_bot()
    await dp.feed_update(bot, message_update("/start vk_ads_oct", premium=True))
    evs = drain(client)

    [update] = of_type(evs, "update")
    assert update["name"] == "message"
    assert update["bot_id"] == BOT_ID
    assert update["user_id"] == 1 and update["chat_id"] == 1
    assert update["chat_type"] == "private"
    assert update["content_type"] == "text"
    assert update["command"] == "start"
    assert update["start_param"] == "vk_ads_oct"
    assert update["text_len"] == len("/start vk_ads_oct")
    assert "text" not in update  # texts are off by default
    assert update["state"] == ""
    assert update["user"] == {"lang": "ru", "is_premium": True, "has_username": True}
    assert update["handled"] is True
    assert isinstance(update["duration_ms"], int)

    [step] = of_type(evs, "step")
    assert step["name"] == "message"
    assert step["handler"].endswith(":build_bot.<locals>.cmd_start")
    assert step["state_from"] == "" and step["state_to"] == "Form:name"
    assert step["error"] is None

    [out] = of_type(evs, "outgoing")
    assert out["name"] == "sendMessage"
    assert out["chat_id"] == 1 and out["user_id"] == 0
    assert out["ok"] is True and out["error"] is None and out["error_code"] == 0


async def test_command_with_bot_mention_is_normalized() -> None:
    dp, bot, client, _ = build_bot()
    await dp.feed_update(bot, message_update("/START@shop_bot"))
    [update] = of_type(drain(client), "update")
    assert update["command"] == "start"
    assert "start_param" not in update


async def test_retry_on_the_same_step_keeps_state() -> None:
    dp, bot, client, _ = build_bot()
    await dp.feed_update(bot, message_update("/start", update_id=1))
    await dp.feed_update(bot, message_update("Иванов И.И.", update_id=2))
    await dp.feed_update(bot, message_update("Иван Иванов", update_id=3))
    steps = of_type(drain(client), "step")
    assert [(s["state_from"], s["state_to"]) for s in steps] == [
        ("", "Form:name"),
        ("Form:name", "Form:name"),  # "stuck on step": regexp rejected the input
        ("Form:name", "Form:group"),
    ]
    assert steps[1]["handler"].endswith("bad_name")


async def test_unhandled_update() -> None:
    dp, bot, client, _ = build_bot()
    await dp.feed_update(bot, message_update("random text"))
    evs = drain(client)
    [update] = of_type(evs, "update")
    assert update["handled"] is False
    assert of_type(evs, "step") == []


async def test_handler_exception_reaches_the_bot_unchanged() -> None:
    dp, bot, client, _ = build_bot()
    with pytest.raises(ValueError, match="handler crashed"):
        await dp.feed_update(bot, message_update("/boom"))
    evs = drain(client)
    [step] = of_type(evs, "step")
    assert step["error"] == "ValueError"
    [update] = of_type(evs, "update")
    assert update["handled"] is True  # a handler was found, it crashed


async def test_callback_query() -> None:
    dp, bot, client, _ = build_bot()
    await dp.feed_update(bot, callback_update("lab:lab2"))
    evs = drain(client)
    [update] = of_type(evs, "update")
    assert update["name"] == "callback_query"
    assert update["callback_data"] == "lab:lab2"
    [step] = of_type(evs, "step")
    assert step["name"] == "callback_query"
    assert step["handler"].endswith("cb_lab")


async def test_capture_text_respects_skip_states() -> None:
    dp, bot, client, _ = build_bot(capture_text=True, skip_text_in_states=[Form.name])
    await dp.feed_update(bot, message_update("/start", update_id=1))
    await dp.feed_update(bot, message_update("Иван Иванов", update_id=2))
    updates = of_type(drain(client), "update")
    assert updates[0]["text"] == "/start"
    assert updates[1]["state"] == "Form:name"
    assert "text" not in updates[1]
    assert updates[1]["text_len"] == len("Иван Иванов")


async def test_caption_counts_as_text() -> None:
    dp, bot, client, _ = build_bot(capture_text=True)
    await dp.feed_update(bot, message_update(None, caption="photo caption"))
    [update] = of_type(drain(client), "update")
    assert update["text_len"] == len("photo caption")
    assert update["text"] == "photo caption"
    assert "command" not in update


async def test_fallback_handler_marks_update_unhandled() -> None:
    dp = Dispatcher()
    bot = make_bot()
    router = Router()

    @router.message(Command("help"))
    async def help_(message: Message) -> None:
        pass

    @router.message()
    async def fallback(message: Message) -> None:
        pass

    dp.include_router(router)
    client = setup(dp, bot, api_key=API_KEY, fallback_handlers=[fallback])
    await dp.feed_update(bot, message_update("/help", update_id=1))
    await dp.feed_update(bot, message_update("what?", update_id=2))
    updates = of_type(drain(client), "update")
    assert [u["handled"] for u in updates] == [True, False]


async def test_fallback_handler_by_name() -> None:
    dp = Dispatcher()
    bot = make_bot()

    @dp.message()
    async def fallback(message: Message) -> None:
        pass

    client = setup(dp, bot, api_key=API_KEY, fallback_handlers=[handler_name(fallback)])
    await dp.feed_update(bot, message_update("what?"))
    [update] = of_type(drain(client), "update")
    assert update["handled"] is False


async def test_block_via_my_chat_member() -> None:
    dp, bot, client, _ = build_bot()
    assert "my_chat_member" in dp.resolve_used_update_types()
    await dp.feed_update(bot, member_update(kicked=True))
    [update] = of_type(drain(client), "update")
    assert update["name"] == "my_chat_member"
    assert update["member_status"] == "kicked"
    assert update["handled"] is True


async def test_track_blocks_off() -> None:
    dp, _, _, _ = build_bot(track_blocks=False)
    assert "my_chat_member" not in dp.resolve_used_update_types()


async def test_sdk_router_never_shadows_bot_handlers() -> None:
    """Even when setup() is called before the bot's router is included."""
    dp = Dispatcher()
    bot = make_bot()
    client = setup(dp, bot, api_key=API_KEY)
    seen: list[str] = []
    router = Router()

    @router.my_chat_member()
    async def on_member(event: ChatMemberUpdated) -> None:
        seen.append(event.new_chat_member.status)

    dp.include_router(router)
    await dp.feed_update(bot, member_update(kicked=True))
    assert seen == ["kicked"]
    [update] = of_type(drain(client), "update")
    assert update["handled"] is True


async def test_outgoing_error_is_normalized_and_reraised() -> None:
    def responder(method: TelegramMethod[Any]) -> Any:
        raise TelegramForbiddenError(
            method=method, message="Forbidden: bot was blocked by the user"
        )

    _dp, bot, client, _ = build_bot(responder)
    with pytest.raises(TelegramForbiddenError):
        await bot.send_message(chat_id=7, text="hi")
    [out] = of_type(drain(client), "outgoing")
    assert out == {
        **out,
        "name": "sendMessage",
        "chat_id": 7,
        "ok": False,
        "error_code": 403,
        "error": "bot_blocked",
    }


async def test_outgoing_skips_get_updates_and_string_chat_ids() -> None:
    _dp, bot, client, _ = build_bot()
    await bot(GetUpdates())
    await bot(SendMessage(chat_id="@channel", text="hi"))
    [out] = of_type(drain(client), "outgoing")
    assert out["name"] == "sendMessage" and out["chat_id"] == 0


async def test_track_fsm_off() -> None:
    dp, bot, client, _ = build_bot(track_fsm=False)
    await dp.feed_update(bot, message_update("/start"))
    [step] = of_type(drain(client), "step")
    assert step["state_from"] == "" and step["state_to"] == ""
    assert step["handler"].endswith("cmd_start")


async def test_sampling_is_per_user_and_stable() -> None:
    dp, bot, client, _ = build_bot(sample_rate=0.5)
    kept = set()
    for uid in range(1, 201):
        await dp.feed_update(bot, message_update("/start", user_id=uid, update_id=uid))
    for ev in drain(client):
        if ev["type"] in ("update", "step"):
            kept.add(ev["user_id"])
    assert 60 < len(kept) < 140
    for uid in range(1, 201):
        assert client._sampled(uid) == (uid in kept)


async def test_sample_rate_zero_keeps_nothing_user_related() -> None:
    dp, bot, client, _ = build_bot(sample_rate=0.0)
    await dp.feed_update(bot, message_update("/start"))
    assert drain(client) == []


async def test_disabled_sdk_installs_nothing() -> None:
    dp = Dispatcher()
    bot = make_bot()
    client = setup(dp, bot, api_key="", enabled=False)
    assert dp["botsight"] is client
    assert len(dp.update.outer_middleware) == 3  # only aiogram's own
    client.track("purchase", user_id=1)
    assert drain(client) == []


async def test_setup_twice_reuses_client() -> None:
    dp = Dispatcher()
    bot = make_bot()
    first = setup(dp, bot, api_key=API_KEY)
    second = setup(dp, bot, api_key=API_KEY)
    assert first is second
    assert len(dp.sub_routers) == 1


async def test_client_is_injected_into_handlers() -> None:
    dp = Dispatcher()
    bot = make_bot()

    @dp.message(Command("buy"))
    async def buy(message: Message, botsight: Botsight) -> None:
        botsight.track("purchase", user_id=message.from_user, props={"amount": 990})

    client = setup(dp, bot, api_key=API_KEY)
    await dp.feed_update(bot, message_update("/buy"))
    [custom] = of_type(drain(client), "custom")
    assert custom["name"] == "purchase"
    assert custom["user_id"] == 1
    assert custom["props"] == {"amount": 990}
