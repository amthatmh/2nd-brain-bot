from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def test_telegram_error_location_names_crossfit_notion_save():
    from second_brain.error_reporting import telegram_error_location

    namespace = {}
    code = compile(
        "def create_wod_log():\n    raise RuntimeError('boom')\n",
        "/tmp/app/second_brain/crossfit/notion.py",
        "exec",
    )
    exec(code, namespace)

    try:
        namespace["create_wod_log"]()
    except RuntimeError as exc:
        location = telegram_error_location(exc)

    assert location == "CrossFit Notion save (second_brain.crossfit.notion.create_wod_log)"


def _exec_at(source: str, filename: str, namespace: dict) -> None:
    exec(compile(source, filename, "exec"), namespace)


def test_telegram_error_location_skips_notion_call_wrapper_frame():
    from second_brain.error_reporting import telegram_error_location

    namespace = {}
    _exec_at(
        "def client_fn():\n    raise RuntimeError('400 validation')\n",
        "/tmp/app/notion_client/api.py",
        namespace,
    )
    _exec_at(
        "def notion_call(fn):\n    return fn()\n",
        "/tmp/app/second_brain/notion/__init__.py",
        namespace,
    )
    _exec_at(
        "def create_strength_log(notion_call, fn):\n    return notion_call(fn)\n",
        "/tmp/app/second_brain/crossfit/notion.py",
        namespace,
    )

    try:
        namespace["create_strength_log"](namespace["notion_call"], namespace["client_fn"])
    except RuntimeError as exc:
        location = telegram_error_location(exc)

    assert location == "CrossFit Notion save (second_brain.crossfit.notion.create_strength_log)"


def test_telegram_error_location_falls_back_to_wrapper_when_no_other_frame():
    from second_brain.error_reporting import telegram_error_location

    namespace = {}
    _exec_at(
        "def client_fn():\n    raise RuntimeError('boom')\n",
        "/tmp/app/notion_client/api.py",
        namespace,
    )
    _exec_at(
        "def notion_call(fn):\n    return fn()\n",
        "/tmp/app/second_brain/notion/__init__.py",
        namespace,
    )

    try:
        namespace["notion_call"](namespace["client_fn"])
    except RuntimeError as exc:
        location = telegram_error_location(exc)

    assert location == "Notion write (second_brain.notion.__init__.notion_call)"


@pytest.mark.asyncio
async def test_send_system_log_skips_main_chat_when_system_chat_not_separate():
    from second_brain.error_reporting import send_system_log

    bot = SimpleNamespace(send_message=AsyncMock())
    fake_config = SimpleNamespace(MY_CHAT_ID=123, ERROR_CHANNEL_ID=123)

    with patch.dict("sys.modules", {"second_brain.config": fake_config}):
        await send_system_log(bot, "boom")

    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_system_log_sends_to_separate_system_chat():
    from second_brain.error_reporting import send_system_log

    bot = SimpleNamespace(send_message=AsyncMock())
    fake_config = SimpleNamespace(MY_CHAT_ID=123, ERROR_CHANNEL_ID=456)

    with patch.dict("sys.modules", {"second_brain.config": fake_config}):
        await send_system_log(bot, "boom")

    bot.send_message.assert_awaited_once_with(chat_id=456, text="boom")
