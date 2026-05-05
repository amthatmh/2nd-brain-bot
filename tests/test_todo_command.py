import asyncio
import importlib
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


REQUIRED_ENV = {
    "TELEGRAM_TOKEN": "x",
    "TELEGRAM_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "x",
    "NOTION_TOKEN": "x",
    "NOTION_DB_ID": "x",
    "NOTION_HABIT_DB": "x",
    "NOTION_LOG_DB": "x",
    "NOTION_STREAK_DB": "x",
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
}


class Msg:
    chat_id = 1

    def __init__(self):
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append((text, kwargs))


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


def test_cmd_todo_builds_picker_with_keyboard_module_helpers():
    async def run():
        main = load_main_module()
        msg = Msg()
        tasks = [{"name": "Pay bill", "context": "Personal", "page_id": "page-1"}]
        keyboard = object()

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=tasks), \
            patch.object(main.kb, "todo_picker_keyboard", return_value=keyboard) as keyboard_mock, \
            patch.object(main.fmt, "context_emoji", return_value="🏠") as emoji_mock:
            await main.cmd_todo(msg)

        assert msg.calls == [
            (
                "✅ *What did you get done?*",
                {"parse_mode": "Markdown", "reply_markup": keyboard},
            )
        ]
        key = next(iter(main.todo_picker_map))
        keyboard_mock.assert_called_once_with(key, main.todo_picker_map, emoji_mock)
        assert main.todo_picker_map[key] == tasks

    asyncio.run(run())


def test_todo_callback_rerenders_remaining_picker_with_keyboard_module_helpers():
    async def run():
        main = load_main_module()
        main.todo_picker_map.clear()
        main.todo_picker_map["0"] = [
            {"name": "Pay bill", "context": "Personal", "page_id": "page-1"},
            {"name": "Email Sam", "context": "Work", "page_id": "page-2"},
        ]
        keyboard = object()
        query = SimpleNamespace(data="td:0:0")
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        update = SimpleNamespace(callback_query=query)

        with patch.object(main, "handle_v10_callback", side_effect=_async_false), \
            patch.object(main.notion_tasks, "mark_done") as mark_done, \
            patch.object(main.notion_tasks, "handle_done_recurring", return_value=False), \
            patch.object(main.kb, "todo_picker_keyboard", return_value=keyboard) as keyboard_mock, \
            patch.object(main.fmt, "context_emoji", return_value="🏠") as emoji_mock:
            await main.handle_callback(update, None)

        mark_done.assert_called_once_with(main.notion, "page-1")
        assert main.todo_picker_map["0"][0]["_done"] is True
        keyboard_mock.assert_called_once_with("0", main.todo_picker_map, emoji_mock)
        query.edit_message_text.assert_awaited_once_with(
            "✅ 1 done · 1 remaining",
            reply_markup=keyboard,
        )

    asyncio.run(run())


async def _async_false(*_args, **_kwargs):
    return False


def test_todo_picker_keyboard_includes_cancel_button():
    main = load_main_module()
    keyboard = main.kb.todo_picker_keyboard(
        "7",
        {"7": [{"name": "Pay bill", "context": "Personal", "page_id": "page-1"}]},
        lambda _context: "🏠",
    )

    cancel_button = keyboard.inline_keyboard[-1][0]
    assert cancel_button.text == "✖️ Cancel"
    assert cancel_button.callback_data == "tdc:7"


def test_todo_cancel_callback_dismisses_picker():
    async def run():
        main = load_main_module()
        main.todo_picker_map.clear()
        main.todo_picker_map["0"] = [
            {"name": "Pay bill", "context": "Personal", "page_id": "page-1"},
        ]
        query = SimpleNamespace(data="tdc:0")
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        update = SimpleNamespace(callback_query=query)

        with patch.object(main, "handle_v10_callback", side_effect=_async_false):
            await main.handle_callback(update, None)

        assert "0" not in main.todo_picker_map
        query.edit_message_text.assert_awaited_once_with("✖️ To Do picker canceled.")

    asyncio.run(run())
