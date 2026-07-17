import asyncio
import importlib
import os
import sys
from unittest.mock import MagicMock, patch

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


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), patch(
        "notion_client.Client", return_value=MagicMock()
    ), patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


def test_task_without_deadline_defaults_to_seven_days():
    async def run():
        main = load_main_module()
        classification = {
            "type": "task",
            "task_name": "Buy Belkin charger",
            "deadline_days": None,
            "context": "🏠 Personal",
            "confidence": "high",
        }
        with patch.object(
            main.notion_tasks, "create_task", return_value=("page-1", None)
        ) as create, patch.object(
            main.notion_tasks, "find_duplicate_active_task", return_value=None
        ):
            result = await main._create_task_from_classification(
                "Buy Belkin charger", classification, None, None, False
            )

        assert result["status"] == "captured"
        assert create.call_args.args[3] == 7
        assert result["horizon_label"] == "🟠 This Week"

    asyncio.run(run())


def test_task_explicit_deadline_is_kept():
    async def run():
        main = load_main_module()
        classification = {
            "type": "task",
            "task_name": "File taxes",
            "deadline_days": 2,
            "context": "🏠 Personal",
            "confidence": "high",
        }
        with patch.object(
            main.notion_tasks, "create_task", return_value=("page-1", None)
        ) as create, patch.object(
            main.notion_tasks, "find_duplicate_active_task", return_value=None
        ):
            await main._create_task_from_classification(
                "File taxes", classification, None, None, False
            )

        assert create.call_args.args[3] == 2

    asyncio.run(run())


def test_todo_picker_keyboard_has_quick_add_row():
    main = load_main_module()
    keyboard = main.kb.todo_picker_keyboard(
        "7",
        {"7": [{"name": "Pay bill", "context": "Personal", "page_id": "page-1"}]},
        lambda _context: "🏠",
    )

    quick_add_row = keyboard.inline_keyboard[-2]
    personal, work = quick_add_row
    assert personal.callback_data == "tda:personal"
    assert work.callback_data == "tda:work"
    assert "Personal" in personal.text
    assert "Work" in work.text
