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
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


def test_split_tasks_keeps_multiline_schedule_command_together():
    main = load_main_module()
    text = "Schedule recurring tasks for\nMonthly Auditing\non every 4th day of the month"

    assert main.split_tasks(text) == [text]


def test_split_tasks_still_splits_clear_batch_lines():
    main = load_main_module()
    text = "Buy milk\nCall mom\nFile expenses"

    assert main.split_tasks(text) == ["Buy milk", "Call mom", "File expenses"]
