import importlib
import os
import sys
from datetime import date
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


def test_reminder_snapshot_uses_deadline_for_today_count():
    main = load_main_module()
    tasks = [
        {
            "page_id": "1",
            "name": "Tomorrow task mislabeled as today",
            "context": "💼 Work",
            "auto_horizon": "🔴 Today",
            "deadline": "2026-04-27",
        }
    ]

    with patch.object(main, "local_today", return_value=date(2026, 4, 26)), \
        patch("second_brain.notion.tasks.get_all_active_tasks", return_value=tasks), \
        patch.object(main, "get_quick_refresh_tasks", return_value=tasks):
        snapshot = main.format_reminder_snapshot(mode="priority", limit=8)

    assert "Today: *0*" in snapshot
