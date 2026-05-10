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


def test_deadline_prop_uses_local_today_for_offsets():
    main = load_main_module()

    with patch("second_brain.notion.tasks.local_today", return_value=date(2026, 4, 27)):
        assert main.notion_tasks._deadline_prop(0) == {"date": {"start": "2026-04-27"}}
        assert main.notion_tasks._deadline_prop(1) == {"date": {"start": "2026-04-28"}}


def test_next_weekday_is_calculated_from_local_today():
    main = load_main_module()

    # Monday from a Sunday should be next day.
    with patch.object(main, "local_today", return_value=date(2026, 4, 26)):
        assert main.next_weekday(0) == date(2026, 4, 27)
