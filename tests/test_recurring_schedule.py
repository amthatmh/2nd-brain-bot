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


def test_monthly_supports_any_ordinal_day():
    main = load_main_module()
    template = {
        "recurring": "🗓️ Monthly",
        "repeat_day": "4th",
        "last_generated": None,
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 4))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 5))


def test_monthly_31st_rolls_to_month_end_for_short_months():
    main = load_main_module()
    template = {
        "recurring": "🗓️ Monthly",
        "repeat_day": "31st",
        "last_generated": None,
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 30))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 29))


def test_quarterly_uses_anchor_month_and_repeat_day():
    main = load_main_module()
    template = {
        "recurring": "📆 Quarterly",
        "repeat_day": "4th",
        "last_generated": "2026-01-04",
        "deadline": "2026-01-04",
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 4))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 5, 4))


def test_quarterly_last_day_rolls_to_month_end():
    main = load_main_module()
    template = {
        "recurring": "📆 Quarterly",
        "repeat_day": "31st",
        "last_generated": "2026-01-31",
        "deadline": "2026-01-31",
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 30))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 29))


def test_next_repeat_day_date_monthly_uses_future_ordinal_day():
    main = load_main_module()
    target = main.next_repeat_day_date("🗓️ Monthly", "4th", date(2026, 4, 1))
    assert target == date(2026, 4, 4)


def test_next_repeat_day_date_monthly_rolls_to_next_month_after_day_passes():
    main = load_main_module()
    target = main.next_repeat_day_date("🗓️ Monthly", "4th", date(2026, 4, 5))
    assert target == date(2026, 5, 4)
