"""Tests for week-level streak calculation in habits_data_handler."""

import importlib
import json
import os
import sys
import unittest
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


def _streak_row(week_of: str, goal_met: bool) -> dict:
    return {
        "properties": {
            "Week Of": {"date": {"start": week_of}},
            "Goal Met": {"checkbox": goal_met},
        }
    }


class TestWeekStreakInHabitsDataHandler(unittest.IsolatedAsyncioTestCase):
    async def test_empty_streak_db_returns_zero(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        with patch.object(main, "notion_query_all", side_effect=[[], []]):
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 0)

    async def test_three_consecutive_goal_met_weeks(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        streak_rows = [
            _streak_row("2026-04-20", True),
            _streak_row("2026-04-13", True),
            _streak_row("2026-04-06", True),
        ]
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]):
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 3)

    async def test_gap_week_resets_streak(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        # T, F, T, T should produce streak 1 (resets at first false week).
        streak_rows = [
            _streak_row("2026-04-20", True),
            _streak_row("2026-04-13", False),
            _streak_row("2026-04-06", True),
            _streak_row("2026-03-30", True),
        ]
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]):
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 1)

    async def test_non_consecutive_week_stops_streak(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        streak_rows = [
            _streak_row("2026-04-20", True),
            _streak_row("2026-04-06", True),
            _streak_row("2026-03-30", True),
        ]
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]):
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 1)


if __name__ == "__main__":
    unittest.main()
