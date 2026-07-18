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
        self.assertEqual(payload["habits"][0]["dayStreak"], 0)
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
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["dayStreak"], 0)
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
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["dayStreak"], 0)
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
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["dayStreak"], 0)
        self.assertEqual(payload["habits"][0]["weekStreak"], 1)

    async def test_duplicate_week_rows_favor_goal_met(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        # Duplicate rows for the same week can exist historically; True should win.
        streak_rows = [
            _streak_row("2026-04-20", False),
            _streak_row("2026-04-20", True),
            _streak_row("2026-04-13", True),
        ]
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 2)

    async def test_current_week_row_does_not_reset_streak(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        # Current week can be in progress and not met yet; it should be ignored.
        streak_rows = [
            _streak_row("2026-04-27", False),
            _streak_row("2026-04-20", True),
            _streak_row("2026-04-13", True),
        ]
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all", side_effect=[[], streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 2)

    async def test_day_streak_counts_consecutive_recent_completions(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        # Generate logs for the final 3 days in the response window.
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        with patch.object(main, "notion_query_all") as mocked_query, \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            mocked_query.side_effect = [
                [
                    {
                        "properties": {
                            "Date": {"date": {"start": "2026-04-25"}},
                            "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                        }
                    },
                    {
                        "properties": {
                            "Date": {"date": {"start": "2026-04-26"}},
                            "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                        }
                    },
                    {
                        "properties": {
                            "Date": {"date": {"start": "2026-04-27"}},
                            "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                        }
                    },
                ],
                [],
            ]
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["dayStreak"], 3)

    async def test_day_streak_survives_unlogged_today(self):
        main = load_main_module()
        main.habit_cache = {
            "Workout": {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        }

        # Done the two days before today, but today (in progress) not logged
        # yet -> the streak must show 2, not reset to 0.
        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        logs = [
            {
                "properties": {
                    "Date": {"date": {"start": day}},
                    "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                }
            }
            for day in ("2026-04-25", "2026-04-26")
        ]
        with patch.object(main, "notion_query_all", side_effect=[logs, []]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["dayStreak"], 2)
        self.assertFalse(payload["habits"][0]["todayDone"])

    async def test_week_streak_falls_back_to_logs_when_streak_rows_missing(self):
        main = load_main_module()
        main.habit_cache = {
            "Protein Shake": {
                "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "name": "Protein Shake",
                "sort": 1,
                "freq_per_week": 5,
                "frequency_label": "5x/week",
            }
        }

        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        logs = [
            {
                "properties": {
                    "Date": {"date": {"start": day}},
                    "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                }
            }
            for day in ("2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24")
        ]
        with patch.object(main, "notion_query_all", side_effect=[logs, []]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 1)

    def test_week_streak_ignores_partial_first_window_week(self):
        # The oldest window week is usually partial (the window rarely starts
        # on a Monday). Goal-met must not be derived from its truncated logs;
        # an authoritative streak row for that week has to stand.
        load_main_module()
        from datetime import datetime as real_datetime, timezone

        from second_brain.healthtrack.routes import _build_habits_data_payload

        class FakeDatetime(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 29, tzinfo=tz)  # Wednesday

        # weeks_history=2 -> window starts Thu 2026-04-16; first week
        # (Mon 2026-04-13) is partial. Logs meet the 3x target for the one
        # full completed week (Apr 20-26); the partial week's goal-met comes
        # from its streak row.
        logs = [
            {
                "properties": {
                    "Date": {"date": {"start": day}},
                    "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                }
            }
            for day in ("2026-04-20", "2026-04-21", "2026-04-22")
        ]
        streak_rows = [_streak_row("2026-04-13", True)]

        payload = _build_habits_data_payload(
            habit_cache={
                "Steps": {
                    "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "name": "Steps",
                    "sort": 1,
                    "freq_per_week": 3,
                    "frequency_label": "3x/week",
                }
            },
            log_db="log",
            habit_db="habit",
            streak_db="streak",
            tz=timezone.utc,
            weeks_history=2,
            query_all_fn=MagicMock(side_effect=[logs, streak_rows]),
            extract_date_fn=lambda s: (s or "")[:10] or None,
            datetime_cls=FakeDatetime,
        )

        self.assertEqual(payload["habits"][0]["weekStreak"], 2)

    async def test_week_streak_uses_logs_when_existing_row_is_stale(self):
        main = load_main_module()
        main.habit_cache = {
            "Water2L": {
                "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "name": "Water2L",
                "sort": 1,
                "freq_per_week": 5,
                "frequency_label": "5x/week",
            }
        }

        fake_today = main.datetime(2026, 4, 27, tzinfo=main.TZ)
        real_fromisoformat = main.datetime.fromisoformat
        logs = [
            {
                "properties": {
                    "Date": {"date": {"start": day}},
                    "Habit": {"relation": [{"id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
                }
            }
            for day in ("2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24")
        ]
        stale_streak_rows = [_streak_row("2026-04-20", False)]
        with patch.object(main, "notion_query_all", side_effect=[logs, stale_streak_rows]), \
            patch.object(main, "datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fake_today
            mocked_datetime.fromisoformat.side_effect = real_fromisoformat
            response = await main.habits_data_handler(MagicMock())

        payload = json.loads(response.text)
        self.assertEqual(payload["habits"][0]["weekStreak"], 1)


if __name__ == "__main__":
    unittest.main()
