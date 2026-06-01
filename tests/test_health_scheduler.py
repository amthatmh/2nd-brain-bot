"""Tests for health Utility Scheduler jobs."""

from __future__ import annotations

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.scheduler import check_and_create_steps_entry, sleep_backfill_job


class TestCheckAndCreateStepsEntry(IsolatedAsyncioTestCase):
    async def test_returns_exists_when_today_steps_entry_is_present(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 8500}}
        }

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch("second_brain.healthtrack.scheduler._find_steps_habit_page_id", return_value="habit-page"), \
             patch("second_brain.healthtrack.scheduler._find_existing_log_entry", return_value="log-page"):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "exists")
        self.assertEqual(result["page_id"], "log-page")
        self.assertEqual(result["steps_count"], 8500)
        notion.pages.create.assert_not_called()

    async def test_creates_placeholder_with_blank_steps_count_when_missing(self):
        notion = MagicMock()
        notion.pages.create.return_value = {"id": "created-page"}
        bot = AsyncMock()

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch("second_brain.healthtrack.scheduler._find_steps_habit_page_id", return_value="habit-page"), \
             patch("second_brain.healthtrack.scheduler._find_existing_log_entry", return_value=None):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
                chat_id=123,
                bot=bot,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "created")
        self.assertEqual(result["page_id"], "created-page")
        create_kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(create_kwargs["parent"], {"database_id": "log-db"})
        props = create_kwargs["properties"]
        self.assertEqual(props["Date"], {"date": {"start": "2026-05-07"}})
        self.assertEqual(props["Habit"], {"relation": [{"id": "habit-page"}]})
        self.assertEqual(props["Source"], {"select": {"name": "Scheduler"}})
        self.assertEqual(props["Entry"], {"title": [{"text": {"content": "Steps"}}]})
        self.assertNotIn("Steps Count", props)
        bot.send_message.assert_not_awaited()

    async def test_returns_error_when_steps_habit_cannot_be_resolved(self):
        notion = MagicMock()

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch("second_brain.healthtrack.scheduler._find_steps_habit_page_id", return_value=None):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["action"], "error")
        notion.pages.create.assert_not_called()


class TestSleepBackfillJob(IsolatedAsyncioTestCase):
    async def test_backfills_qualifying_sleep_rows_only(self):
        notion = MagicMock()
        habit_cache = {
            "Sleep": {
                "page_id": "sleep-page",
                "name": "Sleep",
                "auto_only": True,
            }
        }
        health_rows = [
            {
                "properties": {
                    "Date": {"date": {"start": "2026-05-27"}},
                    "Time in Bed hrs": {"number": 7.5},
                }
            },
            {
                "properties": {
                    "Date": {"date": {"start": "2026-05-28"}},
                    "Time in Bed hrs": {"number": 6.5},
                }
            },
            {
                "properties": {
                    "Date": {"date": {"start": "2026-05-29"}},
                    "Time in Bed hrs": {"number": 8.0},
                }
            },
        ]

        with patch(
            "second_brain.healthtrack.scheduler.query_all",
            side_effect=[health_rows, [], [{"id": "existing-log"}]],
        ), patch("second_brain.healthtrack.config.SLEEP_GOAL_HOURS", 7.0):
            result = await sleep_backfill_job(
                notion,
                "log-db",
                "metrics-db",
                habit_cache,
                "America/Chicago",
            )

        self.assertEqual(result, {"status": "done", "logged": 1, "skipped": 2})
        notion.pages.create.assert_called_once()
        create_kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(create_kwargs["parent"], {"database_id": "log-db"})
        props = create_kwargs["properties"]
        self.assertEqual(props["Entry"], {"title": [{"text": {"content": "Sleep"}}]})
        self.assertEqual(props["Habit"], {"relation": [{"id": "sleep-page"}]})
        self.assertEqual(props["Completed"], {"checkbox": True})
        self.assertEqual(props["Date"], {"date": {"start": "2026-05-27"}})
        self.assertEqual(props["Source"], {"select": {"name": "🛌 Auto"}})

    async def test_skips_when_sleep_habit_missing(self):
        notion = MagicMock()

        result = await sleep_backfill_job(
            notion,
            "log-db",
            "metrics-db",
            {},
            "America/Chicago",
        )

        self.assertEqual(result["status"], "skipped")
        notion.pages.create.assert_not_called()
