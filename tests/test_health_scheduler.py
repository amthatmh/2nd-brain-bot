"""Tests for health Utility Scheduler jobs."""

from __future__ import annotations

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.scheduler import (
    check_and_create_steps_entry,
    register_handlers,
    sleep_backfill_job,
)


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


class TestWeighJobs(IsolatedAsyncioTestCase):
    async def test_weigh_sync_logs_when_weight_exists_this_week(self):
        notion = MagicMock()
        habit_cache = {
            "Weigh": {"page_id": "habit-page", "name": "Weigh", "auto_only": True}
        }
        health_row = {
            "properties": {
                "Date": {"date": {"start": "2026-05-27"}},
                "Weight (kg)": {"number": 76.8},
            }
        }

        with patch("second_brain.healthtrack.scheduler._current_monday_str", return_value="2026-05-25"), \
             patch("second_brain.healthtrack.scheduler.query_all", return_value=[health_row]) as query_all, \
             patch("second_brain.notion.habits.get_week_completion_count", return_value=0):
            result = await weigh_sync_job(
                notion=notion,
                log_db_id="log-db",
                health_metrics_db_id="health-db",
                habit_cache=habit_cache,
                tz="America/Chicago",
            )

        self.assertEqual(result, {"status": "logged", "date": "2026-05-27", "weight_kg": 76.8})
        query_all.assert_called_once_with(
            notion,
            "health-db",
            filter={
                "and": [
                    {"property": "Date", "date": {"on_or_after": "2026-05-25"}},
                    {"property": "Weight (kg)", "number": {"is_not_empty": True}},
                ]
            },
        )
        create_kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(create_kwargs["parent"], {"database_id": "log-db"})
        props = create_kwargs["properties"]
        self.assertEqual(props["Entry"], {"title": [{"text": {"content": "Weigh"}}]})
        self.assertEqual(props["Habit"], {"relation": [{"id": "habit-page"}]})
        self.assertEqual(props["Completed"], {"checkbox": True})
        self.assertEqual(props["Date"], {"date": {"start": "2026-05-27"}})
        self.assertEqual(props["Source"], {"select": {"name": "Scheduler"}})

    async def test_weigh_sync_skips_when_already_logged_this_week(self):
        notion = MagicMock()
        habit_cache = {
            "Weigh": {"page_id": "habit-page", "name": "Weigh", "auto_only": True}
        }

        with patch("second_brain.healthtrack.scheduler.query_all") as query_all, \
             patch("second_brain.notion.habits.get_week_completion_count", return_value=1):
            result = await weigh_sync_job(
                notion=notion,
                log_db_id="log-db",
                health_metrics_db_id="health-db",
                habit_cache=habit_cache,
                tz="America/Chicago",
            )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "already logged this week")
        query_all.assert_not_called()
        notion.pages.create.assert_not_called()

    async def test_register_handlers_exposes_sleep_backfill(self):
        manager = MagicMock()

        register_handlers(manager)

        registered = {
            call.args[0]: call.args[1]
            for call in manager.register_handler.call_args_list
        }
        self.assertIn("sleep_backfill", registered)
        self.assertTrue(callable(registered["sleep_backfill"]))
