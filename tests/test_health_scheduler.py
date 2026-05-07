"""Tests for health Utility Scheduler jobs."""

from __future__ import annotations

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.scheduler import check_and_create_steps_entry


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
        self.assertNotIn("Steps Count", props)
        bot.send_message.assert_awaited_once()

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
