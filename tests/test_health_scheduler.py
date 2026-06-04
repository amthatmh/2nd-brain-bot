"""Tests for health Utility Scheduler jobs."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.scheduler import (
    check_and_create_steps_entry,
    register_handlers,
    sleep_backfill_job,
    weigh_sync_job,
)


def _fake_steps_mod(habit_page_id, existing_log_id, *, last_steps=0, update_ok=True):
    state = {"last_steps": last_steps, "notion_page_id": None}
    existing_log_ids = existing_log_id if isinstance(existing_log_id, list) else [existing_log_id] if existing_log_id else []
    return SimpleNamespace(
        _state=state,
        _find_steps_habit_page_id=MagicMock(return_value=habit_page_id),
        _find_existing_log_entry=MagicMock(return_value=existing_log_ids),
        _date_state=MagicMock(return_value=state),
        _update_log_entry_steps=MagicMock(return_value=update_ok),
    )


class TestCheckAndCreateStepsEntry(IsolatedAsyncioTestCase):
    async def test_updates_existing_steps_entry_on_interval(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 8500}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page")

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["page_id"], "log-page")
        self.assertEqual(result["steps_count"], 8500)
        fake_steps._update_log_entry_steps.assert_called_once_with(notion, "log-page", 8500, False)
        notion.pages.create.assert_not_called()

    async def test_updates_existing_blank_steps_count_from_cached_state(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": None}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page", last_steps=6255)

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["page_id"], "log-page")
        self.assertEqual(result["steps_count"], 6255)
        self.assertIsNone(result["previous_steps_count"])
        fake_steps._update_log_entry_steps.assert_called_once_with(notion, "log-page", 6255, False)
        notion.pages.create.assert_not_called()

    async def test_updates_existing_zero_steps_count_from_latest_webhook_payload(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 0}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page", last_steps=0)
        fake_routes = SimpleNamespace(
            _last_steps_webhook={
                "parsed": [
                    {"steps": 8100, "date": "2026-05-06"},
                    {"steps": 9500, "date": "2026-05-07"},
                    {"steps": 9200, "date": "2026-05-07"},
                ]
            }
        )

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {
                 "second_brain.healthtrack.steps": fake_steps,
                 "second_brain.healthtrack.routes": fake_routes,
             }):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["page_id"], "log-page")
        self.assertEqual(result["steps_count"], 9500)
        self.assertEqual(fake_steps._state["last_steps"], 9500)
        fake_steps._update_log_entry_steps.assert_called_once_with(notion, "log-page", 9500, False)
        notion.pages.create.assert_not_called()

    async def test_archives_duplicate_zero_steps_entry_after_writing_latest_count(self):
        notion = MagicMock()
        notion.pages.retrieve.side_effect = [
            {"properties": {"Steps Count": {"number": 0}}},
            {"properties": {"Steps Count": {"number": 15266}}},
        ]
        fake_steps = _fake_steps_mod(
            "habit-page",
            ["zero-page", "good-page"],
            last_steps=15266,
        )

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["page_id"], "good-page")
        self.assertEqual(result["steps_count"], 15266)
        fake_steps._update_log_entry_steps.assert_called_once_with(notion, "good-page", 15266, True)
        notion.pages.update.assert_called_once_with(page_id="zero-page", archived=True)
        notion.pages.create.assert_not_called()

    async def test_creates_steps_entry_with_cached_steps_count_when_missing(self):
        notion = MagicMock()
        notion.pages.create.return_value = {"id": "created-page"}
        bot = AsyncMock()
        fake_steps = _fake_steps_mod("habit-page", None, last_steps=6255)

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
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
        self.assertEqual(result["steps_count"], 6255)
        create_kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(create_kwargs["parent"], {"database_id": "log-db"})
        props = create_kwargs["properties"]
        self.assertEqual(props["Date"], {"date": {"start": "2026-05-07"}})
        self.assertEqual(props["Habit"], {"relation": [{"id": "habit-page"}]})
        self.assertEqual(props["Source"], {"select": {"name": "Scheduler"}})
        self.assertEqual(props["Entry"], {"title": [{"text": {"content": "Steps"}}]})
        self.assertEqual(props["Steps Count"], {"number": 6255})
        self.assertEqual(props["Completed"], {"checkbox": False})
        bot.send_message.assert_not_awaited()

    async def test_skips_existing_zero_steps_count_when_cache_is_empty(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 0}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page", last_steps=0)

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["page_id"], "log-page")
        self.assertEqual(result["steps_count"], 0)
        fake_steps._update_log_entry_steps.assert_not_called()
        notion.pages.update.assert_not_called()
        notion.pages.create.assert_not_called()

    async def test_skip_reason_reports_latest_payload_when_today_is_missing(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 0}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page", last_steps=0)
        fake_routes = SimpleNamespace(
            _last_steps_webhook={
                "status": "processed",
                "at": "2026-06-04T02:20:33Z",
                "parsed": [
                    {"steps": 11382, "date": "2026-06-01"},
                    {"steps": 15266, "date": "2026-06-02"},
                ],
            }
        )

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-06-03"), \
             patch.dict("sys.modules", {
                 "second_brain.healthtrack.steps": fake_steps,
                 "second_brain.healthtrack.routes": fake_routes,
             }):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(
            result["reason"],
            "No steps data for 2026-06-03; latest Health Auto payload ended at 2026-06-02 with 15266 steps",
        )
        self.assertEqual(result["latest_payload_date"], "2026-06-02")
        self.assertEqual(result["latest_payload_steps"], 15266)
        self.assertEqual(result["payload_dates"], ["2026-06-01", "2026-06-02"])
        fake_steps._update_log_entry_steps.assert_not_called()
        notion.pages.update.assert_not_called()
        notion.pages.create.assert_not_called()

    async def test_previous_7_day_payload_does_not_mask_cached_today_steps(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 0}}
        }
        fake_steps = _fake_steps_mod("habit-page", "log-page", last_steps=220)
        fake_routes = SimpleNamespace(
            _last_steps_webhook={
                "status": "processed",
                "parsed": [
                    {"steps": 11382, "date": "2026-06-01"},
                    {"steps": 15266, "date": "2026-06-02"},
                ],
            }
        )

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-06-03"), \
             patch.dict("sys.modules", {
                 "second_brain.healthtrack.steps": fake_steps,
                 "second_brain.healthtrack.routes": fake_routes,
             }):
            result = await check_and_create_steps_entry(
                notion=notion,
                habit_db_id="habits-db",
                log_db_id="log-db",
                habit_name="Steps",
                tz="America/Chicago",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["steps_count"], 220)
        fake_steps._update_log_entry_steps.assert_called_once_with(notion, "log-page", 220, False)
        notion.pages.create.assert_not_called()

    async def test_skips_missing_steps_entry_when_cache_is_empty(self):
        notion = MagicMock()
        bot = AsyncMock()
        fake_steps = _fake_steps_mod("habit-page", None, last_steps=0)

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": fake_steps}):
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
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["steps_count"], 0)
        notion.pages.create.assert_not_called()
        bot.send_message.assert_not_awaited()

    async def test_returns_error_when_steps_habit_cannot_be_resolved(self):
        notion = MagicMock()

        with patch("second_brain.healthtrack.scheduler._today_str", return_value="2026-05-07"), \
             patch.dict("sys.modules", {"second_brain.healthtrack.steps": _fake_steps_mod(None, None)}):
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

        fake_habits = SimpleNamespace(get_week_completion_count=MagicMock(return_value=0))
        with patch("second_brain.healthtrack.scheduler._current_monday_str", return_value="2026-05-25"), \
             patch("second_brain.healthtrack.scheduler.query_all", return_value=[health_row]) as query_all, \
             patch.dict("sys.modules", {"second_brain.notion.habits": fake_habits}):
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

        fake_habits = SimpleNamespace(get_week_completion_count=MagicMock(return_value=1))
        with patch("second_brain.healthtrack.scheduler.query_all") as query_all, \
             patch.dict("sys.modules", {"second_brain.notion.habits": fake_habits}):
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
        fake_steps = SimpleNamespace(
            handle_steps_sync_check=MagicMock(),
        )
        fake_sleep = SimpleNamespace(
            handle_sleep_resync_job=MagicMock(),
            handle_sleep_sync_job=MagicMock(),
        )
        fake_insights = SimpleNamespace(handle_weekly_health_insight_job=MagicMock())

        with patch.dict("sys.modules", {
            "second_brain.healthtrack.steps": fake_steps,
            "second_brain.healthtrack.sleep": fake_sleep,
            "second_brain.healthtrack.insights": fake_insights,
        }):
            register_handlers(manager)

        registered = {
            call.args[0]: call.args[1]
            for call in manager.register_handler.call_args_list
        }
        self.assertIn("sleep_backfill", registered)
        self.assertIn("steps_sync_check", registered)
        self.assertNotIn("steps_morning_stamp", registered)
        self.assertNotIn("steps_final_stamp", registered)
        self.assertIs(registered["steps_sync_check"], fake_steps.handle_steps_sync_check)
        self.assertTrue(callable(registered["sleep_backfill"]))
