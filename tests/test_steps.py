"""Tests for second_brain/healthtrack/ — steps tracking logic and payload parsing."""

from __future__ import annotations

import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from second_brain.healthtrack.routes import _parse_health_export_payload, register_health_routes
from second_brain.healthtrack.steps import (
    _date_state,
    _load_threshold_state,
    _persist_threshold_state,
    _steps_state,
    backfill_steps_state_from_notion,
    get_steps_state_summary,
    handle_steps_sync,
    handle_steps_final_stamp,
    migrate_steps_entry_titles,
)

# ── Dynamic config loading ───────────────────────────────────────────────────


class TestLoadConfigFromEnvDb(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from second_brain.healthtrack import config as health_config
        self.health_config = health_config
        self.health_config.STEPS_HABIT_NAME = "Steps"

    def tearDown(self):
        self.health_config.STEPS_HABIT_NAME = "Steps"

    async def test_loads_steps_habit_name_from_notion_env_db(self):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [
                {
                    "properties": {
                        "Value": {
                            "rich_text": [
                                {"plain_text": "Daily Steps"},
                            ]
                        }
                    }
                }
            ]
        }

        await self.health_config.load_config_from_env_db(notion, "env-db")

        self.assertEqual(self.health_config.STEPS_HABIT_NAME, "Daily Steps")
        notion.databases.query.assert_called_once_with(
            database_id="env-db",
            filter={"property": "Name", "title": {"equals": "STEPS_HABIT_NAME"}},
        )

    async def test_falls_back_to_steps_when_row_missing(self):
        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}
        self.health_config.STEPS_HABIT_NAME = "Old Value"

        await self.health_config.load_config_from_env_db(notion, "env-db")

        self.assertEqual(self.health_config.STEPS_HABIT_NAME, "Steps")

    async def test_falls_back_to_rich_text_name_filter_if_title_filter_fails(self):
        notion = MagicMock()
        notion.databases.query.side_effect = [
            Exception("title filter unsupported"),
            {
                "results": [
                    {
                        "properties": {
                            "Value": {
                                "rich_text": [
                                    {"plain_text": "Steps"},
                                ]
                            }
                        }
                    }
                ]
            },
        ]

        await self.health_config.load_config_from_env_db(notion, "env-db")

        self.assertEqual(self.health_config.STEPS_HABIT_NAME, "Steps")
        self.assertEqual(notion.databases.query.call_count, 2)


class TestMigrateStepsEntryTitles(unittest.TestCase):
    def test_migrates_legacy_and_blank_steps_titles(self):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [
                {
                    "id": "legacy-page",
                    "properties": {
                        "Entry": {"title": [{"plain_text": "Steps — 2026-05-06"}]},
                    },
                },
                {
                    "id": "current-page",
                    "properties": {"Entry": {"title": [{"plain_text": "Steps"}]}},
                },
                {"id": "blank-page", "properties": {"Entry": {"title": []}}},
            ],
            "has_more": False,
        }

        result = migrate_steps_entry_titles(notion, "log-db", "habit-page")

        self.assertEqual(result, {"renamed": 2, "skipped": 1})
        self.assertEqual(notion.pages.update.call_count, 2)
        updated_page_ids = [
            call.kwargs["page_id"] for call in notion.pages.update.call_args_list
        ]
        self.assertEqual(updated_page_ids, ["legacy-page", "blank-page"])
        for call in notion.pages.update.call_args_list:
            self.assertEqual(
                call.kwargs["properties"],
                {"Entry": {"title": [{"text": {"content": "Steps"}}]}},
            )


# ── Payload parsing ───────────────────────────────────────────────────────────

class TestParseHealthExportPayload(unittest.TestCase):
    def test_flat_format_parses_correctly(self):
        body = {"steps": 11247, "date": "2026-04-28"}
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (11247, "2026-04-28"))

    def test_flat_format_truncates_datetime_to_date(self):
        body = {"steps": 8500, "date": "2026-04-28 23:00:00 +0000"}
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (8500, "2026-04-28"))

    def test_flat_format_accepts_decimal_string_steps(self):
        body = {"steps": "1018.0", "date": "2026-05-04"}
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (1018, "2026-05-04"))

    def test_health_auto_export_nested_format(self):
        body = {
            "data": [
                {
                    "name": "Step Count",
                    "units": "count",
                    "data": [
                        {"date": "2026-04-28 07:00:00 +0000", "qty": 3000},
                        {"date": "2026-04-28 15:00:00 +0000", "qty": 5000},
                        {"date": "2026-04-28 22:00:00 +0000", "qty": 3247},
                    ],
                }
            ]
        }
        result = _parse_health_export_payload(body)
        self.assertIsNotNone(result)
        self.assertEqual(result, (11247, "2026-04-28"))

    def test_health_auto_export_v2_metrics_wrapper(self):
        body = {
            "data": {
                "metrics": [
                    {
                        "name": "Step Count",
                        "units": "count",
                        "data": [{"date": "2026-05-04", "qty": "1018.0"}],
                    }
                ]
            }
        }
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (1018, "2026-05-04"))

    def test_metrics_without_data_wrapper_parses(self):
        body = {
            "metrics": [
                {
                    "name": "Step Count",
                    "data": [{"date": "2026-05-04", "value": "1018"}],
                }
            ]
        }
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (1018, "2026-05-04"))

    def test_nested_format_picks_most_recent_date_even_if_lower(self):
        body = {
            "data": [
                {
                    "name": "Step Count",
                    "data": [
                        {"date": "2026-04-27 23:00:00 +0000", "qty": 12000},
                        {"date": "2026-04-28 22:00:00 +0000", "qty": 9000},
                    ],
                }
            ]
        }
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (9000, "2026-04-28"))

    def test_returns_none_for_empty_body(self):
        self.assertIsNone(_parse_health_export_payload({}))

    def test_returns_none_for_wrong_metric_name(self):
        body = {
            "data": [
                {
                    "name": "Heart Rate",
                    "data": [{"date": "2026-04-28", "qty": 72}],
                }
            ]
        }
        self.assertIsNone(_parse_health_export_payload(body))

    def test_nested_format_with_value_key_instead_of_qty(self):
        body = {
            "data": [
                {
                    "name": "Step Count",
                    "data": [{"date": "2026-04-28", "value": 9000}],
                }
            ]
        }
        result = _parse_health_export_payload(body)
        self.assertEqual(result, (9000, "2026-04-28"))


# ── Steps sync logic ──────────────────────────────────────────────────────────


class TestThresholdStatePersistence(unittest.TestCase):
    def test_persist_threshold_state_uses_standard_env_key(self):
        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}

        _persist_threshold_state(notion, "env-db", "2026-04-28", 12345)

        notion.pages.create.assert_called_once()
        kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(kwargs["parent"], {"database_id": "env-db"})
        props = kwargs["properties"]
        self.assertEqual(
            props["Name"]["title"][0]["text"]["content"],
            "steps_threshold_2026-04-28",
        )
        self.assertEqual(
            props["Value"]["rich_text"][0]["text"]["content"],
            "12345",
        )

    def test_load_threshold_state_reads_message_id(self):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [
                {
                    "id": "env-row",
                    "properties": {
                        "Value": {"rich_text": [{"plain_text": "12345"}]}
                    },
                }
            ]
        }

        message_id = _load_threshold_state(notion, "env-db", "2026-04-28")

        self.assertEqual(message_id, 12345)
        query_kwargs = notion.databases.query.call_args.kwargs
        self.assertEqual(query_kwargs["database_id"], "env-db")
        self.assertEqual(
            query_kwargs["filter"],
            {"property": "Name", "title": {"equals": "steps_threshold_2026-04-28"}},
        )


class TestHandleStepsSync(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _steps_state.clear()

    def _make_notion(self, existing_page_id=None):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [{"id": existing_page_id}] if existing_page_id else []
        }
        notion.pages.create.return_value = {"id": "new-page-id"}
        notion.pages.retrieve.return_value = {"properties": {"Completed": {"checkbox": False}}}
        notion.pages.update.return_value = {}
        return notion

    def _make_tz(self, today="2026-04-28"):
        from zoneinfo import ZoneInfo
        from unittest.mock import patch
        tz = ZoneInfo("America/Chicago")
        return tz

    async def test_sub_threshold_intraday_skips_notion_write(self):
        notion = self._make_notion()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"):
            result = await handle_steps_sync(
                steps=8500,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="habit_db",
                log_db_id="log_db",
                env_db_id="",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
                force_write=False,
            )

        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "sub_threshold_intraday")
        notion.pages.create.assert_not_called()
        notion.pages.update.assert_not_called()

    async def test_force_write_bypasses_sub_threshold_skip(self):
        """steps_final_stamp should write even when steps < threshold."""
        notion = self._make_notion()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=9500,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                env_db_id="",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
                force_write=True,
            )

        self.assertNotEqual(result["action"], "skipped")
        self.assertIn(result["action"], ("created", "updated"))
        self.assertEqual(result["steps"], 9500)

    async def test_sub_threshold_intraday_still_skips_without_force(self):
        """Normal intraday sync below threshold should still be skipped."""
        notion = self._make_notion()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=9500,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                env_db_id="",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
                force_write=False,
            )

        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "sub_threshold_intraday")

    async def test_threshold_crossed_sends_notification_and_creates_entry(self):
        notion = self._make_notion()
        bot = AsyncMock()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=10500,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="habit_db",
                log_db_id="log_db",
                env_db_id="",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
                bot=bot,
                chat_id=12345,
                force_write=False,
            )

        self.assertEqual(result["action"], "created")
        create_kwargs = notion.pages.create.call_args.kwargs
        self.assertEqual(create_kwargs["parent"], {"database_id": "log_db"})
        bot.send_message.assert_awaited_once()
        msg = bot.send_message.await_args.kwargs["text"]
        self.assertIn("10,000 steps hit", msg)
        self.assertIn("10,500", msg)

    async def test_threshold_notification_sent_only_once(self):
        notion = self._make_notion()
        bot = AsyncMock()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            # First sync above threshold
            await handle_steps_sync(
                steps=10200, date_str="2026-04-28", notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                bot=bot, chat_id=12345,
                force_write=False,
            )
            # Second sync above threshold (same day)
            notion.databases.query.return_value = {"results": [{"id": "existing-pid"}]}
            await handle_steps_sync(
                steps=11000, date_str="2026-04-28", notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                bot=bot, chat_id=12345,
                force_write=False,
            )

        # Notification sent exactly once
        self.assertEqual(bot.send_message.await_count, 1)

    async def test_threshold_notification_edits_existing_message(self):
        """Second sync above threshold edits existing message, does not send new."""
        notion = self._make_notion()
        bot = AsyncMock()
        tz = self._make_tz()

        mock_msg = MagicMock()
        mock_msg.message_id = 12345
        bot.send_message = AsyncMock(return_value=mock_msg)
        bot.edit_message_text = AsyncMock()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            await handle_steps_sync(
                steps=10200, date_str="2026-04-28", notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                bot=bot, chat_id=99,
                force_write=False,
            )

            notion.databases.query.return_value = {"results": [{"id": "existing-pid"}]}
            await handle_steps_sync(
                steps=11000, date_str="2026-04-28", notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                bot=bot, chat_id=99,
                force_write=False,
            )

        bot.send_message.assert_awaited_once()
        bot.edit_message_text.assert_awaited_once()

        edit_kwargs = bot.edit_message_text.await_args.kwargs
        self.assertEqual(edit_kwargs["message_id"], 12345)
        self.assertIn("11,000", edit_kwargs["text"])

    async def test_recovers_threshold_message_id_from_env_db_and_edits(self):
        notion = self._make_notion()
        notion.databases.query.return_value = {
            "results": [
                {
                    "id": "env-row",
                    "properties": {
                        "Value": {"rich_text": [{"plain_text": "777"}]},
                    },
                }
            ]
        }
        bot = AsyncMock()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"), \
             patch("second_brain.healthtrack.steps._find_existing_log_entry", return_value="existing-pid"):
            result = await handle_steps_sync(
                steps=11000,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
                env_db_id="env",
                bot=bot,
                chat_id=99,
                force_write=False,
            )

        self.assertEqual(result["action"], "updated")
        bot.send_message.assert_not_awaited()
        bot.edit_message_text.assert_awaited_once()
        edit_kwargs = bot.edit_message_text.await_args.kwargs
        self.assertEqual(edit_kwargs["message_id"], 777)
        self.assertIn("11,000", edit_kwargs["text"])
        self.assertTrue(_steps_state["2026-04-28"]["threshold_notified"])
        self.assertEqual(_steps_state["2026-04-28"]["threshold_message_id"], 777)

    async def test_yesterday_late_arrival_upserts_without_notification(self):
        notion = self._make_notion()
        bot = AsyncMock()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=9350,
                date_str="2026-04-27",  # yesterday
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                bot=bot, chat_id=12345,
                force_write=False,
            )

        # No Telegram notification for yesterday's late arrival
        bot.send_message.assert_not_awaited()
        # Entry created with Completed=False (9350 < 10000)
        self.assertIn(result["action"], ("created", "updated"))

    async def test_old_date_is_skipped(self):
        notion = self._make_notion()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"):
            result = await handle_steps_sync(
                steps=8000,
                date_str="2026-04-25",  # 3 days ago
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                force_write=False,
            )

        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "old_date")

    async def test_existing_entry_is_updated_not_recreated(self):
        notion = self._make_notion(existing_page_id="existing-page-abc")
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=12000,
                date_str="2026-04-28",
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                force_write=False,
            )

        self.assertEqual(result["action"], "updated")
        notion.pages.create.assert_not_called()
        notion.pages.update.assert_called_once()

    async def test_late_correction_preserves_completed_true(self):
        notion = self._make_notion(existing_page_id="existing-page-abc")
        notion.pages.retrieve.return_value = {"properties": {"Completed": {"checkbox": True}}}
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            result = await handle_steps_sync(
                steps=9500,
                date_str="2026-04-27",
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                force_write=False,
            )

        self.assertEqual(result["action"], "updated")
        props = notion.pages.update.call_args.kwargs["properties"]
        self.assertEqual(props["Steps Count"], {"number": 9500})
        self.assertTrue(props["Completed"]["checkbox"])

    async def test_completed_false_when_below_threshold(self):
        notion = self._make_notion()
        tz = self._make_tz()

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            await handle_steps_sync(
                steps=7000,
                date_str="2026-04-27",  # yesterday, below threshold
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
                force_write=False,
            )

        create_call = notion.pages.create.call_args
        props = create_call.kwargs["properties"]
        self.assertFalse(props["Completed"]["checkbox"])
        self.assertEqual(props["Steps Count"]["number"], 7000)


class TestThresholdStatePersistence(unittest.TestCase):
    def test_persist_threshold_state_creates_env_row(self):
        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}

        _persist_threshold_state(notion, "env", "2026-04-28", 12345)

        notion.pages.create.assert_called_once()
        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Name"]["title"][0]["text"]["content"], "steps_threshold_2026-04-28")
        self.assertEqual(props["Value"]["rich_text"][0]["text"]["content"], "12345")

    def test_load_threshold_state_reads_env_row(self):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [
                {
                    "id": "env-row",
                    "properties": {
                        "Value": {"rich_text": [{"plain_text": "12345"}]},
                    },
                }
            ]
        }

        self.assertEqual(_load_threshold_state(notion, "env", "2026-04-28"), 12345)


class TestBackfillStepsStateFromNotion(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _steps_state.clear()

    async def test_backfill_populates_today_from_existing_notion_entry(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 8500}}
        }
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"), \
             patch("second_brain.healthtrack.steps._find_existing_log_entry") as find_existing:
            find_existing.side_effect = lambda notion_arg, log_db_id, habit_page_id, date_str: (
                "today-page" if date_str == "2026-04-28" else None
            )
            await backfill_steps_state_from_notion(
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                habit_name="Steps",
                tz=tz,
            )

        self.assertEqual(_steps_state["2026-04-28"]["last_steps"], 8500)
        self.assertEqual(_steps_state["2026-04-28"]["notion_page_id"], "today-page")
        notion.pages.retrieve.assert_called_once_with(page_id="today-page")


class TestHandleStepsFinalStamp(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _steps_state.clear()

    async def test_nightly_stamp_uses_cached_step_count(self):
        _steps_state["2026-04-28"] = {
            "last_steps": 11500,
            "threshold_notified": True,
            "notion_page_id": None,
        }

        notion = MagicMock()
        notion.databases.query.return_value = {"results": [{"id": "habit-pid"}]}
        notion.pages.create.return_value = {"id": "new-pid"}
        notion.pages.update.return_value = {}
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            results = await handle_steps_final_stamp(
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
            )

        self.assertIn("2026-04-28", results)
        # 11500 steps → Completed=True, entry created or updated
        self.assertIn(results["2026-04-28"]["action"], ("created", "updated"))

    async def test_nightly_stamp_skips_yesterday_when_no_data(self):
        # Nothing in state for yesterday
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}
        notion.pages.create.return_value = {"id": "pid"}

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"):
            results = await handle_steps_final_stamp(
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
            )

        # Today skipped (0 steps), yesterday skipped (no data)
        self.assertNotIn("2026-04-28", results)
        self.assertNotIn("2026-04-27", results)

    async def test_stamp_skips_today_when_no_data(self):
        notion = MagicMock()
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"):
            results = await handle_steps_final_stamp(
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                env_db_id="",
                habit_name="Steps",
                threshold=10000,
                source_label="📱 Apple Watch",
                tz=tz,
            )

        self.assertNotIn("2026-04-28", results)
        self.assertNotIn("2026-04-27", results)
        notion.pages.create.assert_not_called()
        notion.pages.update.assert_not_called()

    async def test_nightly_stamp_skips_today_zero_steps_but_stamps_yesterday(self):
        _steps_state["2026-05-09"] = {
            "last_steps": 0,
            "threshold_notified": False,
            "notion_page_id": None,
        }
        _steps_state["2026-05-08"] = {
            "last_steps": 10596,
            "threshold_notified": True,
            "notion_page_id": None,
        }

        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}
        notion.pages.create.return_value = {"id": "yesterday-pid"}
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-05-09"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-05-08"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"), \
             patch("second_brain.healthtrack.steps._find_existing_log_entry", return_value=None):
            results = await handle_steps_final_stamp(
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
            )

        self.assertNotIn("2026-05-09", results)
        self.assertEqual(results["2026-05-08"]["steps"], 10596)
        self.assertEqual(results["2026-05-08"]["action"], "created")
        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Date"]["date"]["start"], "2026-05-08")
        self.assertEqual(props["Steps Count"]["number"], 10596)

    async def test_nightly_stamp_can_write_below_threshold_today(self):
        _steps_state["2026-04-28"] = {
            "last_steps": 1018,
            "threshold_notified": False,
            "notion_page_id": None,
        }

        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}
        notion.pages.create.return_value = {"id": "pid"}
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"):
            results = await handle_steps_final_stamp(
                notion=notion,
                habit_db_id="h", log_db_id="l", env_db_id="", habit_name="Steps",
                threshold=10000, source_label="📱 Apple Watch", tz=tz,
            )

        self.assertEqual(results["2026-04-28"]["action"], "created")
        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Steps Count"]["number"], 1018)
        self.assertFalse(props["Completed"]["checkbox"])


class TestBackfillStepsStateFromNotion(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _steps_state.clear()

    async def test_backfill_loads_today_steps_from_existing_notion_page(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 8500}}
        }
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        def find_existing(_notion, _log_db_id, _habit_page_id, date_str):
            if date_str == "2026-04-28":
                return "today-pid"
            return None

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"), \
             patch("second_brain.healthtrack.steps._find_existing_log_entry", side_effect=find_existing):
            await backfill_steps_state_from_notion(
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                habit_name="Steps",
                tz=tz,
            )

        self.assertEqual(_steps_state["2026-04-28"]["last_steps"], 8500)
        self.assertEqual(_steps_state["2026-04-28"]["notion_page_id"], "today-pid")

    async def test_backfill_restores_threshold_message_from_env_db(self):
        notion = MagicMock()
        notion.pages.retrieve.return_value = {
            "properties": {"Steps Count": {"number": 11083}}
        }
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Chicago")

        def find_existing(_notion, _log_db_id, _habit_page_id, date_str):
            if date_str == "2026-04-28":
                return "today-pid"
            return None

        with patch("second_brain.healthtrack.steps._local_today", return_value="2026-04-28"), \
             patch("second_brain.healthtrack.steps._yesterday", return_value="2026-04-27"), \
             patch("second_brain.healthtrack.steps._find_steps_habit_page_id", return_value="habit-pid"), \
             patch("second_brain.healthtrack.steps._find_existing_log_entry", side_effect=find_existing), \
             patch("second_brain.healthtrack.steps._load_threshold_state", return_value=12345):
            await backfill_steps_state_from_notion(
                notion=notion,
                habit_db_id="h",
                log_db_id="l",
                env_db_id="env-db",
                habit_name="Steps",
                tz=tz,
            )

        self.assertEqual(_steps_state["2026-04-28"]["last_steps"], 11083)
        self.assertTrue(_steps_state["2026-04-28"]["threshold_notified"])
        self.assertEqual(_steps_state["2026-04-28"]["threshold_message_id"], 12345)


class TestGetStepsStateSummary(unittest.TestCase):
    def setUp(self):
        _steps_state.clear()

    def test_summary_reflects_current_state(self):
        _steps_state["2026-04-28"] = {
            "last_steps": 9500,
            "threshold_notified": False,
            "notion_page_id": "page-abc",
        }
        summary = get_steps_state_summary()
        self.assertEqual(summary["2026-04-28"]["last_steps"], 9500)
        self.assertFalse(summary["2026-04-28"]["threshold_notified"])
        self.assertIsNone(summary["2026-04-28"]["threshold_message_id"])
        self.assertTrue(summary["2026-04-28"]["has_notion_entry"])


class TestStepsRoutes(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _steps_state.clear()

    async def test_steps_status_returns_json(self):
        app = web.Application()
        register_health_routes(
            app,
            notion=MagicMock(),
            habit_db_id="h",
            log_db_id="l",
            env_db_id="",
            tz=None,
            bot_getter=lambda: None,
            chat_id=123,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.get("/api/v1/steps-status")
            body = await response.text()
        finally:
            await client.close()

        self.assertEqual(response.status, 200)
        payload = json.loads(body)
        self.assertTrue(payload["ok"])
        self.assertIn("state", payload)

    async def test_steps_sync_options_preflight_returns_204(self):
        app = web.Application()
        register_health_routes(
            app,
            notion=MagicMock(),
            habit_db_id="h",
            log_db_id="l",
            env_db_id="",
            tz=None,
            bot_getter=lambda: None,
            chat_id=123,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.options("/api/v1/steps-sync")
        finally:
            await client.close()

        self.assertEqual(response.status, 204)
        self.assertIn("X-Health-Secret", response.headers.get("Access-Control-Allow-Headers", ""))

    async def test_steps_sync_awaits_async_telemetry_callback(self):
        callback = AsyncMock()
        app = web.Application()
        register_health_routes(
            app,
            notion=MagicMock(),
            habit_db_id="h",
            log_db_id="l",
            env_db_id="",
            tz=None,
            bot_getter=lambda: None,
            chat_id=123,
            on_sync_result=callback,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            with patch("second_brain.healthtrack.routes.WEBHOOK_SECRET", "secret"), \
                 patch("second_brain.healthtrack.routes.handle_steps_sync", new_callable=AsyncMock) as sync_mock:
                sync_mock.return_value = {
                    "action": "skipped",
                    "steps": 10500,
                    "date": "2026-05-06",
                    "completed": True,
                    "timestamp": "2026-05-06T00:00:00+00:00",
                }
                response = await client.post(
                    "/api/v1/steps-sync",
                    headers={"X-Health-Secret": "secret"},
                    json={"steps": 10500, "date": "2026-05-06"},
                )
                body = await response.text()
        finally:
            await client.close()

        self.assertEqual(response.status, 200, body)
        callback.assert_awaited_once()
        self.assertEqual(callback.await_args.args[0]["timestamp"], "2026-05-06T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
