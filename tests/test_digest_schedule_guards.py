import importlib
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


REQUIRED_ENV = {
    "TELEGRAM_TOKEN": "x",
    "TELEGRAM_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "x",
    "NOTION_TOKEN": "x",
    "NOTION_DB_ID": "x",
    "NOTION_HABIT_DB": "x",
    "NOTION_LOG_DB": "x",
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
    "NOTION_STREAK_DB": "x",
}


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


class _FakeJob:
    def remove(self):
        return None


class _FakeScheduler:
    def __init__(self):
        self.calls = []

    def add_job(self, fn, trigger, **kwargs):
        self.calls.append({"fn": fn, "trigger": trigger, "kwargs": kwargs})
        return _FakeJob()


class TestDigestScheduleGuards(unittest.IsolatedAsyncioTestCase):
    async def test_send_digest_for_slot_skips_when_slot_already_sent_today(self):
        main = load_main_module()
        now = main.datetime.now(main.TZ)
        weekday = now.weekday() < 5
        slot = {"time": "08:15", "is_weekday": weekday, "include_habits": True}
        slot_key = f"{now.date().isoformat()}|{'wd' if weekday else 'we'}|08:15"

        main._digest_slot_sent_today.clear()
        main._digest_slot_sent_today.add(slot_key)

        with patch.object(main, "get_digest_config", AsyncMock()) as mock_cfg, \
            patch.object(main, "send_daily_digest", AsyncMock()) as mock_send:
            await main.send_digest_for_slot(MagicMock(), slot)

        mock_cfg.assert_not_called()
        mock_send.assert_not_called()


class TestDigestSelectorDedupe(unittest.TestCase):
    def test_load_digest_slots_dedupes_duplicate_time_and_day(self):
        main = load_main_module()

        def row(time_text: str, ww_name: str):
            return {
                "properties": {
                    "Time": {"rich_text": [{"plain_text": time_text}]},
                    "Weekday/Weekend": {"select": {"name": ww_name}},
                    "Habits": {"checkbox": True},
                    "Feel": {"checkbox": True},
                    "Max Items": {"number": 10},
                    "🏠 Personal": {"checkbox": True},
                }
            }

        rows = [row("08:15", "Weekday"), row("08:15", "Weekday")]
        with patch.object(main, "notion_query_all", return_value=rows):
            slots = main.load_digest_slots()

        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["time"], "08:15")
        self.assertTrue(slots[0]["is_weekday"])
        self.assertTrue(slots[0]["include_feel"])

    def test_load_digest_slots_keeps_empty_contexts_for_habits_only_rows(self):
        main = load_main_module()
        rows = [
            {
                "properties": {
                    "Time": {"rich_text": [{"plain_text": "07:30"}]},
                    "Weekday/Weekend": {"select": {"name": "Weekday"}},
                    "Habits": {"checkbox": True},
                    "Max Items": {"number": 10},
                    "🏠 Personal": {"checkbox": False},
                    "💼 Work": {"checkbox": False},
                    "🏃 Health": {"checkbox": False},
                    "🤝 HK": {"checkbox": False},
                }
            }
        ]
        with patch.object(main, "notion_query_all", return_value=rows):
            slots = main.load_digest_slots()

        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["contexts"], [])
        self.assertFalse(slots[0]["include_feel"])


class TestDigestTaskFiltering(unittest.TestCase):
    def test_filter_digest_tasks_returns_no_tasks_for_empty_context_selection(self):
        main = load_main_module()
        tasks = [
            {"name": "Work task", "context": "💼 Work"},
            {"name": "Personal task", "context": "🏠 Personal"},
        ]

        filtered = main._filter_digest_tasks(tasks, config={"contexts": [], "max_items": 10, "include_habits": True})

        self.assertEqual(filtered, [])


class TestDigestCatchupFlag(unittest.TestCase):
    def test_build_digest_schedule_does_not_queue_catchup_by_default(self):
        main = load_main_module()
        scheduler = _FakeScheduler()
        slot = {"time": "08:15", "is_weekday": True, "include_habits": True, "max_items": 10, "contexts": ["💼 Work"]}

        with patch.object(main, "load_digest_slots", return_value=[slot]), \
            patch.object(main, "_queue_missed_slots_for_today") as mock_queue:
            main.build_digest_schedule(scheduler, MagicMock())

        mock_queue.assert_not_called()

    def test_build_digest_schedule_queues_catchup_when_enabled(self):
        main = load_main_module()
        scheduler = _FakeScheduler()
        slot = {"time": "08:15", "is_weekday": True, "include_habits": True, "max_items": 10, "contexts": ["💼 Work"]}

        with patch.object(main, "load_digest_slots", return_value=[slot]), \
            patch.object(main, "_queue_missed_slots_for_today") as mock_queue:
            main.build_digest_schedule(scheduler, MagicMock(), queue_catchup=True)

        mock_queue.assert_called_once()


if __name__ == "__main__":
    unittest.main()


class TestManualDigestConfig(unittest.TestCase):
    def test_manual_digest_config_uses_latest_slot_not_after_now(self):
        main = load_main_module()
        now = main.datetime.now(main.TZ).replace(hour=18, minute=45)
        slots = [
            {"time": "08:15", "is_weekday": True, "include_habits": False, "include_weather": False, "include_uvi": False, "contexts": ["💼 Work"], "max_items": 10},
            {"time": "16:30", "is_weekday": True, "include_habits": True, "include_weather": True, "include_uvi": True, "include_feel": True, "contexts": ["🏠 Personal"], "max_items": 5},
            {"time": "23:59", "is_weekday": True, "include_habits": False, "include_weather": False, "include_uvi": False, "contexts": [], "max_items": None},
        ]

        config = main._manual_digest_config_now(slots, now_dt=now)

        self.assertIsNotNone(config)
        self.assertTrue(config["include_habits"])
        self.assertTrue(config["include_weather"])
        self.assertTrue(config["include_uvi"])
        self.assertTrue(config["include_feel"])
        self.assertEqual(config["contexts"], ["🏠 Personal"])
        self.assertEqual(config["max_items"], 5)


    def test_manual_digest_config_forces_weather_for_digest_button(self):
        main = load_main_module()
        now = main.datetime.now(main.TZ).replace(hour=9, minute=0)
        slots = [
            {"time": "08:15", "is_weekday": True, "include_habits": False, "include_weather": False, "include_uvi": False, "contexts": ["💼 Work"], "max_items": 10},
        ]

        config = main._manual_digest_config_now(slots, now_dt=now)

        self.assertIsNotNone(config)
        self.assertTrue(config["include_weather"])
        self.assertFalse(config["include_habits"])
        self.assertEqual(config["contexts"], ["💼 Work"])

    def test_manual_digest_config_uses_late_digest_slot(self):
        main = load_main_module()
        now = main.datetime.now(main.TZ).replace(hour=23, minute=59)
        slots = [
            {"time": "23:59", "is_weekday": True, "include_habits": False, "contexts": [], "max_items": None},
        ]

        config = main._manual_digest_config_now(slots, now_dt=now)

        self.assertIsNotNone(config)
        self.assertTrue(config["include_weather"])
        self.assertFalse(config["include_habits"])
        self.assertEqual(config["contexts"], [])
        self.assertIsNone(config["max_items"])


class TestDailyDigestHabits(unittest.IsolatedAsyncioTestCase):
    async def test_send_daily_digest_renders_cached_habit_after_show_after_gate(self):
        main = load_main_module()
        real_datetime = main.datetime
        test_habit = {
            "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "Test Digest Habit",
            "show_after": "05:00",
            "sort": 1,
            "freq_per_week": None,
        }
        main.habit_cache = {"Test Digest Habit": test_habit}
        bot = MagicMock()
        sent = MagicMock(message_id=42)
        bot.send_message = AsyncMock(return_value=sent)

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]), \
            patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False), \
            patch("second_brain.main.datetime") as mock_dt:
            mock_dt.now.return_value = real_datetime(2026, 5, 7, 8, 0, tzinfo=main.TZ)
            await main.send_daily_digest(
                bot,
                include_habits=True,
                config={
                    "contexts": [],
                    "max_items": None,
                    "include_habits": True,
                    "include_weather": False,
                    "include_uvi": False,
                },
            )

        bot.send_message.assert_awaited_once()
        kwargs = bot.send_message.await_args.kwargs
        button_labels = [
            button.text
            for row in kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("*Habits:* tap to log:", kwargs["text"])
        self.assertIn("Test Digest Habit", button_labels)

    async def test_send_daily_digest_renders_conversational_readiness_button_when_feel_enabled(self):
        main = load_main_module()
        bot = MagicMock()
        sent = MagicMock(message_id=44)
        bot.send_message = AsyncMock(return_value=sent)

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]):
            await main.send_daily_digest(
                bot,
                include_habits=False,
                config={
                    "contexts": [],
                    "max_items": None,
                    "include_habits": False,
                    "include_weather": False,
                    "include_uvi": False,
                    "include_feel": True,
                },
            )

        kwargs = bot.send_message.await_args.kwargs
        self.assertIsNotNone(kwargs["reply_markup"])
        buttons = [
            button
            for row in kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertEqual(buttons[-1].text, "💬 How are you feeling?")
        self.assertEqual(buttons[-1].callback_data, "cf:A")

    async def test_send_daily_digest_filters_habits_before_show_after_gate(self):
        main = load_main_module()
        real_datetime = main.datetime
        main.habit_cache = {
            "Late Digest Habit": {
                "page_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "name": "Late Digest Habit",
                "show_after": "18:00",
                "sort": 1,
                "freq_per_week": None,
            }
        }
        bot = MagicMock()
        sent = MagicMock(message_id=43)
        bot.send_message = AsyncMock(return_value=sent)

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]), \
            patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False), \
            patch("second_brain.main.datetime") as mock_dt:
            mock_dt.now.return_value = real_datetime(2026, 5, 7, 8, 0, tzinfo=main.TZ)
            await main.send_daily_digest(
                bot,
                include_habits=True,
                config={
                    "contexts": [],
                    "max_items": None,
                    "include_habits": True,
                    "include_weather": False,
                    "include_uvi": False,
                },
            )

        kwargs = bot.send_message.await_args.kwargs
        self.assertNotIn("*Habits:* tap to log:", kwargs["text"])
        self.assertIsNone(kwargs["reply_markup"])


class TestTripReminderIntegration(unittest.IsolatedAsyncioTestCase):
    def test_get_upcoming_trips_needing_reminder_filters_and_extracts_details(self):
        main = load_main_module()
        today = main.date.today()
        dep = today + main.timedelta(days=1)
        ret = today + main.timedelta(days=3)
        captured = {}

        class _Databases:
            def query(self, **kwargs):
                captured.update(kwargs)
                return {
                    "results": [
                        {
                            "id": "trip-1",
                            "properties": {
                                "Trip": {"title": [{"plain_text": "Nashville — 14–17 May"}]},
                                "Departure Date": {"date": {"start": dep.isoformat()}},
                                "Return Date": {"date": {"start": ret.isoformat()}},
                                "Purpose": {"select": {"name": "Work"}},
                                "Field Work": {"multi_select": [{"name": "Site Walk"}, {"name": "Noise Measurements"}]},
                                "Weather Summary": {"rich_text": [{"plain_text": "May 14: Sunny, 22°C, 10% rain"}]},
                                "Weather Flags": {"multi_select": [{"name": "Rain"}]},
                            },
                        }
                    ]
                }

        main.NOTION_TRIPS_DB = "trips-db"
        main.notion = type("Notion", (), {"databases": _Databases()})()

        trips = main.get_upcoming_trips_needing_reminder(within_days=2)

        self.assertEqual(captured["database_id"], "trips-db")
        self.assertEqual(captured["filter"]["and"][2], {"property": "Reminder Sent", "checkbox": {"equals": False}})
        self.assertEqual(trips[0]["page_id"], "trip-1")
        self.assertEqual(trips[0]["title"], "Nashville — 14–17 May")
        self.assertEqual(trips[0]["days_until"], 1)
        self.assertEqual(trips[0]["field_work"], ["Site Walk", "Noise Measurements"])
        self.assertEqual(trips[0]["weather_flags"], ["Rain"])

    def test_append_trip_reminders_marks_displayed_trips_sent(self):
        main = load_main_module()
        today = main.date.today()
        updated = []

        class _Pages:
            def update(self, **kwargs):
                updated.append(kwargs)

        main.notion = type("Notion", (), {"pages": _Pages()})()
        with patch.object(
            main,
            "get_upcoming_trips_needing_reminder",
            return_value=[
                {
                    "page_id": "trip-1",
                    "title": "Nashville — 14–17 May",
                    "departure_date": today + main.timedelta(days=2),
                    "return_date": today + main.timedelta(days=5),
                    "days_until": 2,
                    "purpose": "Work",
                    "field_work": ["Site Walk"],
                    "weather_summary": "May 14: Sunny, 22°C, 10% rain",
                    "weather_flags": ["Rain"],
                }
            ],
        ):
            text = main.append_trip_reminders_to_text("Weather body", within_days=2)

        self.assertIn("Weather body", text)
        self.assertIn("──────────────────────────────", text)
        self.assertIn("🧳 *Nashville — 14–17 May*", text)
        self.assertIn("📅 Departing in 2 days", text)
        self.assertIn("🎯 Work trip · Site Walk", text)
        self.assertEqual(updated, [{"page_id": "trip-1", "properties": {"Reminder Sent": {"checkbox": True}}}])

    async def test_weather_command_appends_trip_reminders(self):
        main = load_main_module()
        message = MagicMock()
        message.reply_text = AsyncMock()
        update = MagicMock()
        update.effective_chat.id = main.MY_CHAT_ID
        update.message = message

        with patch.object(main.fmt, "format_weather_snapshot", return_value="🌤️ Forecast"), \
            patch.object(main, "append_trip_reminders_to_text", return_value="🌤️ Forecast\n\n🧳 *Trip*") as mock_append:
            await main.cmd_weather(update, MagicMock())

        mock_append.assert_called_once_with("🌤️ Forecast", within_days=2)
        message.reply_text.assert_awaited_once_with("🌤️ Forecast\n\n🧳 *Trip*", parse_mode="Markdown")
