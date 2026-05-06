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
            {"time": "16:30", "is_weekday": True, "include_habits": True, "include_weather": True, "include_uvi": True, "contexts": ["🏠 Personal"], "max_items": 5},
            {"time": "23:59", "is_weekday": True, "include_habits": False, "include_weather": False, "include_uvi": False, "contexts": [], "max_items": None},
        ]

        config = main._manual_digest_config_now(slots, now_dt=now)

        self.assertIsNotNone(config)
        self.assertTrue(config["include_habits"])
        self.assertTrue(config["include_weather"])
        self.assertTrue(config["include_uvi"])
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
    async def test_send_daily_digest_renders_cached_morning_habit_when_direct_query_empty(self):
        main = load_main_module()
        test_habit = {
            "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "name": "Test Morning Habit",
            "time": "🌅 Morning",
            "sort": 1,
            "freq_per_week": None,
            "weather_gated": False,
        }
        main.habit_cache = {"Test Morning Habit": test_habit}
        bot = MagicMock()
        sent = MagicMock(message_id=42)
        bot.send_message = AsyncMock(return_value=sent)

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]), \
            patch.object(main.notion_habits, "get_habits_by_time", return_value=[]), \
            patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False):
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
        self.assertIn("*Habits:* tap to log:", kwargs["text"])
        self.assertIsNotNone(kwargs["reply_markup"])

    async def test_send_daily_digest_fail_opens_weather_gated_habits_without_uvi(self):
        main = load_main_module()
        habit = {
            "page_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "name": "Test Morning Habit",
            "time_str": "🌅 Morning",
            "frequency": None,
            "completion_count": 0,
            "weather_gated": True,
        }
        bot = MagicMock()
        sent = MagicMock(message_id=43)
        bot.send_message = AsyncMock(return_value=sent)

        def habits_by_time(*, time_filter, **_kwargs):
            return [habit] if time_filter == "🌅 Morning" else []

        with patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]), \
            patch.object(main.notion_habits, "get_habits_by_time", side_effect=habits_by_time), \
            patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False):
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
        self.assertIn("*Habits:* tap to log:", kwargs["text"])
        self.assertIsNotNone(kwargs["reply_markup"])
