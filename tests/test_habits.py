import unittest
import importlib
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.notion.habits import extract_habit_frequency


class TestExtractHabitFrequency(unittest.TestCase):
    def test_prefers_frequency_per_week_number(self):
        props = {
            "Frequency Per Week": {"type": "number", "number": 4},
            "Frequency": {"type": "select", "select": {"name": "2x/week"}},
        }
        self.assertEqual(extract_habit_frequency(props), 4)

    def test_reads_frequency_from_select_text(self):
        props = {
            "Frequency": {"type": "select", "select": {"name": "3x/week"}},
        }
        self.assertEqual(extract_habit_frequency(props), 3)

    def test_reads_frequency_from_label_fallback(self):
        props = {
            "Frequency Label": {"type": "rich_text", "rich_text": [{"plain_text": "5 per week"}]},
        }
        self.assertEqual(extract_habit_frequency(props), 5)

    def test_reads_frequency_from_multi_part_rich_text(self):
        props = {
            "Frequency Label": {
                "type": "rich_text",
                "rich_text": [
                    {"plain_text": "4"},
                    {"text": {"content": "x/week"}},
                ],
            },
        }
        self.assertEqual(extract_habit_frequency(props), 4)

    def test_returns_none_for_missing_frequency(self):
        props = {
            "Frequency": {"type": "select", "select": {"name": "As needed"}},
        }
        self.assertIsNone(extract_habit_frequency(props))


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


class TestLoadHabitCacheFrequency(unittest.TestCase):
    def test_load_habit_cache_reads_frequency_number_field(self):
        main = load_main_module()
        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": {
                "Habit": {"title": [{"text": {"content": "Protein Shake"}}]},
                "Active": {"checkbox": True},
                "Color": {"select": {"name": "Blue"}},
                "Frequency": {"type": "number", "number": 5},
                "Description": {"rich_text": [{"text": {"content": "Post-workout"}}]},
                "Sort": {"number": 3},
            },
        }

        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

        cached = main.habit_cache["Protein Shake"]
        self.assertEqual(cached["freq_per_week"], 5)
        self.assertEqual(cached["frequency_label"], "5x/week")

    def test_load_habit_cache_reads_show_after_plain_text_payload(self):
        main = load_main_module()
        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": {
                "Habit": {"title": [{"text": {"content": "Walk"}}]},
                "Active": {"checkbox": True},
                "Show After": {
                    "type": "rich_text",
                    "rich_text": [{"plain_text": "11:00"}],
                },
            },
        }

        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

        self.assertEqual(main.habit_cache["Walk"]["show_after"], "11:00")

    def test_load_habit_cache_reads_show_after_with_extra_property_whitespace(self):
        main = load_main_module()
        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": {
                "Habit": {"title": [{"text": {"content": "Meditate"}}]},
                "Active": {"checkbox": True},
                "Show After ": {
                    "type": "rich_text",
                    "rich_text": [{"text": {"content": "05:00"}}],
                },
            },
        }

        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

        self.assertEqual(main.habit_cache["Meditate"]["show_after"], "05:00")


class TestShowAfterGating(unittest.TestCase):
    def _load_single_habit(self, main, *, show_after):
        props = {
            "Habit": {"title": [{"text": {"content": "Read"}}]},
            "Active": {"checkbox": True},
            "Color": {"select": {"name": "Blue"}},
            "Description": {"rich_text": [{"text": {"content": "Read a few pages"}}]},
            "Sort": {"number": 1},
        }
        if show_after is not None:
            props["Show After"] = {"rich_text": [{"text": {"content": show_after}}]}
        else:
            props["Show After"] = {"rich_text": []}

        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": props,
        }
        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

    def _pending_names(self, *, show_after, time_str):
        main = load_main_module()
        self._load_single_habit(main, show_after=show_after)
        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False):
            return [habit["name"] for habit in main.pending_habits_for_digest(time_str=time_str)]

    def test_habit_excluded_before_show_after(self):
        self.assertNotIn("Read", self._pending_names(show_after="18:00", time_str="08:00"))

    def test_habit_included_after_show_after(self):
        self.assertIn("Read", self._pending_names(show_after="05:00", time_str="08:00"))

    def test_habit_included_when_show_after_is_none(self):
        self.assertIn("Read", self._pending_names(show_after=None, time_str="08:00"))

    def test_habit_included_at_exact_show_after_time(self):
        self.assertIn("Read", self._pending_names(show_after="08:00", time_str="08:00"))

    def test_manual_habits_list_bypasses_show_after(self):
        self.assertIn("Read", self._pending_names(show_after="18:00", time_str=None))



class TestHabitButtonsMultiSelect(unittest.TestCase):
    def test_habit_buttons_toggle_callbacks_and_done_count(self):
        from second_brain import keyboards as kb

        habits = [
            {
                "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "name": "💪 Workout",
                "icon": "💪",
            },
            {
                "page_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "name": "🥤 Protein Shake",
                "icon": "🥤",
            },
        ]

        markup = kb.habit_buttons(habits, "morning", selected={habits[1]["page_id"]})
        rows = markup.inline_keyboard
        buttons = [button for row in rows for button in row]

        self.assertEqual(buttons[0].text, "💪 Workout")
        self.assertEqual(buttons[0].callback_data, "h:toggle:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(buttons[1].text, "✅ 🥤 Protein Shake")
        self.assertEqual(buttons[1].callback_data, "h:toggle:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        self.assertIn("✅ Done (1)", [button.text for button in buttons])
        self.assertIn("h:done", [button.callback_data for button in buttons])

    def test_habit_buttons_hides_done_when_nothing_selected(self):
        from second_brain import keyboards as kb

        habits = [{"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout"}]

        markup = kb.habit_buttons(habits, "manual", selected=set())
        labels = [button.text for row in markup.inline_keyboard for button in row]

        self.assertNotIn("✅ Done (0)", labels)
        self.assertEqual(labels, ["Workout", "✖️ Cancel"])


class TestHabitToggleCache(unittest.IsolatedAsyncioTestCase):
    async def test_toggle_uses_cached_habits_without_refreshing_notion(self):
        main = load_main_module()
        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        habits = [{"page_id": page_id, "name": "Workout", "sort": 1}]
        message = MagicMock()
        message.message_id = 123
        message.text = "🎯 *Daily habits* — tap habits to select, then tap Done:"
        message.caption = None

        query = MagicMock()
        query.data = "h:toggle:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        query.message = message
        query.edit_message_reply_markup = AsyncMock()
        query.answer = AsyncMock()

        update = MagicMock()
        update.callback_query = query

        main._store_habit_selection_session(message.message_id, habits)

        with patch.object(main, "pending_habits_for_digest", side_effect=AssertionError("should use cached habits")):
            await main.handle_callback(update, MagicMock())

        query.edit_message_reply_markup.assert_awaited_once()
        query.answer.assert_awaited_once()
        self.assertEqual(main._habit_selection_selected(message.message_id), {page_id})


class TestSendDailyDigestHabitsIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_send_daily_digest_uses_pending_habits_for_digest_with_show_after(self):
        main = load_main_module()
        real_datetime = main.datetime

        main.habit_cache = {
            "Early Habit": {
                "page_id": "habit1",
                "name": "Early Habit",
                "show_after": "05:00",
                "sort": 1,
                "freq_per_week": 7,
            },
            "Late Habit": {
                "page_id": "habit2",
                "name": "Late Habit",
                "show_after": "18:00",
                "sort": 2,
                "freq_per_week": 7,
            },
        }

        bot_mock = MagicMock()
        sent = MagicMock(message_id=99)
        bot_mock.send_message = AsyncMock(return_value=sent)

        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False), \
            patch.object(main.notion_tasks, "get_today_and_overdue_tasks", return_value=[]), \
            patch.object(main.fmt, "format_digest_weather_card", return_value=None), \
            patch("second_brain.digest.datetime") as mock_dt:
            mock_dt.now.return_value = real_datetime(2026, 5, 7, 8, 0, tzinfo=main.TZ)

            await main.send_daily_digest(bot_mock, include_habits=True, config=None)

        bot_mock.send_message.assert_awaited()
        call_kwargs = bot_mock.send_message.await_args.kwargs
        button_labels = [
            button.text
            for row in call_kwargs["reply_markup"].inline_keyboard
            for button in row
        ]

        self.assertIn("*Habits:* tap to log:", call_kwargs.get("text", ""))
        self.assertIn("Early Habit", button_labels)
        self.assertNotIn("Late Habit", button_labels)


if __name__ == "__main__":
    unittest.main()
