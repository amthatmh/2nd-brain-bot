import unittest
import importlib
import os
import sys
from datetime import datetime as real_datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

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
    # Reload digest alongside main so both re-bind the *current*
    # second_brain.notion.tasks object. An earlier test may have re-imported
    # that module, leaving digest holding a stale reference; without this, a
    # patch on main.notion_tasks would not reach digest's copy and the real
    # query would run against an unset Notion client.
    sys.modules.pop("second_brain.main", None)
    sys.modules.pop("second_brain.digest", None)
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

    def test_load_habit_cache_reads_auto_only_checkbox(self):
        main = load_main_module()
        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": {
                "Habit": {"title": [{"text": {"content": "Sleep"}}]},
                "Active": {"checkbox": True},
                "Auto Only": {"checkbox": True},
            },
        }

        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

        self.assertTrue(main.habit_cache["Sleep"]["auto_only"])

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

    def test_load_habit_cache_reads_auto_only_checkbox(self):
        main = load_main_module()
        fake_habit = {
            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "properties": {
                "Habit": {"title": [{"text": {"content": "Weigh"}}]},
                "Active": {"checkbox": True},
                "Auto Only": {"checkbox": True},
            },
        }

        main.notion.databases.query = MagicMock(return_value={"results": [fake_habit]})
        main.notion_habits.load_habit_cache(notion=main.notion, notion_habit_db=main.NOTION_HABIT_DB)
        main._refresh_habit_cache_refs()

        self.assertTrue(main.habit_cache["Weigh"]["auto_only"])


class TestHabitLogQueries(unittest.TestCase):
    def test_log_habit_uses_configured_local_timezone_for_date(self):
        from second_brain.notion import habits as notion_habits

        notion = MagicMock()
        configured_tz = ZoneInfo("America/Los_Angeles")

        def fake_now(tz=None):
            utc_now = real_datetime(2026, 5, 26, 6, 47, tzinfo=timezone.utc)
            return utc_now.astimezone(tz) if tz is not None else utc_now

        with patch("second_brain.notion.habits.datetime") as mock_datetime:
            mock_datetime.now.side_effect = fake_now
            notion_habits.log_habit(
                notion=notion,
                log_db_id="log-db",
                habit_page_id="habit-1",
                habit_name="Read",
                tz=configured_tz,
            )

        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Date"]["date"]["start"], "2026-05-25")

    def test_already_logged_today_fails_closed_on_notion_error(self):
        from second_brain.notion import habits as notion_habits

        with patch.object(notion_habits, "query_all", side_effect=RuntimeError("notion down")):
            self.assertTrue(
                notion_habits.already_logged_today(
                    notion=MagicMock(),
                    log_db_id="log-db",
                    habit_page_id="habit-1",
                    tz=timezone.utc,
                )
            )

    def test_get_logged_habit_ids_today_returns_empty_set_on_failure(self):
        from second_brain.notion import habits as notion_habits

        with patch.object(notion_habits, "query_all", side_effect=RuntimeError("notion down")):
            self.assertEqual(
                notion_habits.get_logged_habit_ids_today(
                    notion=MagicMock(),
                    log_db_id="log-db",
                    tz=timezone.utc,
                ),
                set(),
            )


class _JsonRequest:
    method = "POST"

    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


class TestHabitKitHttpLogging(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        from second_brain.healthtrack import routes

        routes.STATE.habits_data_cache.clear()
        routes._habits_data_stale_cache.clear()

    async def test_log_habit_updates_stale_cache_when_already_logged(self):
        from second_brain.healthtrack import routes

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        routes.STATE.habits_data_cache["payload"] = {
            "dates": ["2026-05-29"],
            "todayDate": "2026-05-29",
            "habits": [{"id": page_id, "days": [0], "todayDone": False, "dayStreak": 0}],
        }

        with patch.object(routes, "already_logged_today", return_value=True) as already_logged, \
            patch.object(routes, "create_habit_log") as create_log, \
            patch.object(routes, "_schedule_habits_data_refresh") as schedule_refresh, \
            patch("second_brain.healthtrack.routes.datetime") as mock_datetime:
            mock_datetime.now.return_value = real_datetime(2026, 5, 29, tzinfo=timezone.utc)
            response = await routes.log_habit_http_handler(
                _JsonRequest({"habitId": page_id}),
                notion=MagicMock(),
                habit_cache={"Read": {"page_id": page_id, "name": "Read"}},
                log_db="log-db",
                habit_db="habit-db",
                streak_db="streak-db",
                tz=timezone.utc,
                weeks_history=24,
            )

        self.assertEqual(response.status, 200)
        payload = routes.STATE.habits_data_cache.get("payload")
        self.assertTrue(payload["habits"][0]["todayDone"])
        self.assertEqual(payload["habits"][0]["days"], [1])
        already_logged.assert_called_once()
        create_log.assert_not_called()
        schedule_refresh.assert_called_once()

    async def test_log_habit_accepts_undashed_page_id_from_web(self):
        from second_brain.healthtrack import routes

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

        with patch.object(routes, "already_logged_today", return_value=False), \
            patch.object(routes, "create_habit_log") as create_log, \
            patch.object(routes, "_schedule_habits_data_refresh"):
            response = await routes.log_habit_http_handler(
                _JsonRequest({"habitId": page_id.replace("-", "")}),
                notion=MagicMock(),
                habit_cache={"Read": {"page_id": page_id, "name": "Read"}},
                log_db="log-db",
                habit_db="habit-db",
                streak_db="streak-db",
                tz=timezone.utc,
                weeks_history=24,
            )

        self.assertEqual(response.status, 200)
        create_log.assert_called_once()
        self.assertEqual(create_log.call_args.args[2], page_id)


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

    def test_auto_only_habit_excluded_from_telegram_lists(self):
        main = load_main_module()
        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        main.habit_cache = {
            "Weigh": {"page_id": page_id, "name": "Weigh", "sort": 1, "auto_only": True}
        }

        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "is_on_pace", return_value=False):
            self.assertEqual(main.pending_habits_for_digest(time_str=None), [])


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
    async def test_open_habit_picker_uses_batched_logged_ids(self):
        main = load_main_module()
        logged_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        pending_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        main.habit_cache = {
            "Already Done": {"page_id": logged_id, "name": "Already Done", "sort": 1},
            "Workout": {"page_id": pending_id, "name": "Workout", "sort": 2},
        }
        sent = MagicMock(message_id=789)
        message = MagicMock()
        message.reply_text = AsyncMock(return_value=sent)

        with patch.object(main, "get_logged_habit_ids_today", return_value={logged_id}) as mock_logged_ids, \
            patch.object(main, "already_logged_today", side_effect=AssertionError("should use batched query")):
            await main.open_habit_picker(message)

        mock_logged_ids.assert_called_once_with()
        message.reply_text.assert_awaited_once()
        labels = [
            button.text
            for row in message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("Workout", labels)
        self.assertNotIn("Already Done", labels)
        self.assertEqual(main._habit_selection_habits(sent.message_id), [main.habit_cache["Workout"]])

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

        with patch("second_brain.digest.pending_habits_for_digest", side_effect=AssertionError("should use cached habits")):
            await main.handle_callback(update, MagicMock())

        query.edit_message_reply_markup.assert_awaited_once()
        query.answer.assert_awaited_once()
        self.assertEqual(main._habit_selection_selected(message.message_id), {page_id})

    async def test_toggle_missing_session_recovers_only_message_habits_without_notion_dedupe(self):
        main = load_main_module()
        import second_brain.routers as routers

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        late_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        main.habit_cache = {
            "Workout": {"page_id": page_id, "name": "Workout", "sort": 1},
            "Late Habit": {"page_id": late_id, "name": "Late Habit", "sort": 2},
        }
        message = MagicMock()
        message.message_id = 456
        message.text = "🏃 *Which habit did you complete?*"
        message.caption = None
        message.reply_markup = main.kb.habit_buttons([main.habit_cache["Workout"]], "morning", selected=set())
        main._habit_selections.pop(message.message_id, None)

        query = MagicMock()
        query.data = "h:toggle:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        query.message = message
        query.edit_message_reply_markup = AsyncMock()
        query.answer = AsyncMock()

        with patch.object(main, "already_logged_today", side_effect=AssertionError("should not query Notion")):
            await routers._cb_h_toggle(query, ["h", "toggle", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], MagicMock())

        query.edit_message_reply_markup.assert_awaited_once()
        query.answer.assert_awaited_once()
        self.assertEqual(main._habit_selection_selected(message.message_id), {page_id})
        self.assertEqual(main._habit_selection_habits(message.message_id), [main.habit_cache["Workout"]])
        button_labels = [
            button.text
            for row in query.edit_message_reply_markup.await_args.kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("✅ Workout", button_labels)
        self.assertNotIn("Late Habit", button_labels)

    def test_habit_selection_cleanup_preserves_recent_sessions(self):
        main = load_main_module()
        fresh_habit = {"page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Workout", "sort": 1}
        old_habit = {"page_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "name": "Read", "sort": 2}

        main._store_habit_selection_session(101, [fresh_habit])
        main._store_habit_selection_session(202, [old_habit])
        main._habit_selections[202]["created_at"] = main.time.time() - main._HABIT_SELECTION_TTL_SECONDS - 1

        main.cleanup_old_habit_selections()

        self.assertIn(101, main._habit_selections)
        self.assertNotIn(202, main._habit_selections)


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
            patch("second_brain.main.datetime") as mock_dt:
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


class TestPendingHabitsForDate(unittest.TestCase):
    def test_excludes_auto_only_logged_and_on_pace(self):
        from second_brain.digest import pending_habits_for_date

        cache = {
            "Magnesium": {"page_id": "m", "name": "Magnesium", "sort": 1},
            "Read": {"page_id": "r", "name": "Read", "sort": 2},
            "Workout": {"page_id": "w", "name": "Workout", "sort": 3},
            "Weigh": {"page_id": "wg", "name": "Weigh", "sort": 4, "auto_only": True},
        }
        logged = {"r"}  # Read was already checked off yesterday
        on_pace = {"w"}  # Workout already met its weekly target

        result = pending_habits_for_date(
            habit_cache=cache,
            already_logged=lambda pid: pid in logged,
            is_on_pace=lambda habit: habit["page_id"] in on_pace,
        )

        self.assertEqual([habit["name"] for habit in result], ["Magnesium"])

    def test_ignores_show_after_gating(self):
        from second_brain.digest import pending_habits_for_date

        cache = {
            "Evening Habit": {"page_id": "e", "name": "Evening Habit", "sort": 1, "show_after": "22:00"},
        }

        result = pending_habits_for_date(
            habit_cache=cache,
            already_logged=lambda pid: False,
            is_on_pace=lambda habit: False,
        )

        self.assertEqual([habit["name"] for habit in result], ["Evening Habit"])


class TestYesterdayHabitCatchup(unittest.IsolatedAsyncioTestCase):
    async def test_sends_buttons_and_stores_yesterday_session(self):
        main = load_main_module()
        from second_brain import digest
        from second_brain.notion import habits as notion_habits

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        notion_habits.habit_cache = {
            "Magnesium": {"page_id": page_id, "name": "Magnesium", "sort": 1, "late_night": True},
        }
        digest._notion = MagicMock()
        digest._store_habit_session_fn = main._store_habit_selection_session

        bot = MagicMock()
        sent = MagicMock(message_id=555)
        bot.send_message = AsyncMock(return_value=sent)

        expected_date = (digest.datetime.now(digest.TZ) - digest.timedelta(days=1)).date().isoformat()

        with patch.object(notion_habits, "already_logged_today", return_value=False), \
            patch.object(notion_habits, "is_on_pace", return_value=False):
            await digest.send_yesterday_habit_catchup(bot)

        bot.send_message.assert_awaited_once()
        call_kwargs = bot.send_message.await_args.kwargs
        self.assertIn("Yesterday's habits", call_kwargs["text"])
        labels = [
            button.text
            for row in call_kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("Magnesium", labels)
        self.assertEqual(main._habit_selections[sent.message_id]["log_date"], expected_date)

    async def test_no_message_when_nothing_pending(self):
        main = load_main_module()
        from second_brain import digest
        from second_brain.notion import habits as notion_habits

        notion_habits.habit_cache = {
            "Read": {"page_id": "r", "name": "Read", "sort": 1, "late_night": True},
        }
        digest._notion = MagicMock()

        bot = MagicMock()
        bot.send_message = AsyncMock()

        with patch.object(notion_habits, "already_logged_today", return_value=True), \
            patch.object(notion_habits, "is_on_pace", return_value=False):
            await digest.send_yesterday_habit_catchup(bot)

        bot.send_message.assert_not_awaited()

    async def test_only_late_night_habits_included(self):
        main = load_main_module()
        from second_brain import digest
        from second_brain.notion import habits as notion_habits

        notion_habits.habit_cache = {
            "Magnesium": {"page_id": "m", "name": "Magnesium", "sort": 1, "late_night": True},
            "Workout": {"page_id": "w", "name": "Workout", "sort": 2, "late_night": True},
            "Stretching": {"page_id": "s", "name": "Stretching", "sort": 3, "late_night": False},
            "Water2L": {"page_id": "wa", "name": "Water2L", "sort": 4},
        }
        digest._notion = MagicMock()
        digest._store_habit_session_fn = main._store_habit_selection_session

        bot = MagicMock()
        sent = MagicMock(message_id=556)
        bot.send_message = AsyncMock(return_value=sent)

        with patch.object(notion_habits, "already_logged_today", return_value=False), \
            patch.object(notion_habits, "is_on_pace", return_value=False):
            await digest.send_yesterday_habit_catchup(bot)

        bot.send_message.assert_awaited_once()
        call_kwargs = bot.send_message.await_args.kwargs
        labels = [
            button.text
            for row in call_kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("Magnesium", labels)
        self.assertIn("Workout", labels)
        self.assertNotIn("Stretching", labels)
        self.assertNotIn("Water2L", labels)

    async def test_no_message_when_no_late_night_habits_exist(self):
        load_main_module()
        from second_brain import digest
        from second_brain.notion import habits as notion_habits

        notion_habits.habit_cache = {
            "Stretching": {"page_id": "s", "name": "Stretching", "sort": 1},
        }
        digest._notion = MagicMock()

        bot = MagicMock()
        bot.send_message = AsyncMock()

        with patch.object(notion_habits, "already_logged_today", return_value=False), \
            patch.object(notion_habits, "is_on_pace", return_value=False):
            await digest.send_yesterday_habit_catchup(bot)

        bot.send_message.assert_not_awaited()


class TestYesterdayHabitDoneLogsForYesterday(unittest.IsolatedAsyncioTestCase):
    async def test_done_logs_selected_habit_against_session_log_date(self):
        main = load_main_module()
        import second_brain.routers as routers

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        main.habit_cache = {"Magnesium": {"page_id": page_id, "name": "Magnesium", "sort": 1}}
        main._store_habit_selection_session(
            777, list(main.habit_cache.values()), selected={page_id}, log_date="2026-07-03"
        )

        query = MagicMock()
        query.message = MagicMock(
            message_id=777,
            text="🌙 Yesterday's habits — did you do any of these Friday? Tap to log:",
        )
        query.edit_message_reply_markup = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message.reply_text = AsyncMock()
        query.answer = AsyncMock()

        context = MagicMock()

        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "log_habit") as mock_log, \
            patch.object(main, "check_and_notify_weekly_goals", new=AsyncMock(return_value=set())), \
            patch.object(main, "get_week_completion_count", return_value=1), \
            patch.object(main, "get_habit_frequency", return_value=7):
            await routers._cb_h_done(query, ["h", "done"], context)

        mock_log.assert_called_once()
        self.assertEqual(mock_log.call_args.args[0], page_id)
        self.assertEqual(mock_log.call_args.kwargs.get("log_date"), "2026-07-03")

    async def test_done_folds_summary_into_prompt_message(self):
        """Manual "Which habit did you complete?" prompt is edited into the
        logged summary instead of leaving the prompt + a new reply bubble."""
        main = load_main_module()
        import second_brain.routers as routers

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        main.habit_cache = {"Workout": {"page_id": page_id, "name": "Workout", "sort": 1}}
        main._store_habit_selection_session(
            779, list(main.habit_cache.values()), selected={page_id}, log_date=None
        )

        query = MagicMock()
        query.message = MagicMock(message_id=779, text="🏃 Which habit did you complete?")
        query.edit_message_reply_markup = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message.reply_text = AsyncMock()
        query.answer = AsyncMock()

        context = MagicMock()

        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "log_habit"), \
            patch.object(main, "check_and_notify_weekly_goals", new=AsyncMock(return_value=set())), \
            patch.object(main, "get_week_completion_count", return_value=1), \
            patch.object(main, "get_habit_frequency", return_value=7):
            await routers._cb_h_done(query, ["h", "done"], context)

        query.edit_message_text.assert_awaited_once()
        self.assertIn("✅ Logged: Workout", query.edit_message_text.await_args.args[0])
        query.message.reply_text.assert_not_awaited()

    async def test_done_recovers_log_date_from_message_when_session_lost(self):
        """A redeploy wipes the in-memory session (and its log_date); the done
        handler must recover the target date from the catch-up message itself
        instead of silently logging against today."""
        main = load_main_module()
        import second_brain.routers as routers
        from datetime import datetime, timezone

        page_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        main.habit_cache = {"Magnesium": {"page_id": page_id, "name": "Magnesium", "sort": 1}}
        # Session rebuilt after restart: selection present, log_date lost.
        main._store_habit_selection_session(
            778, list(main.habit_cache.values()), selected={page_id}, log_date=None
        )

        sent_at = datetime(2026, 7, 7, 13, 15, tzinfo=timezone.utc)  # morning local time
        expected_date = (sent_at.astimezone(routers.TZ).date() - routers.timedelta(days=1)).isoformat()

        query = MagicMock()
        query.message = MagicMock(
            message_id=778,
            text="🌙 Yesterday's habits — did you do any of these Monday? Tap to log:",
            date=sent_at,
        )
        query.edit_message_reply_markup = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message.reply_text = AsyncMock()
        query.answer = AsyncMock()

        context = MagicMock()

        with patch.object(main, "already_logged_today", return_value=False), \
            patch.object(main, "log_habit") as mock_log, \
            patch.object(main, "check_and_notify_weekly_goals", new=AsyncMock(return_value=set())), \
            patch.object(main, "get_week_completion_count", return_value=1), \
            patch.object(main, "get_habit_frequency", return_value=7):
            await routers._cb_h_done(query, ["h", "done"], context)

        mock_log.assert_called_once()
        self.assertEqual(mock_log.call_args.args[0], page_id)
        self.assertEqual(mock_log.call_args.kwargs.get("log_date"), expected_date)


if __name__ == "__main__":
    unittest.main()
