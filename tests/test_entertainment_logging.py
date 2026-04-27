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
}


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


class TestEntertainmentLoggingHelpers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = load_main_module()

    def test_performance_schema_venue_select_not_confused_with_source(self):
        schema = {
            "Name": "title",
            "Date": "date",
            "Venue": "select",
            "Source": "select",
            "Notes": "rich_text",
        }

        props = self.main._build_common_entertainment_props(
            schema,
            title="ABBA Voyage",
            when_iso="2026-04-26",
            venue="ABBA Arena",
            notes="Front row",
        )

        self.assertEqual(props["Venue"]["select"]["name"], "ABBA Arena")
        self.assertEqual(props["Source"]["select"]["name"], "📱 Telegram")

    def test_unsupported_place_property_falls_back_to_notes(self):
        schema = {
            "Film": "title",
            "Date": "date",
            "Place": "number",
            "Notes": "rich_text",
        }

        props = self.main._build_common_entertainment_props(
            schema,
            title="Dune",
            when_iso="2026-04-26",
            venue="AMC River East",
            notes="IMAX",
        )

        self.assertNotIn("Place", props)
        self.assertEqual(
            props["Notes"]["rich_text"][0]["text"]["content"],
            "IMAX\nVenue: AMC River East",
        )

    def test_parse_explicit_log_command_for_sport(self):
        parsed = self.main.parse_explicit_entertainment_log("/log sport Cubs vs Sox at Wrigley")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["type"], "entertainment_log")
        self.assertEqual(parsed["log_type"], "sport")
        self.assertEqual(parsed["title"], "Cubs vs Sox")
        self.assertEqual(parsed["venue"], "Wrigley")

    def test_parse_explicit_log_command_maps_movie_keyword_to_cinema(self):
        parsed = self.main.parse_explicit_entertainment_log("/log movie The Drama at AMC Roosevelt Collection 16")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["type"], "entertainment_log")
        self.assertEqual(parsed["log_type"], "cinema")
        self.assertEqual(parsed["title"], "The Drama")
        self.assertEqual(parsed["venue"], "AMC Roosevelt Collection 16")

    def test_parse_explicit_cinema_preserves_venue_and_datetime_with_structured_tail(self):
        parsed = self.main.parse_explicit_entertainment_log(
            "/log cinema The Drama at AMC Roosevelt Collection 16 on 2026/04/30 at 20:40 Seat D6 Auditorium D5 Mark favourite"
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["log_type"], "cinema")
        self.assertEqual(parsed["title"], "The Drama")
        self.assertEqual(parsed["venue"], "AMC Roosevelt Collection 16")
        self.assertEqual(parsed["date"], "2026-04-30T20:40:00")
        self.assertEqual(parsed["notes"], "Seat D6 Auditorium D5")
        self.assertTrue(parsed["favourite"])

    def test_parse_explicit_performance_parses_date_time_and_tail_notes(self):
        parsed = self.main.parse_explicit_entertainment_log(
            "/log performance The Drama at Martin Theatre on 2026/04/29 at 20:40 Seat D6"
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["log_type"], "performance")
        self.assertEqual(parsed["title"], "The Drama")
        self.assertEqual(parsed["venue"], "Martin Theatre")
        self.assertEqual(parsed["date"], "2026-04-29T20:40:00")
        self.assertEqual(parsed["notes"], "Seat D6")

    def test_parse_explicit_log_command_for_sports_plural_and_action_verb(self):
        parsed = self.main.parse_explicit_entertainment_log("/log Sports watched Bears vs Arsenal at Soldier Field")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["log_type"], "sport")
        self.assertEqual(parsed["title"], "Bears vs Arsenal")
        self.assertEqual(parsed["venue"], "Soldier Field")

    def test_parse_explicit_log_command_extracts_trailing_date_and_time(self):
        parsed = self.main.parse_explicit_entertainment_log(
            "/log sport Bears vs Arsenal at Soldier Field on 2026-04-27 at 21:00"
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["title"], "Bears vs Arsenal")
        self.assertEqual(parsed["venue"], "Soldier Field")
        self.assertEqual(parsed["date"], "2026-04-27T21:00:00")

    def test_extract_cinema_visit_details(self):
        seat, auditorium = self.main._extract_cinema_visit_details("Seat D6, Auditorium D5, 20:40")
        self.assertEqual(seat, "D6")
        self.assertEqual(auditorium, 5)

    def test_place_status_property_is_supported(self):
        schema = {
            "Film": "title",
            "Date": "date",
            "Place": "status",
            "Notes": "rich_text",
        }
        props = self.main._build_common_entertainment_props(
            schema,
            title="Dune",
            when_iso="2026-04-26",
            venue="AMC Roosevelt Collection 16",
            notes=None,
        )
        self.assertEqual(props["Place"]["status"]["name"], "AMC Roosevelt Collection 16")

    def test_normalize_entertainment_datetime_extracts_time_from_notes(self):
        normalized = self.main._normalize_entertainment_datetime(
            "2026-04-26",
            "Seat D6, Auditorium D5, 20:40",
        )
        self.assertEqual(normalized, "2026-04-26T20:40:00")

    def test_strip_cinema_structured_notes_removes_redundant_fields(self):
        cleaned = self.main._strip_cinema_structured_notes("Seat D6, Auditorium D5, 20:40")
        self.assertIsNone(cleaned)

    def test_find_existing_cinema_venue_uses_previous_logs(self):
        schema = {
            "Film": "title",
            "Place": "status",
            "Date": "date",
        }
        self.main.NOTION_CINEMA_LOG_DB = "cinema_db"
        self.main.notion_call = MagicMock(return_value={
            "results": [
                {
                    "properties": {
                        "Film": {"title": [{"plain_text": "The Drama"}]},
                        "Place": {"type": "status", "status": {"name": "AMC Roosevelt Collection 16"}},
                    }
                }
            ]
        })
        venue = self.main._find_existing_cinema_venue("The Drama", schema)
        self.assertEqual(venue, "AMC Roosevelt Collection 16")

    def test_parse_cinema_inline_context(self):
        parsed = self.main._parse_cinema_inline_context(
            "The Drama at AMC Roosevelt Collection 16 on 2026/04/27 at 20:40 Seat D6 Auditorium D5"
        )
        self.assertEqual(parsed["title"], "The Drama")
        self.assertEqual(parsed["venue"], "AMC Roosevelt Collection 16")
        self.assertEqual(parsed["date"], "2026/04/27")
        self.assertEqual(parsed["time"], "20:40")
        self.assertEqual(parsed["tail"], "Seat D6 Auditorium D5")

    def test_create_cinema_entry_parses_inline_title_to_fix_date_place_notes(self):
        self.main.NOTION_CINEMA_LOG_DB = "cinema_db"
        self.main.entertainment_schemas["cinema"] = {
            "Film": "title",
            "Date": "date",
            "Place": "status",
            "Notes": "rich_text",
            "Seat": "rich_text",
            "Auditorium": "number",
        }

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.pages.create:
                props = kwargs["properties"]
                self.assertEqual(props["Date"]["date"]["start"], "2026-04-27T20:40:00")
                self.assertEqual(props["Date"]["date"]["time_zone"], "America/Chicago")
                self.assertEqual(props["Place"]["status"]["name"], "AMC Roosevelt Collection 16")
                self.assertNotIn("Notes", props)
                self.assertEqual(props["Seat"]["rich_text"][0]["text"]["content"], "D6")
                self.assertEqual(props["Auditorium"]["number"], 5)
                return {"id": "page-1"}
            if fn == self.main.notion.databases.query:
                return {"results": []}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, fav_saved = self.main.create_entertainment_log_entry({
            "log_type": "cinema",
            "title": "The Drama at AMC Roosevelt Collection 16 on 2026/04/27 at 20:40 Seat D6 Auditorium D5",
            "date": "2026-04-27",
            "venue": None,
            "notes": None,
            "favourite": False,
        })
        self.assertEqual(page_id, "page-1")
        self.assertFalse(fav_saved)

    def test_create_performance_entry_lazy_loads_schema_when_missing(self):
        self.main.NOTION_PERFORMANCES_DB = "performances_db"
        self.main.entertainment_schemas.pop("performances", None)

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.databases.retrieve:
                return {
                    "properties": {
                        "Name": {"type": "title"},
                        "Date": {"type": "date"},
                        "Venue": {"type": "select"},
                        "Notes": {"type": "rich_text"},
                    }
                }
            if fn == self.main.notion.pages.create:
                props = kwargs["properties"]
                self.assertEqual(props["Name"]["title"][0]["text"]["content"], "The Drama")
                self.assertEqual(props["Venue"]["select"]["name"], "Martin Theatre")
                self.assertEqual(props["Date"]["date"]["start"], "2026-04-29T20:40:00")
                self.assertEqual(props["Notes"]["rich_text"][0]["text"]["content"], "Seat D6")
                return {"id": "perf-1"}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, fav_saved = self.main.create_entertainment_log_entry({
            "log_type": "performance",
            "title": "The Drama",
            "date": "2026-04-29T20:40:00",
            "venue": "Martin Theatre",
            "notes": "Seat D6",
            "favourite": False,
        })
        self.assertEqual(page_id, "perf-1")
        self.assertFalse(fav_saved)

    def test_create_performance_entry_retries_without_select_fields_on_write_error(self):
        self.main.NOTION_PERFORMANCES_DB = "performances_db"
        self.main.entertainment_schemas["performances"] = {
            "Name": "title",
            "Date": "date",
            "Venue": "select",
            "Source": "select",
            "Notes": "rich_text",
        }
        calls = {"create": 0}

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.pages.create:
                calls["create"] += 1
                props = kwargs["properties"]
                if calls["create"] == 1:
                    raise RuntimeError("invalid select option")
                self.assertNotIn("Venue", props)
                self.assertNotIn("Source", props)
                self.assertIn("Notes", props)
                notes_text = props["Notes"]["rich_text"][0]["text"]["content"]
                self.assertIn("Seat D6", notes_text)
                self.assertIn("Venue: Martin Theatre", notes_text)
                return {"id": "perf-2"}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, fav_saved = self.main.create_entertainment_log_entry({
            "log_type": "performance",
            "title": "The Drama",
            "date": "2026-04-29T20:40:00",
            "venue": "Martin Theatre",
            "notes": "Seat D6",
            "favourite": False,
        })
        self.assertEqual(page_id, "perf-2")
        self.assertFalse(fav_saved)
        self.assertEqual(calls["create"], 2)

    def test_suggest_known_venue_returns_best_cinema_match(self):
        self.main.NOTION_CINEMA_LOG_DB = "cinema_db"
        self.main.entertainment_schemas["cinema"] = {
            "Film": "title",
            "Venue": "select",
            "Date": "date",
        }
        self.main.notion_call = MagicMock(return_value={
            "results": [
                {
                    "properties": {
                        "Film": {"title": [{"plain_text": "The Drama"}]},
                        "Venue": {"type": "select", "select": {"name": "AMC Roosevelt Collection 16"}},
                    }
                }
            ]
        })
        original, suggested = self.main._suggest_known_venue({
            "log_type": "cinema",
            "venue": "AMC Roosevelt",
        })
        self.assertEqual(original, "AMC Roosevelt")
        self.assertEqual(suggested, "AMC Roosevelt Collection 16")

    def test_suggest_known_venue_works_for_performance_logs(self):
        self.main.NOTION_PERFORMANCES_DB = "performances_db"
        self.main.entertainment_schemas["performances"] = {
            "Name": "title",
            "Place": "status",
            "Date": "date",
        }
        self.main.notion_call = MagicMock(return_value={
            "results": [
                {
                    "properties": {
                        "Name": {"title": [{"plain_text": "The Drama"}]},
                        "Place": {"type": "status", "status": {"name": "Martin Theatre"}},
                    }
                }
            ]
        })
        original, suggested = self.main._suggest_known_venue({
            "log_type": "performance",
            "venue": "martin",
        })
        self.assertEqual(original, "martin")
        self.assertEqual(suggested, "Martin Theatre")

    def test_entertainment_save_error_text_for_missing_performance_schema(self):
        msg = self.main._entertainment_save_error_text(
            ValueError("Performances schema is unavailable"),
            {"log_type": "performance"},
        )
        self.assertIn("NOTION_PERFORMANCES_DB", msg)


class TestEntertainmentEnvFallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = load_main_module()

    def test_legacy_performance_env_var_is_accepted(self):
        legacy_env = dict(REQUIRED_ENV)
        legacy_env.pop("NOTION_PERFORMANCES_DB", None)
        legacy_env["NOTION_PERFORMANCE_DB"] = "legacy_performance_db"
        sys.modules.pop("second_brain.main", None)
        with patch.dict(os.environ, legacy_env, clear=False), \
            patch("notion_client.Client", return_value=MagicMock()), \
            patch("anthropic.Anthropic", return_value=MagicMock()):
            main = importlib.import_module("second_brain.main")
        self.assertEqual(main.NOTION_PERFORMANCES_DB, "legacy_performance_db")

    def test_known_cinema_venue_is_normalized_from_previous_rows(self):
        schema = {
            "Film": "title",
            "Place": "status",
            "Date": "date",
        }
        self.main.NOTION_CINEMA_LOG_DB = "cinema_db"
        self.main.notion_call = MagicMock(return_value={
            "results": [
                {
                    "properties": {
                        "Film": {"title": [{"plain_text": "The Drama"}]},
                        "Place": {"type": "status", "status": {"name": "AMC Roosevelt Collection 16"}},
                    }
                }
            ]
        })
        normalized = self.main._resolve_known_cinema_venue("amc roosevelt", schema)
        self.assertEqual(normalized, "AMC Roosevelt Collection 16")

    def test_inline_date_does_not_override_payload_date_but_adds_time(self):
        self.main.NOTION_CINEMA_LOG_DB = "cinema_db"
        self.main.entertainment_schemas["cinema"] = {
            "Film": "title",
            "Date": "date",
            "Place": "status",
            "Notes": "rich_text",
            "Seat": "rich_text",
            "Auditorium": "number",
        }

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.pages.create:
                props = kwargs["properties"]
                self.assertEqual(props["Date"]["date"]["start"], "2026-04-25T20:40:00")
                self.assertEqual(props["Date"]["date"]["time_zone"], "America/Chicago")
                self.assertNotIn("Notes", props)
                return {"id": "page-2"}
            if fn == self.main.notion.databases.query:
                return {"results": []}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, _ = self.main.create_entertainment_log_entry({
            "log_type": "cinema",
            "title": "The Drama at AMC Roosevelt Collection 16 on 2026/04/27 at 20:40 Seat D6 Auditorium D5",
            "date": "2026-04-25",
            "venue": None,
            "notes": None,
            "favourite": False,
        })
        self.assertEqual(page_id, "page-2")

    def test_place_property_lookup_is_case_insensitive(self):
        schema = {
            "Film": "title",
            "date": "date",
            "place": "status",
        }
        props = self.main._build_common_entertainment_props(
            schema,
            title="The Drama",
            when_iso="2026-04-28T20:40:00",
            venue="AMC Roosevelt Collection 16",
            notes=None,
        )
        self.assertEqual(props["place"]["status"]["name"], "AMC Roosevelt Collection 16")

    def test_strip_datetime_from_notes_removes_redundant_time(self):
        cleaned = self.main._strip_datetime_from_notes("21:00 Venue: Soldier Field")
        self.assertEqual(cleaned, "Venue: Soldier Field")

    def test_create_sports_entry_extracts_time_and_cleans_notes(self):
        self.main.NOTION_SPORTS_LOG_DB = "sports_db"
        self.main.entertainment_schemas["sports"] = {
            "Game": "title",
            "Date": "date",
            "Notes": "rich_text",
            "Place": "status",
        }

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.pages.create:
                props = kwargs["properties"]
                self.assertEqual(props["Date"]["date"]["start"], "2026-02-01T21:00:00")
                self.assertEqual(props["Date"]["date"]["time_zone"], "America/Chicago")
                self.assertEqual(props["Notes"]["rich_text"][0]["text"]["content"], "Venue: Soldier Field")
                return {"id": "sport-page-1"}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, fav_saved = self.main.create_entertainment_log_entry({
            "log_type": "sport",
            "title": "Cubs vs Dodgers",
            "date": "2026-02-01",
            "venue": None,
            "notes": "21:00 Venue: Soldier Field",
            "favourite": False,
        })
        self.assertEqual(page_id, "sport-page-1")
        self.assertFalse(fav_saved)

    def test_create_sports_entry_maps_seat_to_seat_column(self):
        self.main.NOTION_SPORTS_LOG_DB = "sports_db"
        self.main.entertainment_schemas["sports"] = {
            "Game": "title",
            "Date": "date",
            "Notes": "rich_text",
            "Seat": "rich_text",
            "Venue": "select",
        }

        def fake_notion_call(fn, **kwargs):
            if fn == self.main.notion.pages.create:
                props = kwargs["properties"]
                self.assertEqual(props["Seat"]["rich_text"][0]["text"]["content"], "D9")
                self.assertNotIn("Notes", props)
                return {"id": "sport-page-2"}
            return {}

        self.main.notion_call = fake_notion_call
        page_id, fav_saved = self.main.create_entertainment_log_entry({
            "log_type": "sport",
            "title": "The Movie",
            "date": "2026-04-26T20:35:00",
            "venue": "House of Blues",
            "notes": "Seat D9",
            "favourite": False,
        })
        self.assertEqual(page_id, "sport-page-2")
        self.assertFalse(fav_saved)


class TestEntertainmentLogFollowups(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = load_main_module()

    async def test_handle_entertainment_log_prompts_competition_for_sports(self):
        message = MagicMock()
        message.chat = MagicMock(id=1)
        message.reply_text = AsyncMock()
        with patch.object(self.main, "create_entertainment_log_entry", return_value=("sport-page", False)):
            await self.main.handle_entertainment_log(message, {
                "log_type": "sport",
                "title": "Cubs vs Dodgers",
                "date": "2026-02-01",
                "venue": "Soldier Field",
                "notes": None,
            })
        self.assertEqual(message.reply_text.await_count, 2)
        second_call_text = message.reply_text.await_args_list[1].args[0]
        self.assertIn("competition", second_call_text.lower())
        self.assertEqual(self.main.pending_sport_competition_map[1]["page_id"], "sport-page")

    async def test_handle_message_text_sets_sport_competition_followup(self):
        self.main.pending_sport_competition_map.clear()
        self.main.pending_sport_competition_map[1] = {"page_id": "sport-page"}
        self.main.entertainment_schemas["sports"] = {"Competition": "select"}
        self.main.notion_call = MagicMock()
        self.main.route_classified_message_v10 = AsyncMock()

        update = MagicMock()
        update.effective_chat.id = 1
        update.message = MagicMock()
        update.message.text = "Major League Baseball"
        update.message.reply_text = AsyncMock()
        context = MagicMock()
        context.user_data = {}

        await self.main.handle_message_text(update, context)

        self.main.notion_call.assert_called_once_with(
            self.main.notion.pages.update,
            page_id="sport-page",
            properties={"Competition": {"select": {"name": "Major League Baseball"}}},
        )
        self.assertNotIn(1, self.main.pending_sport_competition_map)
        self.main.route_classified_message_v10.assert_not_called()


if __name__ == "__main__":
    unittest.main()
