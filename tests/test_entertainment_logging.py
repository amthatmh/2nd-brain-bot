import importlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch


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


if __name__ == "__main__":
    unittest.main()
