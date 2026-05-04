import unittest
import importlib
import os
import sys
from unittest.mock import MagicMock, patch

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
                "Time": {"select": {"name": "20:00"}},
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


if __name__ == "__main__":
    unittest.main()
