"""Tests for find_duplicate_active_task / fuzzy_match edge cases."""

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


class TestTaskDedup(unittest.TestCase):
    def test_exact_match_returns_task(self):
        main = load_main_module()
        tasks = [{"name": "Pay electricity bill", "page_id": "1"}]
        self.assertEqual(main.fuzzy_match("Pay electricity bill", tasks), tasks[0])

    def test_partial_substring_match_returns_task(self):
        main = load_main_module()
        tasks = [{"name": "Call Alice about taxes", "page_id": "1"}]
        self.assertEqual(main.fuzzy_match("call alice", tasks), tasks[0])

    def test_no_match_returns_none(self):
        main = load_main_module()
        tasks = [{"name": "Book dentist", "page_id": "1"}]
        self.assertIsNone(main.fuzzy_match("renew passport", tasks))

    def test_normalization_strips_punctuation_and_trailing_s(self):
        main = load_main_module()
        task = {"name": "Clean emails!!!", "page_id": "123"}
        with patch.object(main, "get_all_active_tasks", return_value=[task]):
            found = main.find_duplicate_active_task("clean email")
        self.assertEqual(found, task)


if __name__ == "__main__":
    unittest.main()
