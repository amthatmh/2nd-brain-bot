import importlib
import os
import sys
import unittest
from datetime import date
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
    "NOTION_STREAK_DB": "x",
}


def load_tasks_module():
    sys.modules.pop("second_brain.notion.tasks", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.notion.tasks")


class TestDigestTaskOrdering(unittest.TestCase):
    def test_get_today_and_overdue_tasks_prioritizes_nearest_deadlines(self):
        notion_tasks = load_tasks_module()
        today = date(2026, 5, 4)
        fake_tasks = [
            {"name": "Book TN Travels", "context": "💼 Work", "deadline": "2026-05-09", "auto_horizon": "🟠 This Week"},
            {"name": "T5 Check", "context": "💼 Work", "deadline": "2026-05-05", "auto_horizon": "🟠 This Week"},
            {"name": "Conduct monthly expense audit", "context": "🏠 Personal", "deadline": "2026-05-05", "auto_horizon": "🟠 This Week"},
            {"name": "Activities input", "context": "💼 Work", "deadline": "2026-05-08", "auto_horizon": "🟠 This Week"},
            {"name": "Fix sink drain", "context": "🏠 Personal", "deadline": "2026-05-07", "auto_horizon": "🟠 This Week"},
        ]

        with patch.object(notion_tasks, "get_all_active_tasks", return_value=fake_tasks), \
            patch.object(notion_tasks, "local_today", return_value=today):
            ordered = notion_tasks.get_today_and_overdue_tasks(None, "db", limit=3)

        self.assertEqual([t["name"] for t in ordered], ["Conduct monthly expense audit", "T5 Check", "Fix sink drain"])


if __name__ == "__main__":
    unittest.main()
