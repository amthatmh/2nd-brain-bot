import importlib
import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from second_brain.services.note_utils import deadline_days_to_label


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


def load_formatters():
    sys.modules.pop("second_brain.formatters", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.formatters")


class TestDeadlineDaysToLabel(unittest.TestCase):
    def test_tomorrow_has_its_own_bucket(self):
        # Regression: a +1 day deadline used to collapse into "This Week".
        self.assertEqual(deadline_days_to_label(1), "🟢 Tomorrow")

    def test_other_buckets_unchanged(self):
        self.assertEqual(deadline_days_to_label(None), "⚪ Backburner")
        self.assertEqual(deadline_days_to_label(0), "🔴 Today")
        self.assertEqual(deadline_days_to_label(-3), "🔴 Today")
        self.assertEqual(deadline_days_to_label(2), "🟠 This Week")
        self.assertEqual(deadline_days_to_label(7), "🟠 This Week")
        self.assertEqual(deadline_days_to_label(8), "🟡 This Month")
        self.assertEqual(deadline_days_to_label(40), "⚪ Backburner")


class TestHorizonFromDeadline(unittest.TestCase):
    def test_derives_label_from_date_not_notion_formula(self):
        f = load_formatters()
        with patch.object(f, "local_today", return_value=date(2026, 6, 27)):
            self.assertEqual(f._horizon_from_deadline("2026-06-27"), "🔴 Today")
            # A next-day deadline must read as Tomorrow, never Today.
            self.assertEqual(f._horizon_from_deadline("2026-06-28"), "🟢 Tomorrow")
            self.assertEqual(f._horizon_from_deadline("2026-06-26"), "🔴 Today")  # overdue clamps to <=0
            self.assertEqual(f._horizon_from_deadline("2026-07-02"), "🟠 This Week")
            self.assertEqual(f._horizon_from_deadline(None), "⚪ Backburner")
            self.assertEqual(f._horizon_from_deadline("garbage"), "⚪ Backburner")


class TestDailyDigestTodayUsesDeadline(unittest.TestCase):
    def test_tomorrow_task_not_in_today_section(self):
        f = load_formatters()
        # auto_horizon says "Today" (the Notion mislabel) but the deadline is tomorrow.
        tasks = [
            {"name": "Call the plumber", "context": "🏠 Personal",
             "deadline": "2026-06-28", "auto_horizon": "🔴 Today"},
            {"name": "Real today task", "context": "🏠 Personal",
             "deadline": "2026-06-27", "auto_horizon": "🔴 Today"},
        ]
        with patch.object(f, "local_today", return_value=date(2026, 6, 27)), \
            patch.object(f, "format_weather_block", return_value="weather"), \
            patch.object(f, "weather_unavailable_digest_line", return_value="weather"), \
            patch.object(f.wx, "fetch_weather", return_value={}):
            text, ordered = f.format_daily_digest(tasks)

        names = [t["name"] for t in ordered]
        self.assertIn("Real today task", names)
        self.assertIn("Call the plumber", names)
        # The tomorrow task must be labeled Tomorrow in the carry-over section,
        # and must not be promoted into the Today section.
        self.assertIn("🟢 Tomorrow", text)
        today_section = text.split("Carry-over")[0]
        self.assertNotIn("Call the plumber", today_section)


if __name__ == "__main__":
    unittest.main()
