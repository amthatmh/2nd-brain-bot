"""Tests for dashboard weekly-activity selection (current week by date)."""

from __future__ import annotations

import unittest
from datetime import date

from second_brain.healthtrack.dashboard import _activity_score, current_week_entry

WEEKLY = [
    {"week": "2026-07-06", "workout_days": 6, "steps_days": 6},
]

# Wednesday of the week starting Monday 2026-07-13 — that week has no entry.
CURRENT_WEEK_START = date(2026, 7, 13)


class TestCurrentWeekEntry(unittest.TestCase):
    def test_returns_matching_week(self):
        entry = current_week_entry(WEEKLY, date(2026, 7, 6))
        self.assertEqual(entry["workout_days"], 6)

    def test_missing_week_defaults_to_zeros(self):
        entry = current_week_entry(WEEKLY, CURRENT_WEEK_START)
        self.assertEqual(entry, {"week": "2026-07-13", "workout_days": 0, "steps_days": 0})


class TestActivityScoreCurrentWeek(unittest.TestCase):
    def test_description_uses_current_week_not_last_populated_week(self):
        # Regression: with no completed rows this week, the description used
        # to read the previous week's totals as "this week".
        score = _activity_score(WEEKLY, CURRENT_WEEK_START)
        self.assertIn("0/7 workout days · 0/7 steps days this week", score["description"])
        self.assertIn("add 3 workout days", score["recommendation"])

    def test_description_uses_populated_current_week(self):
        weekly = WEEKLY + [{"week": "2026-07-13", "workout_days": 1, "steps_days": 2}]
        score = _activity_score(weekly, CURRENT_WEEK_START)
        self.assertIn("1/7 workout days · 2/7 steps days this week", score["description"])

    def test_no_week_start_falls_back_to_last_entry(self):
        score = _activity_score(WEEKLY)
        self.assertIn("6/7 workout days · 6/7 steps days this week", score["description"])

    def test_empty_weekly_is_no_data(self):
        score = _activity_score([], CURRENT_WEEK_START)
        self.assertIsNone(score["value"])


if __name__ == "__main__":
    unittest.main()
