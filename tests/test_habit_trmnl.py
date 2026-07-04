"""Tests for the TRMNL habit tracker card logic."""

from __future__ import annotations

import unittest
from datetime import date

from second_brain.habitkit.trmnl import _clean_name, _sleep_state, build_habit_card_payload


class TestCleanName(unittest.TestCase):
    def test_strips_leading_emoji(self):
        self.assertEqual(_clean_name("💊💊 Allergy Meds"), "Allergy Meds")
        self.assertEqual(_clean_name("💪💪 Workout"), "Workout")

    def test_keeps_plain_names(self):
        self.assertEqual(_clean_name("Water2L"), "Water2L")
        self.assertEqual(_clean_name("Handstand+PushUp"), "Handstand+PushUp")

    def test_handles_empty(self):
        self.assertEqual(_clean_name(None), "")
        self.assertEqual(_clean_name(""), "")

TODAY = date(2026, 7, 5)  # a Sunday


def _habits_data(habits, dates=None):
    # 8-day history so the 7-day window slices off the oldest day.
    dates = dates or [f"2026-06-{d:02d}" for d in range(28, 31)] + [
        f"2026-07-{d:02d}" for d in range(1, 6)
    ]
    return {
        "generated": "2026-07-05T12:00:00+00:00",
        "todayDate": TODAY.isoformat(),
        "dates": dates,
        "habits": habits,
    }


def _habit(name, days, trmnl=True, icon="💪", streak=0, today_done=False):
    return {
        "name": name,
        "icon": icon,
        "days": days,
        "trmnl": trmnl,
        "dayStreak": streak,
        "todayDone": today_done,
    }


class TestSleepDotGrading(unittest.TestCase):
    def test_sleep_state_thresholds(self):
        self.assertEqual(_sleep_state(430, 420), 1)  # met goal
        self.assertEqual(_sleep_state(420, 420), 1)  # exactly goal
        self.assertEqual(_sleep_state(400, 420), 2)  # 6-7h -> partial
        self.assertEqual(_sleep_state(360, 420), 2)  # exactly 6h -> partial
        self.assertEqual(_sleep_state(359, 420), 0)  # >1h short -> missed
        self.assertEqual(_sleep_state(0, 420), 0)

    def test_sleep_habit_dots_graded_from_minutes(self):
        sleep = _habit("Sleep", [1] * 8)  # log days ignored once minutes present
        sleep["sleepMinutes"] = [430, 400, 350, 420, 380, 300, 415, 440]
        sleep["sleepThreshold"] = 420
        p = build_habit_card_payload(_habits_data([sleep]), TODAY)
        # last 7 minutes -> [400,350,420,380,300,415,440]
        self.assertEqual(p["habits"][0]["days"], [2, 0, 1, 2, 0, 2, 1])

    def test_non_sleep_habit_stays_binary(self):
        p = build_habit_card_payload(_habits_data([_habit("Workout", [1, 0, 1, 0, 1, 1, 1, 0])]), TODAY)
        self.assertEqual(set(p["habits"][0]["days"]) <= {0, 1}, True)


class TestBuildHabitCardPayload(unittest.TestCase):
    def test_only_flagged_habits_shown(self):
        data = _habits_data(
            [
                _habit("Workout", [1] * 8, trmnl=True),
                _habit("Stretching", [1] * 8, trmnl=False),
            ]
        )
        p = build_habit_card_payload(data, TODAY)
        self.assertEqual(p["count"], 1)
        self.assertEqual([h["name"] for h in p["habits"]], ["Workout"])

    def test_days_sliced_to_last_seven(self):
        # 8-day days array -> only the trailing 7 survive.
        data = _habits_data([_habit("Read", [0, 1, 1, 0, 1, 1, 1, 0])])
        p = build_habit_card_payload(data, TODAY)
        h = p["habits"][0]
        self.assertEqual(h["days"], [1, 1, 0, 1, 1, 1, 0])
        self.assertEqual(len(h["days"]), 7)
        self.assertEqual(h["done"], 5)

    def test_day_headers_are_trailing_week_initials(self):
        # Window Jun 29 (Mon) .. Jul 5 (Sun).
        p = build_habit_card_payload(_habits_data([_habit("Water", [1] * 8)]), TODAY)
        self.assertEqual(p["day_headers"], ["M", "T", "W", "T", "F", "S", "S"])
        self.assertEqual(p["range_label"], "Jun 29 – Jul 5")

    def test_streak_and_today_passthrough(self):
        data = _habits_data([_habit("Meditate", [1] * 8, streak=12, today_done=True)])
        h = build_habit_card_payload(data, TODAY)["habits"][0]
        self.assertEqual(h["streak"], 12)
        self.assertTrue(h["today_done"])

    def test_empty_when_none_flagged(self):
        data = _habits_data([_habit("Sleep", [1] * 8, trmnl=False)])
        p = build_habit_card_payload(data, TODAY)
        self.assertEqual(p["count"], 0)
        self.assertEqual(p["habits"], [])

    def test_missing_dates_degrade_gracefully(self):
        p = build_habit_card_payload({"habits": [_habit("X", [1, 0, 1])]}, TODAY)
        self.assertEqual(p["day_headers"], [])
        self.assertEqual(p["range_label"], "")
        self.assertEqual(p["today_date"], TODAY.isoformat())
        self.assertEqual(p["habits"][0]["days"], [1, 0, 1])


if __name__ == "__main__":
    unittest.main()
