"""Tests for the TRMNL daily workout card logic."""

from __future__ import annotations

import unittest
from datetime import date

from second_brain.crossfit.trmnl import build_workout_card_payload, split_section

# Wednesday of week 2026-07-06 — matches the real Notion row this card renders.
TODAY = date(2026, 7, 8)

SECTION_B = (
    "Take 15 Minutes to Complete 5 Sets of:\n"
    "Power Clean + Split Jerk (2+2)\n"
    "Training notes:\n"
    "• Drop and reset after first power clean, re-rack the first jerk from overhead.\n"
    "• SLSC Standards (Clean and Jerk) Bronze: .8x BW, Silver: 1x BW, Gold: 1.3x BW, Platinum: 1.7x BW\n"
    "Midline Focused Chippers/Quartets — 43:00-55:00"
)

SECTION_C = (
    "12 Minute AMRAP\n"
    "100' Sandbag Carry (150/100)\n"
    "10 Double DB Front Squats (50s/35s)\n"
    "20 Russian KB Swings (70/53)\n"
    "50' Double DB Front Rack Walking Lunge (50s/35s)\n"
    "Training notes:\n"
    "• Each exercise should be doable unbroken (when fresh)\n"
    "• This will turn into a slugfest, posterior chain will be under attack throughout most of this.\n"
    "• Take methodical breaks early to not bonk early."
)


class TestSplitSection(unittest.TestCase):
    def test_splits_workout_from_notes(self):
        s = split_section(SECTION_C)
        self.assertEqual(s["lines"][0], "12 Minute AMRAP")
        self.assertEqual(len(s["lines"]), 5)
        self.assertEqual(len(s["notes"]), 3)
        self.assertTrue(s["notes"][0].startswith("Each exercise"))

    def test_strips_bullets_from_notes(self):
        s = split_section("Work\nTraining notes:\n• note one\n- note two")
        self.assertEqual(s["notes"], ["note one", "note two"])

    def test_drops_schedule_time_marker(self):
        s = split_section(SECTION_B)
        self.assertNotIn(
            "Midline Focused Chippers/Quartets — 43:00-55:00",
            s["lines"] + s["notes"],
        )
        self.assertEqual(len(s["lines"]), 2)
        self.assertEqual(len(s["notes"]), 2)

    def test_handles_empty(self):
        self.assertEqual(split_section(None), {"lines": [], "notes": []})
        self.assertEqual(split_section(""), {"lines": [], "notes": []})

    def test_no_notes_header_means_all_lines(self):
        s = split_section("5 Rounds\n10 Pull-Ups")
        self.assertEqual(s["lines"], ["5 Rounds", "10 Pull-Ups"])
        self.assertEqual(s["notes"], [])


class TestBuildWorkoutCardPayload(unittest.TestCase):
    def test_full_payload(self):
        p = build_workout_card_payload(TODAY, "Performance", SECTION_B, SECTION_C)
        self.assertTrue(p["found"])
        self.assertEqual(p["day_label"], "WEDNESDAY")
        self.assertEqual(p["date_label"], "Jul 8")
        self.assertEqual(p["track"], "Performance")
        self.assertEqual(p["section_b"]["lines"][1], "Power Clean + Split Jerk (2+2)")
        self.assertEqual(len(p["section_c"]["lines"]), 5)

    def test_missing_row_not_found(self):
        p = build_workout_card_payload(TODAY, None, "", "")
        self.assertFalse(p["found"])
        self.assertEqual(p["track"], "")

    def test_section_c_only_still_found(self):
        p = build_workout_card_payload(TODAY, "Hyrox", "", "For Time:\n1000m Row")
        self.assertTrue(p["found"])


if __name__ == "__main__":
    unittest.main()
