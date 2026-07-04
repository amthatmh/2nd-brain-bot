"""Tests for the TRMNL health coaching card logic."""

from __future__ import annotations

import unittest
from datetime import date

from types import SimpleNamespace

from second_brain.healthtrack.trmnl import (
    StepSummary,
    Verdict,
    build_card_payload,
    compute_verdict,
    summarize_steps,
)

TODAY = date(2026, 7, 3)  # a Friday


def _flag(severity="none", today_hrv=60.0, today_rhr=54.0, mean_hrv=65.0, mean_rhr=53.0):
    return SimpleNamespace(
        severity=severity,
        today_hrv=today_hrv,
        today_rhr=today_rhr,
        windows={7: SimpleNamespace(mean_hrv=mean_hrv, mean_rhr=mean_rhr)},
    )


class TestComputeVerdict(unittest.TestCase):
    def test_both_markers_force_rest(self):
        # Body veto: even with a big activity gap, red recovery => REST.
        v = compute_verdict("both", workout_gap=3, steps_gap=7)
        self.assertEqual(v.level, "rest")
        self.assertEqual(v.word, "REST")
        self.assertLessEqual(len(v.line.split()), 10)

    def test_single_marker_is_steady(self):
        v = compute_verdict("single", workout_gap=2, steps_gap=0)
        self.assertEqual(v.level, "steady")
        self.assertLessEqual(len(v.line.split()), 10)

    def test_recovered_and_behind_is_push(self):
        v = compute_verdict("none", workout_gap=2, steps_gap=1)
        self.assertEqual(v.level, "push")
        self.assertEqual(v.arrow, "↑")
        self.assertIn("2 workouts", v.line)
        self.assertLessEqual(len(v.line.split()), 10)

    def test_recovered_single_workout_gap_is_singular(self):
        v = compute_verdict("none", workout_gap=1, steps_gap=0)
        self.assertEqual(v.level, "push")
        self.assertIn("1 workout ", v.line)

    def test_recovered_and_on_track_is_steady_maintain(self):
        v = compute_verdict("none", workout_gap=0, steps_gap=0)
        self.assertEqual(v.level, "steady")
        self.assertIn("track", v.line.lower())

    def test_recovered_steps_gap_only_still_push(self):
        v = compute_verdict("none", workout_gap=0, steps_gap=3)
        self.assertEqual(v.level, "push")

    def test_no_data_defaults_to_steady(self):
        for severity in ("no_data", "insufficient"):
            v = compute_verdict(severity, workout_gap=3, steps_gap=7)
            self.assertEqual(v.level, "steady")
            self.assertLessEqual(len(v.line.split()), 10)


class TestSummarizeSteps(unittest.TestCase):
    def _rows(self, mapping: dict[str, int]) -> list[dict]:
        return [{"date": d, "count": c} for d, c in mapping.items()]

    def test_today_avg_and_week_total(self):
        # Mon 2026-06-29 .. Fri 2026-07-03 is the current ISO week.
        rows = self._rows(
            {
                "2026-06-29": 8000,  # Mon
                "2026-06-30": 6000,  # Tue
                "2026-07-01": 10000,  # Wed
                "2026-07-02": 4000,  # Thu
                "2026-07-03": 1693,  # Fri (today)
            }
        )
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.today, 1693)
        self.assertEqual(s.week_total, 8000 + 6000 + 10000 + 4000 + 1693)
        self.assertEqual(s.avg7, round((8000 + 6000 + 10000 + 4000 + 1693) / 5))

    def test_avg7_excludes_missing_days_not_zero(self):
        # Only two days present in the 7-day window -> average of those two.
        rows = self._rows({"2026-07-02": 5000, "2026-07-03": 3000})
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.avg7, 4000)

    def test_avg7_ignores_days_outside_window(self):
        # 2026-06-25 is 8 days before today -> outside the 7-day window.
        rows = self._rows({"2026-06-25": 20000, "2026-07-03": 2000})
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.avg7, 2000)
        self.assertEqual(s.today, 2000)

    def test_week_total_excludes_prior_week(self):
        # Sun 2026-06-28 belongs to the previous ISO week.
        rows = self._rows({"2026-06-28": 12000, "2026-06-29": 5000, "2026-07-03": 1000})
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.week_total, 5000 + 1000)

    def test_missing_today_is_zero(self):
        rows = self._rows({"2026-07-02": 5000})
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.today, 0)

    def test_empty_rows(self):
        s = summarize_steps([], TODAY)
        self.assertEqual(s, StepSummary(today=0, avg7=0, week_total=0))

    def test_malformed_rows_skipped(self):
        rows = [
            {"date": None, "count": 5000},
            {"date": "not-a-date", "count": 5000},
            {"date": "2026-07-03", "count": None},
            {"date": "2026-07-03", "count": 1500},
        ]
        s = summarize_steps(rows, TODAY)
        self.assertEqual(s.today, 1500)


class TestBuildCardPayload(unittest.TestCase):
    def _dashboard(self, workout_days=2, steps_days=5, prev_workout=1):
        return {
            "generated_at": "2026-07-03T12:00:00Z",
            "scores": {"activity": {"value": 68, "description": "2/7 workout days · 5/7 steps days", "recommendation": "Next move: add 1 workout day."}},
            "weekly_activity": [
                {"week": "2026-06-22", "workout_days": prev_workout, "steps_days": 4},
                {"week": "2026-06-29", "workout_days": workout_days, "steps_days": steps_days},
            ],
            "metrics": {"total_sleep": [{"date": f"2026-06-{d}", "value": 6.8} for d in range(27, 31)]},
        }

    def test_verdict_push_when_recovered_and_behind(self):
        p = build_card_payload(self._dashboard(workout_days=2, steps_days=5), _flag("none"), StepSummary(1693, 7321, 51000), TODAY)
        self.assertEqual(p["verdict"]["level"], "push")
        self.assertEqual(p["workouts"], {"done": 2, "target": 3, "trend": "↑"})
        self.assertEqual(p["steps"]["avg7"], 7321)
        self.assertEqual(p["activity"]["score"], 68)
        self.assertEqual(p["day_label"], "Fri · Day 5/7")

    def test_verdict_rest_overrides_activity_gap(self):
        p = build_card_payload(self._dashboard(workout_days=0, steps_days=0), _flag("both"), StepSummary(0, 0, 0), TODAY)
        self.assertEqual(p["verdict"]["level"], "rest")

    def test_hrv_rhr_trend_from_flag_baseline(self):
        # today_hrv 60 vs mean 65 -> down; today_rhr 54 vs mean 53 -> up.
        p = build_card_payload(self._dashboard(), _flag("none", today_hrv=60, today_rhr=54, mean_hrv=65, mean_rhr=53), StepSummary(1, 1, 1), TODAY)
        self.assertEqual(p["hrv"]["arrow"], "↓")
        self.assertEqual(p["rhr"]["arrow"], "↑")
        self.assertEqual(p["hrv"]["value"], 60)

    def test_missing_metrics_degrade_gracefully(self):
        p = build_card_payload({"scores": {}, "weekly_activity": [], "metrics": {}}, _flag("no_data", today_hrv=None, today_rhr=None), StepSummary(0, 0, 0), TODAY)
        self.assertEqual(p["verdict"]["level"], "steady")
        self.assertIsNone(p["activity"]["score"])
        self.assertIsNone(p["hrv"]["value"])
        self.assertIsNone(p["sleep"]["avg7_hours"])


if __name__ == "__main__":
    unittest.main()
