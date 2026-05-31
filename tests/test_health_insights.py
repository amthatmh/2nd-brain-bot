"""Tests for weekly health insights."""

from __future__ import annotations

import unittest
from datetime import date, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.insights import (
    build_health_insight_prompt,
    compute_week_stats,
    generate_weekly_health_insight,
    get_travel_context,
)


def _number(value):
    return {"number": value}


def _date_prop(value: str):
    return {"date": {"start": value}}


def _rich_text(value: str):
    return {"rich_text": [{"plain_text": value}]}


def _multi_select(values: list[str]):
    return {"multi_select": [{"name": value} for value in values]}


def _health_row(day: str, **overrides):
    props = {
        "Date": _date_prop(day),
        "Total Sleep (min)": _number(420),
        "Deep Sleep (min)": _number(84),
        "REM Sleep (min)": _number(105),
        "Awake in Bed (min)": _number(35),
        "Sleep Efficiency (%)": _number(90),
        "Bedtime": _date_prop(f"{day}T23:00:00-05:00"),
        "HRV (ms)": _number(42),
        "Resting Heart Rate (bpm)": _number(58),
        "VO2 Max": _number(41.5),
        "Active Energy (kcal)": _number(500),
        "Exercise Time (min)": _number(30),
        "Weight (kg)": _number(76),
    }
    props.update(overrides)
    return {"id": f"row-{day}", "properties": props}


class TestHealthInsightAggregation(unittest.TestCase):
    def test_compute_week_stats_with_full_data(self):
        rows = [
            _health_row("2026-05-01", **{"Exercise Time (min)": _number(30), "Weight (kg)": _number(76.0)}),
            _health_row("2026-05-02", **{"Exercise Time (min)": _number(25), "Weight (kg)": _number(76.1)}),
            _health_row("2026-05-03", **{"Exercise Time (min)": _number(0), "Weight (kg)": _number(76.2)}),
            _health_row("2026-05-04", **{"Exercise Time (min)": _number(35), "Weight (kg)": _number(76.3)}),
            _health_row("2026-05-05", **{"Exercise Time (min)": _number(30), "Weight (kg)": _number(76.4)}),
            _health_row("2026-05-06", **{"Exercise Time (min)": _number(0), "Weight (kg)": _number(76.5)}),
            _health_row("2026-05-07", **{"Exercise Time (min)": _number(40), "Weight (kg)": _number(76.6)}),
        ]

        stats = compute_week_stats(rows)

        self.assertEqual(stats.days_with_data, 7)
        self.assertEqual(stats.avg_sleep_min, 420)
        self.assertEqual(stats.avg_deep_min, 84)
        self.assertEqual(stats.avg_rem_min, 105)
        self.assertEqual(stats.avg_awake_min, 35)
        self.assertEqual(stats.avg_sleep_efficiency, 90)
        self.assertAlmostEqual(stats.avg_deep_pct, 20)
        self.assertAlmostEqual(stats.avg_rem_pct, 25)
        self.assertEqual(stats.avg_hrv, 42)
        self.assertEqual(stats.avg_rhr, 58)
        self.assertEqual(stats.exercise_days, 5)
        self.assertEqual(stats.latest_weight, 76.6)

    def test_compute_week_stats_with_sparse_null_data(self):
        rows = [
            _health_row("2026-05-01", **{"HRV (ms)": _number(None), "Exercise Time (min)": _number(0)}),
            _health_row("2026-05-02", **{"HRV (ms)": _number(40), "Total Sleep (min)": _number(None)}),
            _health_row("2026-05-03", **{"HRV (ms)": _number(50), "Exercise Time (min)": _number(None)}),
        ]

        stats = compute_week_stats(rows)

        self.assertEqual(stats.days_with_data, 3)
        self.assertEqual(stats.avg_hrv, 45)
        self.assertEqual(stats.exercise_days, 1)
        self.assertEqual(stats.avg_sleep_min, 420)

    def test_compute_week_stats_with_empty_list(self):
        stats = compute_week_stats([])

        self.assertEqual(stats.days_with_data, 0)
        self.assertIsNone(stats.avg_sleep_min)
        self.assertIsNone(stats.avg_deep_min)
        self.assertIsNone(stats.avg_rem_min)
        self.assertIsNone(stats.avg_awake_min)
        self.assertIsNone(stats.avg_hrv)
        self.assertIsNone(stats.last_vo2)
        self.assertEqual(stats.exercise_days, 0)
        self.assertEqual(stats.daily_readiness, [])
        self.assertEqual(stats.daily_exercise_min, [])

    def test_compute_week_stats_includes_daily_trends(self):
        rows = [
            _health_row(
                "2026-05-03",
                **{"Readiness Score": _number(80), "Exercise Time (min)": _number(0)},
            ),
            _health_row(
                "2026-05-01",
                **{"Readiness Score": _number(70), "Exercise Time (min)": _number(45)},
            ),
            _health_row(
                "2026-05-02",
                **{"Readiness Score": _number(None), "Exercise Time (min)": _number(20)},
            ),
            _health_row(
                "2026-05-04",
                **{"Readiness Score": _number(0), "Exercise Time (min)": _number(None)},
            ),
        ]

        stats = compute_week_stats(rows)

        self.assertEqual(stats.daily_readiness, [("2026-05-01", 70), ("2026-05-03", 80)])
        self.assertEqual(stats.daily_exercise_min, [("2026-05-01", 45), ("2026-05-02", 20)])


class TestTravelContext(unittest.TestCase):
    def test_get_travel_context_with_overlapping_trip(self):
        notion = MagicMock()
        notion.databases.query.return_value = {
            "results": [
                {
                    "properties": {
                        "Departure Date": _date_prop("2026-05-02"),
                        "Return Date": _date_prop("2026-05-06"),
                        "Destination(s)": _rich_text("Nashville, TN"),
                        "Purpose": _multi_select(["Work"]),
                    }
                }
            ]
        }

        context = get_travel_context(notion, "trips-db", date(2026, 5, 1), date(2026, 5, 7))

        self.assertEqual(context["destinations"], "Nashville, TN")
        self.assertEqual(context["purpose"], "Work")
        self.assertEqual(context["dep_date"], "2026-05-02")
        self.assertEqual(context["ret_date"], "2026-05-06")

    def test_get_travel_context_with_no_trips(self):
        notion = MagicMock()
        notion.databases.query.return_value = {"results": []}

        context = get_travel_context(notion, "trips-db", date(2026, 5, 1), date(2026, 5, 7))

        self.assertIsNone(context)


class TestHealthInsightPrompt(unittest.TestCase):
    def test_build_health_insight_prompt_without_travel(self):
        stats = compute_week_stats([_health_row("2026-05-01")])

        prompt = build_health_insight_prompt(
            stats,
            stats,
            stats,
            "May 1-May 7",
            None,
            "2026-05-08",
            best_night_str="May 1 (7.0h)",
            worst_night_str="May 1 (7.0h, 35 min awake)",
        )

        self.assertNotIn("TRAVEL CONTEXT", prompt)
        self.assertIn("WEEKLY DATA", prompt)
        self.assertIn("REM 105 min", prompt)
        self.assertIn("Sleep nights: best May 1 (7.0h), worst May 1 (7.0h, 35 min awake)", prompt)
        self.assertIn("Write exactly 7 sections", prompt)

    def test_build_health_insight_prompt_with_travel(self):
        stats = compute_week_stats([_health_row("2026-05-01")])

        prompt = build_health_insight_prompt(
            stats,
            stats,
            stats,
            "May 1-May 7",
            {
                "destinations": "Nashville, TN",
                "purpose": "Work",
                "dep_date": "2026-05-02",
                "ret_date": "2026-05-06",
            },
            "2026-05-08",
        )

        self.assertIn("TRAVEL CONTEXT", prompt)
        self.assertIn("Nashville, TN trip", prompt)


class TestHealthInsightOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_insufficient_data_guard_sends_warning_without_claude(self):
        bot = MagicMock()
        bot.send_message = AsyncMock()
        rows = [
            _health_row("2026-05-22"),
            _health_row("2026-05-23"),
        ]

        with patch("second_brain.healthtrack.insights.fetch_health_range", return_value=rows), \
             patch("second_brain.healthtrack.insights.call_claude_for_insight") as claude_call, \
             patch("second_brain.healthtrack.insights.update_health_profile") as update_profile:
            result = await generate_weekly_health_insight(
                bot,
                notion=MagicMock(),
                metrics_db_id="metrics-db",
                trips_db_id="trips-db",
                chat_id=123,
                tz=timezone.utc,
                claude_model="claude-test",
                today=date(2026, 5, 30),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "insufficient_data")
        claude_call.assert_not_called()
        update_profile.assert_not_called()
        bot.send_message.assert_awaited_once()
        self.assertIn("Insufficient health data", bot.send_message.await_args.kwargs["text"])


if __name__ == "__main__":
    unittest.main()
