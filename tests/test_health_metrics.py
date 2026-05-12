"""Tests for Health Auto Export daily metrics sync."""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from second_brain.healthtrack.metrics import (
    MalformedHealthMetricsPayload,
    handle_health_metrics_sync,
    parse_health_metrics_payload,
)
from second_brain.healthtrack.routes import register_health_routes

PAYLOAD = {
    "data": [
        {
            "name": "Weight",
            "units": "kg",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 76.86}],
        },
        {
            "name": "Heart Rate Variability",
            "units": "ms",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 39.0}],
        },
    ]
}


SNAKE_CASE_PAYLOAD = {
    "data": {
        "metrics": [
            {
                "name": "weight_body_mass",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 76.86}],
            },
            {
                "name": "body_fat_percentage",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 18.2}],
            },
            {
                "name": "lean_body_mass",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 140.5}],
            },
            {
                "name": "resting_heart_rate",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 58}],
            },
            {
                "name": "heart_rate_variability",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 39.0}],
            },
            {
                "name": "vo2_max",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 42.1}],
            },
            {
                "name": "respiratory_rate",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 14.5}],
            },
            {
                "name": "apple_exercise_time",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 32}],
            },
            {
                "name": "active_energy",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 512.3}],
            },
            {
                "name": "basal_energy_burned",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 1680.4}],
            },
            {
                "name": "flights_climbed",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 8}],
            },
            {
                "name": "headphone_audio_exposure",
                "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 71.2}],
            },
        ]
    }
}


HUMAN_READABLE_STANDARD_PAYLOAD = {
    "data": [
        {
            "name": "Weight",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 76.86}],
        },
        {
            "name": "Body Fat Percentage",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 18.2}],
        },
        {
            "name": "Lean Body Mass",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 63.7}],
        },
        {
            "name": "Resting Heart Rate",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 58}],
        },
        {
            "name": "Heart Rate Variability",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 39.0}],
        },
        {
            "name": "VO2 Max",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 42.1}],
        },
        {
            "name": "Respiratory Rate",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 14.5}],
        },
        {
            "name": "Apple Exercise Time",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 32}],
        },
        {
            "name": "Active Energy",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 512.3}],
        },
        {
            "name": "Resting Energy",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 1680.4}],
        },
        {
            "name": "Flights Climbed",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 8}],
        },
        {
            "name": "Headphone Audio Exposure",
            "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 71.2}],
        },
    ]
}


class TestHealthMetricsParsing(unittest.TestCase):
    def test_parse_accepts_v2_metrics_wrapper(self):
        payload = {"data": {"metrics": PAYLOAD["data"]}}

        date_str, values, skipped = parse_health_metrics_payload(
            payload, ZoneInfo("UTC")
        )

        self.assertEqual(date_str, "2026-05-09")
        self.assertEqual(values["Weight (kg)"], 76.86)
        self.assertEqual(values["HRV (ms)"], 39.0)
        self.assertEqual(skipped, [])

    def test_parse_rejects_empty_v2_metrics_wrapper(self):
        with self.assertRaisesRegex(
            MalformedHealthMetricsPayload, "data array is empty"
        ):
            parse_health_metrics_payload({"data": {}}, ZoneInfo("UTC"))

    def test_parse_rejects_empty_v1_metrics_array(self):
        with self.assertRaisesRegex(
            MalformedHealthMetricsPayload, "data array is empty"
        ):
            parse_health_metrics_payload({"data": []}, ZoneInfo("UTC"))

    def test_parse_rejects_missing_data_key(self):
        with self.assertRaisesRegex(
            MalformedHealthMetricsPayload, "top-level data array"
        ):
            parse_health_metrics_payload({"metrics": []}, ZoneInfo("UTC"))

    def test_parse_maps_all_standard_snake_case_metrics(self):
        with patch("second_brain.healthtrack.metrics.log.warning") as warning_log:
            date_str, values, skipped = parse_health_metrics_payload(
                SNAKE_CASE_PAYLOAD, ZoneInfo("UTC")
            )

        self.assertEqual(date_str, "2026-05-09")
        self.assertEqual(skipped, [])
        warning_log.assert_not_called()
        self.assertEqual(
            values,
            {
                "Weight (kg)": 76.86,
                "Body Fat %": 18.2,
                "Lean Body Mass (kg)": 140.5,
                "Resting Heart Rate (bpm)": 58.0,
                "HRV (ms)": 39.0,
                "VO2 Max": 42.1,
                "Respiratory Rate (brpm)": 14.5,
                "Exercise Time (min)": 32.0,
                "Active Energy (kcal)": 512.3,
                "Resting Energy (kcal)": 1680.4,
                "Flights Climbed": 8.0,
                "Headphone Audio Exposure (dB)": 71.2,
            },
        )

    def test_parse_maps_all_standard_human_readable_metrics(self):
        date_str, values, skipped = parse_health_metrics_payload(
            HUMAN_READABLE_STANDARD_PAYLOAD, ZoneInfo("UTC")
        )

        self.assertEqual(date_str, "2026-05-09")
        self.assertEqual(skipped, [])
        self.assertEqual(values["Weight (kg)"], 76.86)
        self.assertEqual(values["Lean Body Mass (kg)"], 63.7)
        self.assertEqual(values["Resting Energy (kcal)"], 1680.4)
        self.assertEqual(values["Headphone Audio Exposure (dB)"], 71.2)
        self.assertEqual(len(values), 12)

    def test_parse_maps_known_metrics_and_skips_unknown(self):
        payload = {
            "data": [
                *PAYLOAD["data"],
                {
                    "name": "Unknown Future Metric",
                    "data": [{"date": "2026-05-09 21:00:00 +0000", "qty": 1}],
                },
            ]
        }

        date_str, values, skipped = parse_health_metrics_payload(
            payload, ZoneInfo("UTC")
        )

        self.assertEqual(date_str, "2026-05-09")
        self.assertEqual(values["Weight (kg)"], 76.86)
        self.assertEqual(values["HRV (ms)"], 39.0)
        self.assertEqual(skipped, ["Unknown Future Metric"])


class TestHealthMetricsUpsert(unittest.IsolatedAsyncioTestCase):
    async def test_create_new_row_when_no_existing_page(self):
        notion = MagicMock()
        notion.databases.query.side_effect = [{"results": []}, {"results": []}]
        notion.pages.create.return_value = {"id": "new-page"}

        result = await handle_health_metrics_sync(
            body=PAYLOAD,
            notion=notion,
            metrics_db_id="metrics-db",
            tz=ZoneInfo("UTC"),
        )

        self.assertEqual(result["action"], "created")
        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Name"]["title"][0]["text"]["content"], "2026-05-09 Log")
        self.assertEqual(props["Date"]["date"]["start"], "2026-05-09")
        self.assertEqual(props["Weight (kg)"], {"number": 76.86})
        self.assertEqual(props["HRV (ms)"], {"number": 39.0})

    async def test_update_by_date_fallback_when_name_misses(self):
        notion = MagicMock()
        notion.databases.query.side_effect = [
            {"results": []},
            {"results": [{"id": "date-page"}]},
        ]

        result = await handle_health_metrics_sync(
            body=PAYLOAD,
            notion=notion,
            metrics_db_id="metrics-db",
            tz=ZoneInfo("UTC"),
        )

        self.assertEqual(result["action"], "updated_by_date")
        notion.pages.update.assert_called_once()
        self.assertEqual(notion.pages.update.call_args.kwargs["page_id"], "date-page")
        self.assertNotIn("Name", notion.pages.update.call_args.kwargs["properties"])
        self.assertNotIn("Date", notion.pages.update.call_args.kwargs["properties"])


class TestHealthMetricsRoute(unittest.IsolatedAsyncioTestCase):
    async def test_health_sync_malformed_payload_returns_400(self):
        app = web.Application()
        register_health_routes(
            app,
            notion=MagicMock(),
            habit_db_id="h",
            log_db_id="l",
            env_db_id="",
            tz=ZoneInfo("UTC"),
            bot_getter=lambda: None,
            chat_id=123,
            health_metrics_db_id="metrics-db",
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            with patch("second_brain.healthtrack.routes.WEBHOOK_SECRET", "secret"):
                response = await client.post(
                    "/api/v1/health-sync",
                    headers={"X-Health-Secret": "secret"},
                    json={"metrics": []},
                )
                body = await response.text()
        finally:
            await client.close()

        self.assertEqual(response.status, 400, body)
        self.assertFalse(json.loads(body)["ok"])


if __name__ == "__main__":
    unittest.main()
