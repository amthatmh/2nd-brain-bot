"""Tests for Health Auto Export daily metrics sync."""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from second_brain.healthtrack.metrics import (
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


class TestHealthMetricsParsing(unittest.TestCase):
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

        date_str, values, skipped = parse_health_metrics_payload(payload, ZoneInfo("UTC"))

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
