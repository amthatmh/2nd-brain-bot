"""Tests for Google Health/Fitbit sleep sync."""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from second_brain.healthtrack.sleep import (
    _parse_google_stages,
    fetch_sleep_data,
    handle_sleep_backfill_job,
    handle_sleep_sync,
    parse_sleep_data_point,
)
from second_brain.healthtrack.dashboard import _sleep_score


def _ms(minutes: int) -> int:
    return minutes * 60 * 1000


FULL_SLEEP_POINT = {
    "startTime": "2026-05-28T23:00:00-05:00",
    "endTime": "2026-05-29T07:00:00-05:00",
    "sleepSummary": {"totalDurationMs": _ms(420)},
    "stagesSummary": {
        "deepDurationMs": _ms(90),
        "remDurationMs": _ms(100),
        "lightDurationMs": _ms(230),
        "awakeDurationMs": _ms(60),
    },
}


class TestSleepParsing(unittest.TestCase):
    def test_parse_sleep_data_point_with_full_stage_summary(self):
        parsed = parse_sleep_data_point(FULL_SLEEP_POINT, ZoneInfo("America/Chicago"))

        self.assertEqual(parsed["date_str"], "2026-05-29")
        self.assertEqual(parsed["bedtime_iso"], "2026-05-28T23:00:00-05:00")
        self.assertEqual(parsed["wake_time_iso"], "2026-05-29T07:00:00-05:00")
        self.assertEqual(parsed["total_sleep_min"], 420)
        self.assertEqual(parsed["deep_min"], 90)
        self.assertEqual(parsed["rem_min"], 100)
        self.assertEqual(parsed["light_min"], 230)
        self.assertEqual(parsed["awake_min"], 60)
        self.assertEqual(parsed["time_in_bed_min"], 480)
        self.assertEqual(parsed["sleep_efficiency"], 0.875)

    def test_parse_sleep_data_point_with_partial_list_stage_summary(self):
        point = {
            "startTime": "2026-05-28T23:30:00Z",
            "endTime": "2026-05-29T06:30:00Z",
            "sleepSummary": {"totalDurationMs": _ms(390)},
            "stagesSummary": [
                {"stage": "DEEP", "durationMs": _ms(80)},
                {"stage": "REM", "durationMs": _ms(95)},
            ],
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["deep_min"], 80)
        self.assertEqual(parsed["rem_min"], 95)
        self.assertEqual(parsed["light_min"], 0)
        self.assertEqual(parsed["awake_min"], 0)

    def test_parse_sleep_data_point_with_reconciled_summary_shape(self):
        point = {
            "startTime": "",
            "endTime": "",
            "sleepSummary": {
                "startTime": "2026-05-28T23:10:00-05:00",
                "endTime": "2026-05-29T07:00:00-05:00",
                "minutesAsleep": "421",
                "minutesInSleepPeriod": "470",
            },
            "stagesSummary": [
                {"type": "DEEP", "minutes": "114", "count": "10"},
                {"type": "REM", "minutes": "81", "count": "7"},
                {"type": "LIGHT", "minutes": "226", "count": "22"},
                {"type": "AWAKE", "minutes": "49", "count": "15"},
            ],
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("America/Chicago"))

        self.assertEqual(parsed["date_str"], "2026-05-29")
        self.assertEqual(parsed["total_sleep_min"], 421)
        self.assertEqual(parsed["deep_min"], 114)
        self.assertEqual(parsed["rem_min"], 81)
        self.assertEqual(parsed["light_min"], 226)
        self.assertEqual(parsed["awake_min"], 49)
        self.assertEqual(parsed["time_in_bed_min"], 470)
        self.assertEqual(parsed["sleep_efficiency"], 0.8957)

    def test_parse_sleep_data_point_uses_minutes_in_sleep_period_when_asleep_missing(self):
        point = {
            "startTime": "2026-05-28T23:00:00Z",
            "endTime": "2026-05-29T07:00:00Z",
            "sleepSummary": {"minutesInSleepPeriod": "480"},
            "stagesSummary": [],
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["total_sleep_min"], 480)
        self.assertEqual(parsed["sleep_efficiency"], 1.0)

    def test_parse_sleep_data_point_no_summary_falls_back_to_time_in_bed(self):
        point = {
            "startTime": "2026-05-28T23:00:00Z",
            "endTime": "2026-05-29T07:00:00Z",
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["time_in_bed_min"], 480)
        self.assertEqual(parsed["total_sleep_min"], parsed["time_in_bed_min"])
        self.assertEqual(parsed["sleep_efficiency"], 1.0)

    def test_parse_google_health_connect_stages(self):
        point = {
            "startTime": "2026-05-30T01:00:00Z",
            "endTime": "2026-05-30T08:00:00Z",
            "sleep": {
                "stages": [
                    {
                        "interval": {
                            "startTime": "2026-05-30T01:00:00Z",
                            "endTime": "2026-05-30T01:20:00Z",
                        },
                        "stage": 1,
                    },
                    {
                        "interval": {
                            "startTime": "2026-05-30T01:20:00Z",
                            "endTime": "2026-05-30T04:20:00Z",
                        },
                        "stage": 4,
                    },
                    {
                        "interval": {
                            "startTime": "2026-05-30T04:20:00Z",
                            "endTime": "2026-05-30T05:50:00Z",
                        },
                        "stage": 5,
                    },
                    {
                        "interval": {
                            "startTime": "2026-05-30T05:50:00Z",
                            "endTime": "2026-05-30T07:20:00Z",
                        },
                        "stage": 6,
                    },
                    {
                        "interval": {
                            "startTime": "2026-05-30T07:20:00Z",
                            "endTime": "2026-05-30T08:00:00Z",
                        },
                        "stage": 3,
                    },
                ],
            },
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["deep_min"], 90)
        self.assertEqual(parsed["rem_min"], 90)
        self.assertEqual(parsed["light_min"], 180)
        self.assertEqual(parsed["awake_min"], 20)
        self.assertEqual(parsed["total_sleep_min"], 360)
        self.assertEqual(parsed["sleep_efficiency"], 0.8571)

    def test_parse_google_stages_helper_returns_empty_for_unrecognised_format(self):
        self.assertEqual(_parse_google_stages([{"stage": "NAPPING", "minutes": 90}]), {})

    def test_parse_google_stages_string_types_with_timestamps(self):
        point = {
            "startTime": "2026-05-31T09:00:00Z",
            "endTime": "2026-05-31T10:53:00Z",
            "sleep": {
                "summary": {
                    "minutesAsleep": "105",
                    "minutesInSleepPeriod": "113",
                },
                "stages": [
                    {
                        "type": "DEEP",
                        "startTime": "2026-05-31T09:00:00Z",
                        "endTime": "2026-05-31T09:30:00Z",
                    },
                    {
                        "type": "REM",
                        "startTime": "2026-05-31T09:30:00Z",
                        "endTime": "2026-05-31T10:00:00Z",
                    },
                    {
                        "type": "LIGHT",
                        "startTime": "2026-05-31T10:00:00Z",
                        "endTime": "2026-05-31T10:45:00Z",
                    },
                    {
                        "type": "AWAKE",
                        "startTime": "2026-05-31T10:45:00Z",
                        "endTime": "2026-05-31T10:53:00Z",
                    },
                ],
            },
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["deep_min"], 30.0)
        self.assertEqual(parsed["rem_min"], 30.0)
        self.assertEqual(parsed["light_min"], 45.0)
        self.assertEqual(parsed["awake_min"], 8.0)
        self.assertEqual(parsed["total_sleep_min"], 105)
        self.assertEqual(parsed["sleep_efficiency"], 0.9292)

    def test_parse_google_stages_compact_string_format(self):
        point = {
            "startTime": "2026-05-31T01:00:00Z",
            "endTime": "2026-05-31T08:33:00Z",
            "stagesSummary": [
                {"type": "LIGHT", "minutes": "310"},
                {"type": "DEEP", "minutes": "54"},
                {"type": "REM", "minutes": "80"},
                {"type": "AWAKE", "minutes": "7"},
            ],
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["light_min"], 310.0)
        self.assertEqual(parsed["deep_min"], 54.0)
        self.assertEqual(parsed["rem_min"], 80.0)
        self.assertEqual(parsed["awake_min"], 7.0)
        self.assertEqual(parsed["total_sleep_min"], 444.0)
        self.assertEqual(parsed["sleep_efficiency"], 0.9801)

    def test_parse_sleep_data_point_with_null_stage_summary(self):
        point = {
            "startTime": "2026-05-28T23:00:00Z",
            "endTime": "2026-05-29T07:00:00Z",
            "sleepSummary": {"totalDurationMs": _ms(400)},
            "stagesSummary": None,
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["deep_min"], 0)
        self.assertEqual(parsed["rem_min"], 0)
        self.assertEqual(parsed["light_min"], 0)
        self.assertEqual(parsed["awake_min"], 0)

    def test_parse_sleep_data_point_with_nested_stage_duration_ms(self):
        point = {
            "startTime": "2026-05-28T23:00:00Z",
            "endTime": "2026-05-29T07:00:00Z",
            "sleepSummary": {"totalDurationMs": _ms(400)},
            "stagesSummary": {
                "stages": [
                    {"type": "DEEP", "durationMs": _ms(70)},
                    {"type": "REM", "durationMs": _ms(90)},
                ]
            },
        }

        parsed = parse_sleep_data_point(point, ZoneInfo("UTC"))

        self.assertEqual(parsed["deep_min"], 70)
        self.assertEqual(parsed["rem_min"], 90)

    def test_parse_sleep_data_point_wraps_missing_time_error(self):
        point = {
            "startTime": "",
            "endTime": "2026-05-29T07:00:00Z",
            "sleepSummary": {"totalDurationMs": _ms(400)},
        }

        with self.assertRaisesRegex(ValueError, "sleep_sync: unparseable time fields"):
            parse_sleep_data_point(point, ZoneInfo("UTC"))

    def test_parse_sleep_data_point_sleep_score(self):
        base_point = {
            "startTime": "2026-05-28T23:00:00Z",
            "endTime": "2026-05-29T07:00:00Z",
        }
        cases = [
            ({}, None),
            ({"sleepScore": "82"}, 82),
        ]

        for sleep_summary, expected_score in cases:
            with self.subTest(sleep_summary=sleep_summary):
                parsed = parse_sleep_data_point(
                    {**base_point, "sleepSummary": sleep_summary},
                    ZoneInfo("UTC"),
                )

                self.assertEqual(parsed["sleep_score"], expected_score)


class TestFetchSleepData(unittest.TestCase):
    @patch("second_brain.healthtrack.sleep.httpx.get")
    def test_fetch_sleep_data_normalizes_reconciled_point(self, mock_get):
        raw_point = {
            "startTime": "",
            "endTime": "",
            "sleepSummary": {
                "startTime": "2026-05-28T23:10:00-05:00",
                "endTime": "2026-05-29T07:00:00-05:00",
                "minutesAsleep": "421",
            },
            "stagesSummary": [{"type": "DEEP", "minutes": "114", "count": "10"}],
        }
        mock_response = MagicMock()
        mock_response.json.return_value = {"dataPoints": [raw_point]}
        mock_get.return_value = mock_response

        point = fetch_sleep_data("access-token", "2026-05-28", ZoneInfo("America/Chicago"))

        self.assertEqual(point["startTime"], "2026-05-28T23:10:00-05:00")
        self.assertEqual(point["endTime"], "2026-05-29T07:00:00-05:00")
        self.assertEqual(point["sleepSummary"], raw_point["sleepSummary"])
        self.assertEqual(point["stagesSummary"], raw_point["stagesSummary"])
        mock_response.raise_for_status.assert_called_once()

    @patch("second_brain.healthtrack.sleep.httpx.get")
    def test_fetch_sleep_data_selects_longest_session(self, mock_get):
        nap_point = {
            "startTime": "2026-05-28T15:00:00-05:00",
            "endTime": "2026-05-28T16:00:00-05:00",
            "sleepSummary": {"totalDurationMs": _ms(60)},
        }
        main_sleep_point = {
            "startTime": "2026-05-28T23:30:00-05:00",
            "endTime": "2026-05-29T07:00:00-05:00",
            "sleepSummary": {"totalDurationMs": _ms(450)},
        }
        mock_response = MagicMock()
        mock_response.json.return_value = {"dataPoints": [nap_point, main_sleep_point]}
        mock_get.return_value = mock_response

        point = fetch_sleep_data("access-token", "2026-05-28", ZoneInfo("America/Chicago"))

        self.assertEqual(point["startTime"], main_sleep_point["startTime"])
        self.assertEqual(point["endTime"], main_sleep_point["endTime"])
        self.assertEqual(point["sleepSummary"], main_sleep_point["sleepSummary"])


class TestSleepUpsert(unittest.IsolatedAsyncioTestCase):
    async def test_handle_sleep_sync_creates_new_row(self):
        notion = MagicMock()
        notion.databases.query.side_effect = [{"results": []}, {"results": []}]
        notion.pages.create.return_value = {"id": "new-page"}

        with patch("second_brain.healthtrack.sleep.refresh_access_token", return_value="access-token"), \
             patch("second_brain.healthtrack.sleep.fetch_sleep_data", return_value=FULL_SLEEP_POINT) as fetch_sleep:
            result = await handle_sleep_sync(
                notion=notion,
                metrics_db_id="metrics-db",
                client_id="client",
                client_secret="secret",
                refresh_token="refresh",
                target_date="2026-05-29",
                tz=ZoneInfo("America/Chicago"),
            )

        self.assertEqual(result["action"], "created")
        self.assertEqual(result["date"], "2026-05-29")
        fetch_sleep.assert_called_once_with("access-token", "2026-05-28", ZoneInfo("America/Chicago"))
        props = notion.pages.create.call_args.kwargs["properties"]
        self.assertEqual(props["Name"]["title"][0]["text"]["content"], "2026-05-29 Log")
        self.assertEqual(props["Date"]["date"]["start"], "2026-05-29")
        self.assertEqual(props["Bedtime"]["date"]["start"], "2026-05-28T23:00:00-05:00")
        self.assertEqual(props["Wake Time"]["date"]["start"], "2026-05-29T07:00:00-05:00")
        self.assertEqual(props["Total Sleep (min)"], {"number": 420})
        self.assertEqual(props["Sleep Efficiency (%)"], {"number": 0.875})

    async def test_handle_sleep_sync_updates_existing_row(self):
        notion = MagicMock()
        notion.databases.query.return_value = {"results": [{"id": "existing-page"}]}

        with patch("second_brain.healthtrack.sleep.refresh_access_token", return_value="access-token"), \
             patch("second_brain.healthtrack.sleep.fetch_sleep_data", return_value=FULL_SLEEP_POINT):
            result = await handle_sleep_sync(
                notion=notion,
                metrics_db_id="metrics-db",
                client_id="client",
                client_secret="secret",
                refresh_token="refresh",
                target_date="2026-05-29",
                tz=ZoneInfo("America/Chicago"),
            )

        self.assertEqual(result["action"], "updated")
        notion.pages.update.assert_called_once()
        self.assertEqual(notion.pages.update.call_args.kwargs["page_id"], "existing-page")
        props = notion.pages.update.call_args.kwargs["properties"]
        self.assertNotIn("Name", props)
        self.assertNotIn("Date", props)
        notion.pages.create.assert_not_called()


class TestSleepNoData(unittest.IsolatedAsyncioTestCase):
    async def test_handle_sleep_sync_returns_no_data(self):
        notion = MagicMock()

        with patch("second_brain.healthtrack.sleep.refresh_access_token", return_value="access-token"), \
             patch("second_brain.healthtrack.sleep.fetch_sleep_data", return_value=None):
            result = await handle_sleep_sync(
                notion=notion,
                metrics_db_id="metrics-db",
                client_id="client",
                client_secret="secret",
                refresh_token="refresh",
                target_date="2026-05-29",
                tz=ZoneInfo("America/Chicago"),
            )

        self.assertEqual(result, {"action": "no_data", "date": "2026-05-29", "page_id": None})
        notion.databases.query.assert_not_called()
        notion.pages.create.assert_not_called()
        notion.pages.update.assert_not_called()

    async def test_handle_sleep_backfill_logs_no_data_days(self):
        fake_config = SimpleNamespace(
            GOOGLE_HEALTH_CLIENT_ID="client",
            GOOGLE_HEALTH_CLIENT_SECRET="secret",
            GOOGLE_HEALTH_REFRESH_TOKEN="refresh",
            NOTION_HEALTH_METRICS_DB="metrics-db",
            TZ=ZoneInfo("UTC"),
        )
        fake_main = SimpleNamespace(notion=MagicMock())

        with patch.dict(
            "sys.modules",
            {"second_brain.config": fake_config, "second_brain.main": fake_main},
        ), patch(
            "second_brain.healthtrack.sleep.handle_sleep_sync",
            new=AsyncMock(return_value={"action": "no_data", "date": "2026-05-29", "page_id": None}),
        ), self.assertLogs("second_brain.healthtrack.sleep", level="WARNING") as logs:
            result = await handle_sleep_backfill_job(MagicMock(), "2026-05-29", "2026-05-29")

        self.assertTrue(result["ok"])
        self.assertIn("sleep_backfill: no data for wake date 2026-05-29", "\n".join(logs.output))


class TestSleepDashboard(unittest.TestCase):
    def test_sleep_score_uses_targets_instead_of_coming_soon(self):
        metrics = {
            "total_sleep": [{"date": "2026-05-28", "value": 390}, {"date": "2026-05-29", "value": 430}],
            "deep_sleep": [{"date": "2026-05-28", "value": 80}, {"date": "2026-05-29", "value": 95}],
            "sleep_efficiency": [{"date": "2026-05-28", "value": 82}, {"date": "2026-05-29", "value": 88}],
        }

        score = _sleep_score(metrics)

        self.assertEqual(score["value"], 100)
        self.assertNotEqual(score.get("status"), "coming_soon")
        self.assertIn("Total 430 min", score["description"])


if __name__ == "__main__":
    unittest.main()
