"""Tests for the daily RHR/HRV recovery flag."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from second_brain.healthtrack.recovery import (
    RECOVERY_MIN_BASELINE_DAYS,
    build_recovery_message,
    compute_recovery_flag,
    generate_recovery_alert,
)

TARGET = date(2026, 5, 15)


def _row(day: str, rhr: float | None = 58, hrv: float | None = 42) -> dict:
    props: dict = {"Date": {"date": {"start": day}}}
    if rhr is not None:
        props["Resting Heart Rate (bpm)"] = {"number": rhr}
    if hrv is not None:
        props["HRV (ms)"] = {"number": hrv}
    return {"id": f"row-{day}", "properties": props}


def _baseline(days: int, rhr: float = 58, hrv: float = 42) -> list[dict]:
    """Rows for the ``days`` calendar days immediately before TARGET."""
    from datetime import timedelta

    return [
        _row((TARGET - timedelta(days=offset)).isoformat(), rhr=rhr, hrv=hrv)
        for offset in range(1, days + 1)
    ]


class TestComputeRecoveryFlag(unittest.TestCase):
    def test_both_markers_trip(self):
        rows = _baseline(7, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=64, hrv=36)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertEqual(flag.severity, "both")
        self.assertTrue(flag.windows[7].rhr_spike)
        self.assertTrue(flag.windows[7].hrv_drop)

    def test_single_marker_trips(self):
        rows = _baseline(7, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=64, hrv=42)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertEqual(flag.severity, "single")
        self.assertTrue(flag.windows[7].rhr_spike)
        self.assertFalse(flag.windows[7].hrv_drop)

    def test_no_trip_when_within_baseline(self):
        rows = _baseline(7, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=58, hrv=42)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertEqual(flag.severity, "none")

    def test_missing_today_is_no_data(self):
        rows = _baseline(7) + [_row(TARGET.isoformat(), rhr=64, hrv=None)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertEqual(flag.severity, "no_data")

    def test_thin_baseline_is_insufficient(self):
        rows = _baseline(RECOVERY_MIN_BASELINE_DAYS - 1) + [_row(TARGET.isoformat(), rhr=90, hrv=10)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertEqual(flag.severity, "insufficient")

    def test_sigma_boundary_is_strict(self):
        # Baseline RHR {56,58,60}: mean 58, pstdev ~1.633. mean + 1σ ≈ 59.63.
        # Today 59 sits below the threshold, so no spike.
        from datetime import timedelta

        rows = [
            _row((TARGET - timedelta(days=1)).isoformat(), rhr=56, hrv=42),
            _row((TARGET - timedelta(days=2)).isoformat(), rhr=58, hrv=42),
            _row((TARGET - timedelta(days=3)).isoformat(), rhr=60, hrv=42),
            _row(TARGET.isoformat(), rhr=59, hrv=42),
        ]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertFalse(flag.windows[7].rhr_spike)


class TestBuildRecoveryMessage(unittest.TestCase):
    def test_both_message_has_action(self):
        rows = _baseline(14, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=64, hrv=36)]
        flag = compute_recovery_flag(rows, TARGET)
        msg = build_recovery_message(flag)
        self.assertIsNotNone(msg)
        self.assertIn("🔴", msg)
        self.assertIn("deload", msg)

    def test_single_message_is_soft(self):
        rows = _baseline(7, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=64, hrv=42)]
        flag = compute_recovery_flag(rows, TARGET)
        msg = build_recovery_message(flag)
        self.assertIsNotNone(msg)
        self.assertIn("🟡", msg)

    def test_none_message_is_silent(self):
        rows = _baseline(7) + [_row(TARGET.isoformat(), rhr=58, hrv=42)]
        flag = compute_recovery_flag(rows, TARGET)
        self.assertIsNone(build_recovery_message(flag))


class TestGenerateRecoveryAlert(unittest.IsolatedAsyncioTestCase):
    async def test_sends_on_both(self):
        rows = _baseline(7, rhr=58, hrv=42) + [_row(TARGET.isoformat(), rhr=64, hrv=36)]
        bot = MagicMock()
        bot.send_message = AsyncMock()
        with patch("second_brain.healthtrack.recovery.fetch_health_range", return_value=rows):
            result = await generate_recovery_alert(
                bot, notion=MagicMock(), metrics_db_id="db", chat_id=1, tz=None, today=TARGET
            )
        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["severity"], "both")
        bot.send_message.assert_awaited_once()

    async def test_quiet_when_no_trip(self):
        rows = _baseline(7) + [_row(TARGET.isoformat(), rhr=58, hrv=42)]
        bot = MagicMock()
        bot.send_message = AsyncMock()
        with patch("second_brain.healthtrack.recovery.fetch_health_range", return_value=rows):
            result = await generate_recovery_alert(
                bot, notion=MagicMock(), metrics_db_id="db", chat_id=1, tz=None, today=TARGET
            )
        self.assertEqual(result["status"], "quiet")
        bot.send_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
