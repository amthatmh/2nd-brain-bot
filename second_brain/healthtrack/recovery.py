"""Daily morning RHR/HRV recovery flag from the Health Metrics Log.

Reads today's Resting Heart Rate + HRV, compares them against rolling 7-day and
14-day baselines (mean ± sigma), and sends a Telegram message only when the
signal warrants it: a strong alert when a RHR spike and HRV drop coincide on
the 7-day window (the strongest recovery signal), a soft FYI when only one
marker trips, and silence otherwise. Scheduled via the ``recovery_alert``
Utility Scheduler job registered in :mod:`second_brain.healthtrack.scheduler`.

The 7-day window drives the alert; the 14-day window is shown alongside as
agreement/context. Number parsing reuses the Health Metrics helpers in
:mod:`second_brain.healthtrack.insights`.
"""

from __future__ import annotations

import logging
import os
import statistics
from dataclasses import dataclass
from datetime import date, timedelta

from second_brain.healthtrack.insights import (
    _num,
    _row_date,
    _send_health_message,
    fetch_health_range,
)
from second_brain.monitoring import track_job_execution
from second_brain.utils import local_today

log = logging.getLogger(__name__)

# How many standard deviations from the baseline counts as a spike/crash.
RECOVERY_SIGMA: float = float(os.environ.get("RECOVERY_SIGMA", "1.0"))
# Minimum number of present baseline days per metric before a window is usable.
RECOVERY_MIN_BASELINE_DAYS: int = 3

_WINDOWS: tuple[int, ...] = (7, 14)


@dataclass
class WindowStats:
    """Baseline statistics for one metric window relative to today."""

    days: int
    n_rhr: int
    n_hrv: int
    mean_rhr: float | None
    std_rhr: float | None
    mean_hrv: float | None
    std_hrv: float | None
    rhr_spike: bool
    hrv_drop: bool

    @property
    def usable(self) -> bool:
        return self.n_rhr >= RECOVERY_MIN_BASELINE_DAYS and self.n_hrv >= RECOVERY_MIN_BASELINE_DAYS


@dataclass
class RecoveryFlag:
    """Result of comparing today's RHR/HRV against rolling baselines."""

    date: str
    today_rhr: float | None
    today_hrv: float | None
    windows: dict[int, WindowStats]
    # "both" | "single" | "none" | "no_data" | "insufficient"
    severity: str


def _metric_by_date(rows: list[dict]) -> dict[date, tuple[float | None, float | None]]:
    """Map each dated row to its (RHR, HRV) pair."""
    by_date: dict[date, tuple[float | None, float | None]] = {}
    for row in rows:
        row_day = _row_date(row)
        if row_day is None:
            continue
        by_date[row_day] = (
            _num(row, "Resting Heart Rate (bpm)"),
            _num(row, "HRV (ms)"),
        )
    return by_date


def _window_stats(
    by_date: dict[date, tuple[float | None, float | None]],
    target: date,
    today_rhr: float | None,
    today_hrv: float | None,
    days: int,
) -> WindowStats:
    """Compute baseline stats over the ``days`` before ``target`` and flag today."""
    start = target - timedelta(days=days)
    rhr_values: list[float] = []
    hrv_values: list[float] = []
    for offset in range(1, days + 1):
        day = target - timedelta(days=offset)
        if day < start:
            break
        pair = by_date.get(day)
        if pair is None:
            continue
        rhr, hrv = pair
        if rhr is not None:
            rhr_values.append(rhr)
        if hrv is not None:
            hrv_values.append(hrv)

    mean_rhr = statistics.fmean(rhr_values) if rhr_values else None
    std_rhr = statistics.pstdev(rhr_values) if len(rhr_values) >= 2 else (0.0 if rhr_values else None)
    mean_hrv = statistics.fmean(hrv_values) if hrv_values else None
    std_hrv = statistics.pstdev(hrv_values) if len(hrv_values) >= 2 else (0.0 if hrv_values else None)

    rhr_spike = (
        today_rhr is not None
        and mean_rhr is not None
        and std_rhr is not None
        and today_rhr > mean_rhr + RECOVERY_SIGMA * std_rhr
    )
    hrv_drop = (
        today_hrv is not None
        and mean_hrv is not None
        and std_hrv is not None
        and today_hrv < mean_hrv - RECOVERY_SIGMA * std_hrv
    )

    return WindowStats(
        days=days,
        n_rhr=len(rhr_values),
        n_hrv=len(hrv_values),
        mean_rhr=mean_rhr,
        std_rhr=std_rhr,
        mean_hrv=mean_hrv,
        std_hrv=std_hrv,
        rhr_spike=bool(rhr_spike),
        hrv_drop=bool(hrv_drop),
    )


def compute_recovery_flag(rows: list[dict], target: date) -> RecoveryFlag:
    """Compare today's RHR/HRV against rolling baselines and classify severity.

    Severity is driven by the 7-day window:
      * ``no_data``      — today's RHR or HRV is missing.
      * ``insufficient`` — fewer than ``RECOVERY_MIN_BASELINE_DAYS`` baseline days.
      * ``both``         — RHR spike and HRV drop both trip.
      * ``single``       — exactly one of them trips.
      * ``none``         — neither trips.
    """
    by_date = _metric_by_date(rows)
    today_rhr, today_hrv = by_date.get(target, (None, None))

    windows = {
        days: _window_stats(by_date, target, today_rhr, today_hrv, days)
        for days in _WINDOWS
    }

    if today_rhr is None or today_hrv is None:
        severity = "no_data"
    elif not windows[7].usable:
        severity = "insufficient"
    else:
        tripped = int(windows[7].rhr_spike) + int(windows[7].hrv_drop)
        severity = {2: "both", 1: "single", 0: "none"}[tripped]

    return RecoveryFlag(
        date=target.isoformat(),
        today_rhr=today_rhr,
        today_hrv=today_hrv,
        windows=windows,
        severity=severity,
    )


def _agreement_text(flag: RecoveryFlag) -> str:
    """Describe whether the 14-day window agrees with the 7-day flag."""
    w14 = flag.windows[14]
    if not w14.usable:
        return "14-day baseline: not enough history yet."
    both = w14.rhr_spike and w14.hrv_drop
    if both:
        return "14-day baseline agrees — both markers off vs the fortnight too."
    if w14.rhr_spike or w14.hrv_drop:
        return "14-day baseline partly agrees (one marker off)."
    return "14-day baseline still looks normal — may be an acute blip."


def build_recovery_message(flag: RecoveryFlag) -> str | None:
    """Render the Telegram message for a flag, or None when nothing to send."""
    w7 = flag.windows[7]
    if flag.severity == "both":
        return (
            f"🔴 *Recovery flag* — {flag.date}\n"
            f"RHR *{flag.today_rhr:.0f} bpm* ↑ (7d avg {w7.mean_rhr:.0f} ±{w7.std_rhr:.0f})\n"
            f"HRV *{flag.today_hrv:.0f} ms* ↓ (7d avg {w7.mean_hrv:.0f} ±{w7.std_hrv:.0f})\n"
            f"{_agreement_text(flag)}\n\n"
            "Both markers tripped together — the strongest recovery signal. "
            "Consider a deload or easy day."
        )
    if flag.severity == "single":
        if w7.rhr_spike:
            line = f"RHR *{flag.today_rhr:.0f} bpm* ↑ (7d avg {w7.mean_rhr:.0f} ±{w7.std_rhr:.0f})"
        else:
            line = f"HRV *{flag.today_hrv:.0f} ms* ↓ (7d avg {w7.mean_hrv:.0f} ±{w7.std_hrv:.0f})"
        return (
            f"🟡 *Recovery note* — {flag.date}\n"
            f"{line}\n"
            "Only one marker moved; likely noise, but worth watching."
        )
    return None


async def generate_recovery_alert(
    bot,
    *,
    notion=None,
    metrics_db_id: str | None = None,
    chat_id: int | str | None = None,
    tz=None,
    today: date | None = None,
) -> dict:
    """Run the daily recovery check and send a Telegram message when warranted."""
    if notion is None or metrics_db_id is None or chat_id is None or tz is None:
        from second_brain.main import MY_CHAT_ID, NOTION_HEALTH_METRICS_DB, TZ, notion as main_notion

        notion = notion or main_notion
        metrics_db_id = metrics_db_id if metrics_db_id is not None else NOTION_HEALTH_METRICS_DB
        chat_id = chat_id if chat_id is not None else MY_CHAT_ID
        tz = tz or TZ

    if not metrics_db_id:
        log.warning("recovery_alert: health metrics database not configured; skipping")
        return {"status": "skipped", "reason": "missing_metrics_db"}

    target = today or local_today(tz)
    start = target - timedelta(days=max(_WINDOWS))
    rows = fetch_health_range(notion, metrics_db_id, start, target)
    flag = compute_recovery_flag(rows, target)

    message = build_recovery_message(flag)
    if message is None:
        log.info("recovery_alert: %s severity=%s — no message sent", flag.date, flag.severity)
        return {"status": "quiet", "severity": flag.severity, "date": flag.date}

    await _send_health_message(bot, chat_id, message)
    log.info("recovery_alert: sent %s alert for %s", flag.severity, flag.date)
    return {"status": "sent", "severity": flag.severity, "date": flag.date}


@track_job_execution("recovery_alert")
async def handle_recovery_alert_job(bot=None) -> dict:
    """Utility Scheduler job wrapper for the daily RHR/HRV recovery check."""
    return await generate_recovery_alert(bot)
