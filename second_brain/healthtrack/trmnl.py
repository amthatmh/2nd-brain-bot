"""TRMNL e-ink health coaching card.

Assembles the weekly coaching dashboard rendered on the TRMNL display. The
device polls a token-guarded JSON endpoint; the payload is built here from the
existing dashboard rollups (``build_dashboard_payload``), the recovery flag
(``compute_recovery_flag``) and a small step-count aggregation from the Habit
Log.

The two pieces of real logic — the daily verdict and the step summary — are
pure functions so they can be unit tested without Notion.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from aiohttp import web

# Weekly targets mirror dashboard._activity_score: 3 workout days, 7 step days.
WORKOUT_TARGET: int = 3
STEPS_TARGET_DAYS: int = 7

log = logging.getLogger(__name__)


@dataclass
class Verdict:
    """The daily push/steady/rest call shown at the top of the card."""

    level: str  # "push" | "steady" | "rest"
    word: str
    arrow: str
    line: str  # <=10-word reminder


def _push_line(workout_gap: int) -> str:
    if workout_gap >= 1:
        unit = "workout" if workout_gap == 1 else "workouts"
        return f"Recovered. Add {workout_gap} {unit} — push today."
    return "Recovered. Green light — push hard today."


def compute_verdict(severity: str, workout_gap: int, steps_gap: int) -> Verdict:
    """Combine recovery severity with the weekly activity gap.

    ``severity`` is ``RecoveryFlag.severity`` ("both"/"single"/"none"/
    "no_data"/"insufficient"). Recovery is the body's veto: a red flag forces
    REST regardless of how far behind the week is. When recovery is green the
    activity gap decides PUSH (behind, work harder) vs STEADY (on track).
    """
    if severity == "both":
        return Verdict("rest", "REST", "↓", "HRV suppressed. Rest or easy movement only.")
    if severity == "single":
        return Verdict("steady", "STEADY", "→", "Mild strain. Moderate effort, skip max intensity.")
    if severity == "none":
        if workout_gap > 0 or steps_gap > 0:
            return Verdict("push", "PUSH", "↑", _push_line(workout_gap))
        return Verdict("steady", "STEADY", "→", "Recovered and on track. Maintain today.")
    # no_data / insufficient — baseline still building, don't overreach.
    return Verdict("steady", "STEADY", "→", "Recovery data building. Keep effort steady.")


@dataclass
class StepSummary:
    """Today / 7-day average / week total from the Habit Log step counts."""

    today: int
    avg7: int
    week_total: int


def summarize_steps(rows: list[dict[str, Any]], today: date) -> StepSummary:
    """Reduce dated step-count rows into the three headline numbers.

    ``rows`` is ``[{"date": "YYYY-MM-DD", "count": int}, ...]``. ``avg7`` is the
    mean over the 7 days ending today that actually have a count (missing days
    are excluded, not treated as zero). ``week_total`` sums the current ISO week
    (Monday-anchored, matching dashboard._weekly_activity).
    """
    by_day: dict[date, int] = {}
    for row in rows:
        raw = row.get("date")
        count = row.get("count")
        if not raw or count is None:
            continue
        try:
            day = date.fromisoformat(raw[:10])
        except ValueError:
            continue
        by_day[day] = int(count)

    today_count = by_day.get(today, 0)

    window_start = today - timedelta(days=6)
    window = [c for d, c in by_day.items() if window_start <= d <= today]
    avg7 = round(sum(window) / len(window)) if window else 0

    week_start = today - timedelta(days=today.weekday())
    week_total = sum(c for d, c in by_day.items() if week_start <= d <= today)

    return StepSummary(today=today_count, avg7=avg7, week_total=week_total)


# --- Payload assembly -------------------------------------------------------

_DAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _avg_last7(series: list[dict[str, Any]] | None) -> float | None:
    """Mean of the ``value`` field over the last 7 dated points, or None."""
    points = [p.get("value") for p in (series or []) if p.get("value") is not None]
    tail = points[-7:]
    return sum(tail) / len(tail) if tail else None


def _pct_delta(today: float | None, baseline: float | None) -> float | None:
    if today is None or not baseline:
        return None
    return round((today - baseline) / baseline * 100, 1)


def _arrow(delta: float | None) -> str:
    if delta is None or delta == 0:
        return "→"
    return "↑" if delta > 0 else "↓"


def build_card_payload(
    dashboard: dict[str, Any],
    flag: Any,
    steps: StepSummary,
    today: date,
) -> dict[str, Any]:
    """Flatten dashboard rollups + recovery flag + steps into the card payload.

    Pure: takes already-fetched inputs so it is unit-testable without Notion.
    Recovery numbers (HRV/RHR + their trend) come from ``flag`` so they stay
    consistent with the verdict, which is also derived from ``flag``.
    """
    scores = dashboard.get("scores", {}) or {}
    activity = scores.get("activity", {}) or {}
    weekly = dashboard.get("weekly_activity") or []
    metrics = dashboard.get("metrics", {}) or {}

    latest_week = weekly[-1] if weekly else {"workout_days": 0, "steps_days": 0}
    workout_days = latest_week.get("workout_days", 0)
    steps_days = latest_week.get("steps_days", 0)
    workout_gap = max(WORKOUT_TARGET - workout_days, 0)
    steps_gap = max(STEPS_TARGET_DAYS - steps_days, 0)

    verdict = compute_verdict(flag.severity, workout_gap, steps_gap)

    prev_workout = weekly[-2].get("workout_days", 0) if len(weekly) >= 2 else workout_days
    workout_trend = _arrow(workout_days - prev_workout)

    w7 = flag.windows.get(7) if getattr(flag, "windows", None) else None
    hrv_delta = _pct_delta(flag.today_hrv, getattr(w7, "mean_hrv", None))
    rhr_delta = _pct_delta(flag.today_rhr, getattr(w7, "mean_rhr", None))

    sleep_avg = _avg_last7(metrics.get("total_sleep"))

    return {
        "generated_at": dashboard.get("generated_at"),
        "day_label": f"{_DAY_ABBR[today.weekday()]} · Day {today.weekday() + 1}/7",
        "verdict": {
            "level": verdict.level,
            "word": verdict.word,
            "arrow": verdict.arrow,
            "line": verdict.line,
        },
        "activity": {
            "score": activity.get("value"),
            "desc": activity.get("description", ""),
            "recommendation": activity.get("recommendation", ""),
        },
        "steps": {
            "today": steps.today,
            "avg7": steps.avg7,
            "week_total": steps.week_total,
        },
        "workouts": {"done": workout_days, "target": WORKOUT_TARGET, "trend": workout_trend},
        "hrv": {
            "value": round(flag.today_hrv) if flag.today_hrv is not None else None,
            "delta_pct": hrv_delta,
            "arrow": _arrow(hrv_delta),
        },
        "rhr": {
            "value": round(flag.today_rhr) if flag.today_rhr is not None else None,
            "delta_pct": rhr_delta,
            "arrow": _arrow(rhr_delta),
        },
        "sleep": {
            "avg7_hours": round(sleep_avg, 2) if sleep_avg is not None else None,
            "arrow": _arrow(_pct_delta(sleep_avg, 7.0)),
        },
    }


# --- HTTP handler -----------------------------------------------------------

# Fetch enough history for the 14-day recovery baseline plus today.
_RECOVERY_LOOKBACK_DAYS = 15
# Steps only need the current week + 7-day window; a small buffer is plenty.
_STEP_LOOKBACK_DAYS = 8


def _fetch_step_rows(notion, log_db_id: str, today: date) -> list[dict[str, Any]]:
    from second_brain.notion.properties import query_all

    start = today - timedelta(days=_STEP_LOOKBACK_DAYS)
    rows = query_all(
        notion,
        log_db_id,
        filter={
            "and": [
                {"property": "Entry", "title": {"equals": "Steps"}},
                {"property": "Date", "date": {"on_or_after": start.isoformat()}},
            ]
        },
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        props = row.get("properties", {})
        day = (props.get("Date", {}).get("date") or {}).get("start")
        count = props.get("Steps Count", {}).get("number")
        if day and count is not None:
            out.append({"date": day, "count": count})
    return out


def create_trmnl_health_handler(
    *, notion, health_metrics_db_id: str, habit_log_db_id: str, tz, readiness_db_id: str = ""
):
    """Token-guarded JSON endpoint that TRMNL polls for the health card."""
    from datetime import datetime

    from second_brain.healthtrack.dashboard import build_dashboard_payload
    from second_brain.healthtrack.insights import fetch_health_range
    from second_brain.healthtrack.recovery import compute_recovery_flag

    async def handler(request: web.Request) -> web.Response:
        token = os.environ.get("TRMNL_HEALTH_TOKEN", "").strip()
        if not token:
            return web.json_response({"error": "not_configured"}, status=503)
        if request.rel_url.query.get("token") != token:
            return web.json_response({"error": "forbidden"}, status=403)
        if not health_metrics_db_id or not habit_log_db_id:
            return web.json_response({"error": "missing_db"}, status=503)

        today = datetime.now(tz).date()
        try:
            dashboard = await asyncio.to_thread(
                build_dashboard_payload,
                notion=notion,
                health_metrics_db_id=health_metrics_db_id,
                habit_log_db_id=habit_log_db_id,
                range_value="1m",
                tz=tz,
                readiness_db_id=readiness_db_id,
            )
            health_rows = await asyncio.to_thread(
                fetch_health_range,
                notion,
                health_metrics_db_id,
                today - timedelta(days=_RECOVERY_LOOKBACK_DAYS),
                today,
            )
            flag = compute_recovery_flag(health_rows, today)
            step_rows = await asyncio.to_thread(_fetch_step_rows, notion, habit_log_db_id, today)
            steps = summarize_steps(step_rows, today)
            payload = build_card_payload(dashboard, flag, steps, today)
        except Exception as exc:  # noqa: BLE001 - HTTP handler returns JSON errors.
            log.exception("/trmnl/health error: %s", exc)
            return web.json_response({"error": "build_failure", "message": str(exc)}, status=500)

        return web.Response(text=json.dumps(payload), content_type="application/json")

    return handler
