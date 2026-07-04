"""TRMNL e-ink habit tracker card.

Renders the past 7 days of each opted-in habit as a dot grid, one habit per row.
Habits opt in via a ``TRMNL`` checkbox in the Habit DB, so noise like Stretching,
Sleep or Weigh-in stays off the small display.

The card reuses the HabitKit dashboard rollup (``_build_habits_data_payload``,
served from ``STATE.habits_data_cache``) so the day grid stays consistent with
the web dashboard. The only real logic — slicing that rollup down to a 7-day
card payload — is a pure function so it can be unit tested without Notion.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date

from aiohttp import web

log = logging.getLogger(__name__)

# Monday-first weekday initials for the column headers.
_DAY_LETTER = ["M", "T", "W", "T", "F", "S", "S"]
_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

WINDOW_DAYS = 7


def _weekday_letter(iso: str) -> str:
    try:
        return _DAY_LETTER[date.fromisoformat(iso).weekday()]
    except (ValueError, TypeError):
        return ""


def _range_label(window: list[str]) -> str:
    """"Jun 29 – Jul 5" from the first/last date in the window."""
    if not window:
        return ""
    try:
        start = date.fromisoformat(window[0])
        end = date.fromisoformat(window[-1])
    except (ValueError, TypeError):
        return ""
    return f"{_MONTH_ABBR[start.month]} {start.day} – {_MONTH_ABBR[end.month]} {end.day}"


def build_habit_card_payload(habits_data: dict, today: date) -> dict:
    """Reduce the HabitKit rollup to the past-7-days habit card payload.

    ``habits_data`` is the ``_build_habits_data_payload`` shape: ``dates`` (the
    full history window) and ``habits`` (each with a ``days`` 0/1 array aligned
    to ``dates``, plus ``trmnl``/``dayStreak``/``icon``). Only habits with
    ``trmnl`` truthy are shown; each is sliced to the last 7 days.
    """
    dates = habits_data.get("dates") or []
    window = dates[-WINDOW_DAYS:]
    day_headers = [_weekday_letter(d) for d in window]

    habits_out = []
    for habit in habits_data.get("habits", []):
        if not habit.get("trmnl"):
            continue
        days = (habit.get("days") or [])[-WINDOW_DAYS:]
        habits_out.append(
            {
                "name": habit.get("name"),
                "icon": habit.get("icon"),
                "days": days,
                "done": sum(1 for d in days if d),
                "streak": habit.get("dayStreak", 0),
                "today_done": bool(habit.get("todayDone")),
            }
        )

    return {
        "generated_at": habits_data.get("generated"),
        "today_date": habits_data.get("todayDate") or today.isoformat(),
        "range_label": _range_label(window),
        "day_headers": day_headers,
        "habits": habits_out,
        "count": len(habits_out),
    }


def create_trmnl_habits_handler(*, tz):
    """Token-guarded JSON endpoint that TRMNL polls for the habit card.

    Serves the already-maintained HabitKit cache (prewarmed on startup and
    refreshed on schedule) so the endpoint adds no Notion queries of its own.
    """
    from datetime import datetime

    from second_brain.state import STATE

    async def handler(request: web.Request) -> web.Response:
        token = os.environ.get("TRMNL_HABITS_TOKEN", "").strip()
        if not token:
            return web.json_response({"error": "not_configured"}, status=503)
        if request.rel_url.query.get("token") != token:
            return web.json_response({"error": "forbidden"}, status=403)

        habits_data = STATE.habits_data_cache.get("payload")
        if not habits_data:
            return web.json_response({"error": "cache_cold"}, status=503)

        today = datetime.now(tz).date()
        payload = build_habit_card_payload(habits_data, today)
        return web.Response(text=json.dumps(payload), content_type="application/json")

    return handler
