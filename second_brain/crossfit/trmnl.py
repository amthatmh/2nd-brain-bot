"""TRMNL e-ink daily workout card.

Renders today's Performance-track workout (Section B strength piece and
Section C conditioning piece) from the Workout Days DB. The device polls a
token-guarded JSON endpoint; only the raw Section B/C text is shown — parsed
metadata (movements relations, duration, format selects) is derivable from the
text and stays off the card.

The only real logic — splitting a section's stored text into workout lines vs
training-note bullets — is a pure function so it can be unit tested without
Notion.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
from datetime import date, timedelta

from aiohttp import web

log = logging.getLogger(__name__)

_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Class-schedule markers like "Midline Focused Chippers/Quartets — 43:00-55:00"
# occasionally survive parsing inside the stored section text; they are gym
# floor-plan noise, not workout content. Same shape as notion._extract_sections.
_TIME_MARKER = re.compile(r"^\s*[\w][\w\s\-/]+—\s*\d{1,2}:\d{2}-\d{1,2}:\d{2}\s*$")

_TRAINING_NOTES = re.compile(r"^\s*training\s+notes?\s*:?\s*$", re.IGNORECASE)

_BULLET_PREFIX = re.compile(r"^[•\-\*]\s*")

PREFERRED_TRACK = "Performance"


def split_section(text: str | None) -> dict:
    """Split stored Section B/C text into workout ``lines`` and ``notes``.

    Everything before a "Training notes:" line is the workout itself; bullet
    lines after it are coaching notes (rendered smaller on the card).
    Schedule time-markers are dropped wherever they appear.
    """
    lines: list[str] = []
    notes: list[str] = []
    in_notes = False
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or _TIME_MARKER.match(line):
            continue
        if _TRAINING_NOTES.match(line):
            in_notes = True
            continue
        line = _BULLET_PREFIX.sub("", line)
        (notes if in_notes else lines).append(line)
    return {"lines": lines, "notes": notes}


def build_workout_card_payload(
    today: date,
    track: str | None,
    section_b: str | None,
    section_c: str | None,
) -> dict:
    """Assemble the workout card payload from today's Workout Days row text."""
    b = split_section(section_b)
    c = split_section(section_c)
    return {
        "found": bool(b["lines"] or c["lines"]),
        "day_label": today.strftime("%A").upper(),
        "date_label": f"{_MONTH_ABBR[today.month]} {today.day}",
        "track": track or "",
        "section_b": b,
        "section_c": c,
    }


def _fetch_today_row(notion, workout_days_db_id: str, today: date) -> dict | None:
    """Fetch today's Workout Days row, preferring the Performance track."""
    from second_brain.notion import notion_call

    day_name = today.strftime("%A")
    monday = (today - timedelta(days=today.weekday())).isoformat()
    results = notion_call(
        notion.databases.query,
        database_id=workout_days_db_id,
        filter={
            "and": [
                {"property": "Day", "select": {"equals": day_name}},
                {"property": "Week Of", "date": {"equals": monday}},
            ]
        },
        page_size=5,
    ).get("results", [])
    if not results:
        return None
    return next(
        (
            r for r in results
            if (r.get("properties", {}).get("Track", {}).get("select") or {}).get("name") == PREFERRED_TRACK
        ),
        results[0],
    )


def _rich_text(props: dict, key: str) -> str:
    return "".join(
        chunk.get("plain_text", "")
        for chunk in (props.get(key, {}).get("rich_text") or [])
    ).strip()


def create_trmnl_workout_handler(*, notion, workout_days_db_id: str, tz):
    """Token-guarded JSON endpoint that TRMNL polls for the workout card.

    Today's row rarely changes between polls, so the built payload is cached
    in ``STATE.workout_card_cache`` per calendar day (30 min TTL) to avoid
    hitting Notion on every device refresh.
    """
    from datetime import datetime

    from second_brain.state import STATE

    async def handler(request: web.Request) -> web.Response:
        token = os.environ.get("TRMNL_WORKOUT_TOKEN", "").strip()
        if not token:
            return web.json_response({"error": "not_configured"}, status=503)
        if not hmac.compare_digest(request.rel_url.query.get("token") or "", token):
            return web.json_response({"error": "forbidden"}, status=403)
        if not workout_days_db_id:
            return web.json_response({"error": "missing_db"}, status=503)

        today = datetime.now(tz).date()
        cache_key = today.isoformat()
        payload = STATE.workout_card_cache.get(cache_key)
        if payload is None:
            try:
                row = await asyncio.to_thread(_fetch_today_row, notion, workout_days_db_id, today)
            except Exception as exc:  # noqa: BLE001 - HTTP handler returns JSON errors.
                log.exception("/trmnl/workout error: %s", exc)
                return web.json_response({"error": "build_failure", "message": str(exc)}, status=500)

            props = (row or {}).get("properties", {})
            track = (props.get("Track", {}).get("select") or {}).get("name")
            payload = build_workout_card_payload(
                today,
                track,
                _rich_text(props, "Section B"),
                _rich_text(props, "Section C"),
            )
            STATE.workout_card_cache[cache_key] = payload

        return web.Response(text=json.dumps(payload), content_type="application/json")

    return handler
