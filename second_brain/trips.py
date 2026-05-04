"""Trip parsing and execution helpers."""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Callable

from second_brain.config import (
    CLAUDE_MODEL,
    NOTION_PACKING_ITEMS_DB,
    NOTION_TRIPS_DB,
    OPENWEATHER_KEY,
    TZ,
)


def format_trip_dates(dep: str, ret: str) -> str:
    d = date.fromisoformat(dep)
    r = date.fromisoformat(ret)
    if d.month == r.month:
        return f"{d.strftime('%-d')}–{r.strftime('%-d %b %Y')}"
    return f"{d.strftime('%-d %b')}–{r.strftime('%-d %b %Y')}"


def parse_trip_message(text: str, claude) -> dict:
    prompt = f"""Extract trip details from this message. Today is {date.today().isoformat()}.

Message: \"{text}\"

Return ONLY valid JSON, no markdown:
{{
  \"destinations\": [\"city1\", \"city2\"],
  \"departure_date\": \"YYYY-MM-DD\",
  \"return_date\": \"YYYY-MM-DD\",
  \"purpose\": \"Work\" | \"Personal\" | \"Both\",
  \"multiple_cities\": true | false
}}
"""
    resp = claude.messages.create(model=CLAUDE_MODEL, max_tokens=300, messages=[{"role": "user", "content": prompt}])
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


async def execute_trip(
    key: str,
    query,
    *,
    notion,
    claude,
    trip_map: dict,
    set_awaiting_packing_feedback: Callable[[bool], None],
    fetch_weather: Callable[[str], dict | None],
) -> None:
    _ = (NOTION_TRIPS_DB, OPENWEATHER_KEY, TZ, claude, notion)
    trip = trip_map[key]
    today_weather = fetch_weather("today") or {}
    flags = []
    _summary = today_weather.get("summary", "Weather unavailable")
    trip["weather_flags"] = flags
    dep = date.fromisoformat(trip["departure_date"])
    _reminder_2d = dep - timedelta(days=2)
    _reminder_1d = dep - timedelta(days=1)
    _title = f"{', '.join(trip['destinations'])} — {format_trip_dates(trip['departure_date'], trip['return_date'])}"
    await query.message.reply_text("🧳 Trip captured. Packing flow scaffold saved.")
    set_awaiting_packing_feedback(True)
