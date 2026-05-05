"""Trip parsing and execution helpers."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Callable

from second_brain.config import CLAUDE_MODEL, NOTION_TRIPS_DB

logger = logging.getLogger(__name__)


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
    fallback = {
        "destinations": _extract_destinations(text),
        "departure_date": None,
        "return_date": None,
        "purpose": _infer_purpose(text),
        "multiple_cities": False,
    }

    if claude is None:
        return fallback

    try:
        resp = claude.messages.create(model=CLAUDE_MODEL, max_tokens=300, messages=[{"role": "user", "content": prompt}])
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        parsed = json.loads(raw)
        return {
            "destinations": parsed.get("destinations") or fallback["destinations"],
            "departure_date": parsed.get("departure_date"),
            "return_date": parsed.get("return_date"),
            "purpose": parsed.get("purpose") or fallback["purpose"],
            "multiple_cities": bool(parsed.get("multiple_cities")),
        }
    except Exception as exc:
        logger.warning("Trip NLP parse failed, using fallback extraction: %s", exc)
        return fallback


def _extract_destinations(text: str) -> list[str]:
    match = re.search(r"\bto\s+([^,;]+)", text, re.IGNORECASE)
    if not match:
        return ["Trip"]
    raw = re.sub(r"\bfrom\b.*$", "", match.group(1), flags=re.IGNORECASE).strip()
    parts = [p.strip() for p in re.split(r"\s*(?:/| and )\s*", raw) if p.strip()]
    return parts or ["Trip"]


def _infer_purpose(text: str) -> str:
    lower = text.lower()
    has_work = "work" in lower or "site" in lower or "client" in lower
    has_personal = "personal" in lower or "family" in lower or "vacation" in lower
    if has_work and has_personal:
        return "Both"
    if has_personal:
        return "Personal"
    return "Work"


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
    _ = (fetch_weather, claude)
    trip = trip_map[key]

    database_id = _normalize_notion_database_id(NOTION_TRIPS_DB)
    if not database_id:
        await query.message.reply_text("⚠️ NOTION_TRIPS_DB is not configured, so I couldn't save this trip.")
        return

    title = f"{', '.join(trip['destinations'])} — {format_trip_dates(trip['departure_date'], trip['return_date'])}"

    properties = {
        "Trip": {"title": [{"text": {"content": title}}]},
        "Departure Date": {"date": {"start": trip["departure_date"]}},
        "Return Date": {"date": {"start": trip["return_date"]}},
        "Destination(s)": {"rich_text": [{"text": {"content": ", ".join(trip.get("destinations") or [])}}]},
        "Duration": {"select": {"name": trip.get("duration_label") or ""}},
        "Purpose": {"select": {"name": trip.get("purpose") or "Work"}},
        "Field Work": {"multi_select": [{"name": item} for item in (trip.get("field_work_types") or []) if item and item != "None"]},
        "Multiple Sites": {"checkbox": bool(trip.get("multiple_sites"))},
        "Checked Luggage": {"checkbox": bool(trip.get("checked_luggage"))},
    }

    # Avoid sending empty select names; Notion API rejects them.
    if not properties["Duration"]["select"]["name"]:
        properties.pop("Duration")

    try:
        notion.pages.create(parent={"database_id": database_id}, properties=properties)
    except Exception as exc:
        await query.message.reply_text(f"⚠️ I couldn't save the trip to Notion: {exc}")
        return

    await query.message.reply_text("🧳 Trip saved to Notion. Packing flow scaffold saved.")
    set_awaiting_packing_feedback(True)


def _normalize_notion_database_id(raw_id: str) -> str:
    cleaned = re.sub(r"[^0-9a-fA-F]", "", (raw_id or ""))
    if len(cleaned) < 32:
        return ""
    cleaned = cleaned[-32:]
    return f"{cleaned[0:8]}-{cleaned[8:12]}-{cleaned[12:16]}-{cleaned[16:20]}-{cleaned[20:32]}"
