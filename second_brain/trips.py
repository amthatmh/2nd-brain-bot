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
    fetch_weather: Callable[[str], dict | None] | None = None,
    fetch_trip_weather_range: Callable[[str, str, str], list[dict]] | None = None,
) -> None:
    _ = (fetch_weather, claude)
    trip = trip_map[key]

    database_id = _normalize_notion_database_id(NOTION_TRIPS_DB)
    if not database_id:
        await query.message.reply_text("⚠️ NOTION_TRIPS_DB looks invalid or inaccessible. Use the exact database ID and ensure it's shared with your integration.")
        return

    title = f"{', '.join(trip['destinations'])} — {format_trip_dates(trip['departure_date'], trip['return_date'])}"

    weather_summary, weather_flags = _build_trip_weather_summary(
        trip.get("departure_date"),
        trip.get("return_date"),
        ", ".join(trip.get("destinations") or []),
        fetch_weather=fetch_weather,
        fetch_trip_weather_range=fetch_trip_weather_range,
    )
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
        "Weather Flags": {"rich_text": [{"text": {"content": weather_flags}}]},
        "Weather Summary": {"rich_text": [{"text": {"content": weather_summary}}]},
    }

    # Avoid sending empty select names; Notion API rejects them.
    if not properties["Duration"]["select"]["name"]:
        properties.pop("Duration")
    properties = _adapt_trip_properties_to_schema(notion, database_id, properties)
    if "Trip" not in properties:
        await query.message.reply_text("⚠️ Your Trips database is missing a title property named 'Trip'.")
        return

    try:
        notion.pages.create(parent={"database_id": database_id}, properties=properties)
    except Exception as exc:
        await query.message.reply_text(f"⚠️ I couldn't save the trip to Notion: {exc}")
        return

    await query.message.reply_text("🧳 Trip saved to Notion. Packing flow scaffold saved.")
    set_awaiting_packing_feedback(True)


def _normalize_notion_database_id(raw_id: str) -> str:
    cleaned = re.sub(r"[^0-9a-fA-F]", "", (raw_id or ""))
    if len(cleaned) != 32:
        return ""
    return f"{cleaned[0:8]}-{cleaned[8:12]}-{cleaned[12:16]}-{cleaned[16:20]}-{cleaned[20:32]}"


def _adapt_trip_properties_to_schema(notion, database_id: str, payload: dict) -> dict:
    try:
        schema = notion.databases.retrieve(database_id=database_id).get("properties", {})
    except Exception:
        return payload

    adapted: dict = {}
    field_work_target = "Field Work" if "Field Work" in schema else ("Field Work Types" if "Field Work Types" in schema else None)
    for name, value in payload.items():
        target_name = field_work_target if name == "Field Work" and field_work_target else name
        prop = schema.get(target_name)
        if not prop:
            continue
        ptype = prop.get("type")
        if name == "Field Work":
            items = [x.get("name", "") for x in value.get("multi_select", []) if x.get("name")]
            if ptype == "multi_select":
                adapted[target_name] = {"multi_select": [{"name": item} for item in items]}
            elif ptype == "rich_text":
                adapted[target_name] = {"rich_text": [{"text": {"content": ", ".join(items) if items else "None"}}]}
            elif ptype == "select" and items:
                adapted[target_name] = {"select": {"name": items[0]}}
            continue
        if name == "Weather Flags":
            raw_text = ""
            if value.get("rich_text"):
                raw_text = value["rich_text"][0].get("text", {}).get("content", "")
            tokens = [t.strip() for t in raw_text.split(",") if t.strip()]
            if ptype == "multi_select":
                adapted[target_name] = {"multi_select": [{"name": item} for item in tokens]}
            elif ptype == "rich_text":
                adapted[target_name] = {"rich_text": [{"text": {"content": ", ".join(tokens)}}]}
            continue
        if name == "Weather Summary":
            raw_text = ""
            if value.get("rich_text"):
                raw_text = value["rich_text"][0].get("text", {}).get("content", "")
            if ptype == "rich_text":
                adapted[target_name] = {"rich_text": [{"text": {"content": raw_text}}]}
            elif ptype == "select" and raw_text:
                adapted[target_name] = {"select": {"name": raw_text[:100]}}
            continue
        adapted[target_name] = value
    return adapted


def _build_trip_weather_summary(
    departure_date: str | None,
    return_date: str | None,
    destination: str,
    *,
    fetch_weather: Callable[[str], dict | None] | None,
    fetch_trip_weather_range: Callable[[str, str, str], list[dict]] | None,
) -> tuple[str, str]:
    snapshots: list[tuple[str, dict]] = []
    if fetch_trip_weather_range and departure_date and return_date and destination:
        try:
            rows = fetch_trip_weather_range(departure_date, return_date, destination)
        except Exception:
            rows = []
        for row in rows:
            label = row.get("label") or row.get("date") or "Day"
            snapshots.append((label, row))
    elif fetch_weather:
        for bucket in ("today", "tomorrow"):
            try:
                data = fetch_weather(bucket)
            except Exception:
                data = None
            if data:
                snapshots.append((bucket, data))
    if not snapshots:
        return "", ""
    labels: list[str] = []
    flags: list[str] = []
    for bucket, item in snapshots:
        condition = item.get("condition", "Unknown")
        precip = int(item.get("precip_chance", 0))
        hi = item.get("temp_high", item.get("temp"))
        lo = item.get("temp_low", item.get("temp"))
        labels.append(f"{bucket.title()}: {condition}, {lo}–{hi}°C, {precip}% rain")
        condition_l = condition.lower()
        if precip >= 40 or "rain" in condition_l or "drizzle" in condition_l or "thunder" in condition_l:
            flags.append("Rain")
        if hi is not None and hi >= 30:
            flags.append("Hot")
        if lo is not None and lo <= 5:
            flags.append("Cold")
        if "snow" in condition_l or "sleet" in condition_l or "blizzard" in condition_l:
            flags.append("Snow")
    return " | ".join(labels), ", ".join(sorted(set(flags)))
