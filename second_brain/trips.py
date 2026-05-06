"""Trip parsing and execution helpers."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Callable

from second_brain.config import (
    CLAUDE_MODEL,
    NOTION_PACKING_ITEMS_DB,
    NOTION_TRIPS_DB,
)
from second_brain.notion import notion_call

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
    _ = (fetch_weather, claude, set_awaiting_packing_feedback)
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
        page = notion_call(notion.pages.create, parent={"database_id": database_id}, properties=properties)
    except Exception as exc:
        await query.message.reply_text(f"⚠️ I couldn't save the trip to Notion: {exc}")
        return

    page_id = (page or {}).get("id")

    blocks: list[dict] = []
    if page_id:
        try:
            blocks = build_packing_blocks(trip, notion)
            if blocks:
                notion_call(notion.blocks.children.append, block_id=page_id, children=blocks)
                logger.info("Appended %s packing checklist blocks for trip %s", len(blocks), page_id)
        except Exception as exc:
            logger.warning("Packing checklist generation failed for trip %s: %s", page_id, exc)
    else:
        logger.warning("Trip page ID missing; skipping packing checklist generation for %s", title)

    item_count = sum(1 for block in blocks if block.get("type") == "to_do")
    await query.message.reply_text(f"✅ Trip saved to Notion. Packing checklist added ({item_count} items).")


def build_packing_blocks(trip: dict, notion_client=None) -> list[dict]:
    """
    Query NOTION_PACKING_ITEMS_DB, filter by trip context, group by Category,
    return a flat list of heading_2 + to_do Notion blocks.
    """
    if not NOTION_PACKING_ITEMS_DB:
        logger.warning("NOTION_PACKING_ITEMS_DB is not set; skipping packing checklist generation")
        return []
    if notion_client is None:
        notion_client = globals().get("notion")
    if notion_client is None:
        raise ValueError("A Notion client is required to build packing blocks")

    field_work = {fw.lower() for fw in trip.get("field_work_types", []) if fw}

    items = []
    cursor = None
    while True:
        kwargs = {"database_id": NOTION_PACKING_ITEMS_DB}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion_call(notion_client.databases.query, **kwargs)
        items.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    grouped: dict[str, list[str]] = {}
    for page in items:
        props = page.get("properties", {})
        name = _extract_title(props)
        if not name:
            continue
        always = props.get("Always", {}).get("checkbox", False)
        fw_tags = _extract_multi_select(props.get("Field Work"))
        matches_fw = bool(field_work & {tag.lower() for tag in fw_tags})
        if not always and not matches_fw:
            continue
        category = _extract_select_or_text(props.get("Category")) or "Other"
        grouped.setdefault(category, []).append(name)

    blocks: list[dict] = []
    for category in sorted(grouped):
        blocks.append(
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": category}}],
                },
            }
        )
        for item in sorted(grouped[category]):
            blocks.append(
                {
                    "object": "block",
                    "type": "to_do",
                    "to_do": {
                        "rich_text": [{"type": "text", "text": {"content": item}}],
                        "checked": False,
                    },
                }
            )
    return blocks


def _extract_title(props: dict) -> str:
    """Extract plain text from whichever property has type == 'title'."""
    for prop in props.values():
        if prop.get("type") == "title":
            return "".join(rich_text.get("plain_text", "") for rich_text in prop.get("title", [])).strip()
    return ""


def _extract_select_or_text(prop: dict | None) -> str:
    """Extract string from a select or rich_text Notion property."""
    if not prop:
        return ""
    prop_type = prop.get("type")
    if prop_type == "select":
        return (prop.get("select") or {}).get("name", "").strip()
    if prop_type == "rich_text":
        return "".join(rich_text.get("plain_text", "") for rich_text in prop.get("rich_text", [])).strip()
    return ""


def _extract_multi_select(prop: dict | None) -> list[str]:
    """Extract list of option names from a multi_select Notion property."""
    if not prop or prop.get("type") != "multi_select":
        return []
    return [option.get("name", "") for option in prop.get("multi_select", []) if option.get("name")]


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


def refresh_upcoming_trip_weather(
    notion,
    notion_trips_db: str,
    *,
    fetch_trip_weather_range: Callable[[str, str, str], list[dict]] | None,
    lookahead_days: int = 5,
) -> int:
    if not notion_trips_db or not fetch_trip_weather_range:
        return 0
    today = date.today()
    upper = today.toordinal() + lookahead_days
    try:
        resp = notion.databases.query(
            database_id=notion_trips_db,
            filter={
                "and": [
                    {"property": "Departure Date", "date": {"on_or_after": today.isoformat()}},
                    {"property": "Departure Date", "date": {"on_or_before": date.fromordinal(upper).isoformat()}},
                ]
            },
            page_size=50,
        )
    except Exception:
        return 0
    rows = resp.get("results", [])
    updated = 0
    for row in rows:
        props = row.get("properties", {})
        dep = props.get("Departure Date", {}).get("date", {}).get("start")
        ret = props.get("Return Date", {}).get("date", {}).get("start")
        dest_parts = props.get("Destination(s)", {}).get("rich_text", [])
        destination = dest_parts[0].get("plain_text", "").strip() if dest_parts else ""
        if not dep or not ret or not destination:
            continue
        summary, flags = _build_trip_weather_summary(
            dep,
            ret,
            destination,
            fetch_weather=None,
            fetch_trip_weather_range=fetch_trip_weather_range,
        )
        payload = _adapt_trip_properties_to_schema(
            notion,
            notion_trips_db,
            {
                "Trip": {"title": [{"text": {"content": "ignore"}}]},
                "Weather Flags": {"rich_text": [{"text": {"content": flags}}]},
                "Weather Summary": {"rich_text": [{"text": {"content": summary}}]},
            },
        )
        payload.pop("Trip", None)
        if not payload:
            continue
        try:
            notion.pages.update(page_id=row["id"], properties=payload)
            updated += 1
        except Exception:
            continue
    return updated
