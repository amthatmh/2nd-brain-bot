"""Trip parsing and execution helpers."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta
from typing import Callable

from second_brain.ai.client import get_claude_client
from second_brain.config import (
    CLAUDE_MODEL,
    NOTION_PACKING_ITEMS_DB,
    NOTION_TRIPS_DB,
)
from second_brain.notion import notion_call
from second_brain.notion.properties import (
    extract_multi_select,
    extract_rich_text,
    extract_select,
    extract_title,
    query_all,
    rich_text_prop,
    title_prop,
)
from utils.date_parser import parse_date

logger = logging.getLogger(__name__)


WEATHER_PLACEHOLDER_SUMMARY = "⏳ Weather forecast available 5 days before departure"


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
  \"purpose\": [\"Work\"] | [\"Personal\"] | [\"Work\", \"Personal\"],
  \"multiple_cities\": true | false
}}
"""
    fallback = {
        "destinations": _extract_destinations(text),
        "departure_date": None,
        "return_date": None,
        "purpose_list": _infer_purpose_list(text),
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
            "departure_date": _resolve_trip_date(parsed.get("departure_date")),
            "return_date": _resolve_trip_date(parsed.get("return_date")),
            "purpose_list": _normalize_purpose_list(parsed.get("purpose")) or fallback["purpose_list"],
            "multiple_cities": bool(parsed.get("multiple_cities")),
        }
    except Exception as exc:
        logger.warning("Trip NLP parse failed, using fallback extraction: %s", exc)
        return fallback


def _resolve_trip_date(raw: str | None) -> str | None:
    if not raw:
        return None
    parsed = parse_date(raw)
    if parsed.ambiguous:
        return None
    return parsed.resolved


def _extract_destinations(text: str) -> list[str]:
    match = re.search(r"\bto\s+([^,;]+)", text, re.IGNORECASE)
    if not match:
        return ["Trip"]
    raw = re.sub(r"\bfrom\b.*$", "", match.group(1), flags=re.IGNORECASE).strip()
    parts = [p.strip() for p in re.split(r"\s*(?:/| and )\s*", raw) if p.strip()]
    return parts or ["Trip"]


def _infer_purpose_list(text: str) -> list[str]:
    lower = text.lower()
    has_work = "work" in lower or "site" in lower or "client" in lower
    has_personal = "personal" in lower or "family" in lower or "vacation" in lower
    if has_work and has_personal:
        return ["Work", "Personal"]
    if has_personal:
        return ["Personal"]
    return ["Work"]


def _normalize_purpose_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        raw_values = ["Work", "Personal"] if raw == "Both" else [raw]
    elif isinstance(raw, list):
        raw_values = raw
    else:
        raw_values = []

    normalized: list[str] = []
    for value in raw_values:
        if not isinstance(value, str):
            continue
        token = value.strip()
        if token == "Both":
            for purpose in ("Work", "Personal"):
                if purpose not in normalized:
                    normalized.append(purpose)
        elif token in {"Work", "Personal"} and token not in normalized:
            normalized.append(token)
    return normalized


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
    schedule_weather_refresh: Callable[[str, dict], None] | None = None,
) -> None:
    _ = (claude, set_awaiting_packing_feedback)
    trip = trip_map[key]

    database_id = _normalize_notion_database_id(NOTION_TRIPS_DB)
    if not database_id:
        await query.message.reply_text("⚠️ NOTION_TRIPS_DB looks invalid or inaccessible. Use the exact database ID and ensure it's shared with your integration.")
        return

    title = f"{', '.join(trip['destinations'])} — {format_trip_dates(trip['departure_date'], trip['return_date'])}"

    needs_weather_refresh = False
    try:
        days_until_departure = (date.fromisoformat(trip["departure_date"]) - date.today()).days
    except Exception:
        days_until_departure = 0
    if days_until_departure > 5:
        weather_summary, weather_flags = WEATHER_PLACEHOLDER_SUMMARY, []
        needs_weather_refresh = True
    else:
        weather_summary, weather_flags = _build_trip_weather_summary(
            trip.get("departure_date"),
            trip.get("return_date"),
            ", ".join(trip.get("destinations") or []),
            fetch_weather=fetch_weather,
            fetch_trip_weather_range=fetch_trip_weather_range,
        )
    properties = {
        "Trip": title_prop(title),
        "Departure Date": {"date": {"start": trip["departure_date"]}},
        "Return Date": {"date": {"start": trip["return_date"]}},
        "Destination(s)": rich_text_prop(", ".join(trip.get("destinations") or [])),
        "Duration": {"select": {"name": trip.get("duration_label") or ""}},
        "Purpose": {"multi_select": [{"name": purpose} for purpose in (trip.get("purpose_list") or ["Work"])]},
        "Field Work": {"multi_select": [{"name": item} for item in (trip.get("field_work_types") or []) if item and item != "None"]},
        "Multiple Sites": {"checkbox": bool(trip.get("multiple_sites"))},
        "Checked Luggage": {"checkbox": bool(trip.get("checked_luggage"))},
        "Weather Flags": {"multi_select": [{"name": item} for item in weather_flags]},
        "Weather Summary": rich_text_prop(weather_summary),
        "Reminder Sent": {"checkbox": False},
        "Packing Done": {"checkbox": False},
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
    if page_id:
        trip["notion_page_id"] = page_id
        if needs_weather_refresh and schedule_weather_refresh:
            schedule_weather_refresh(key, trip)

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
    purpose_list = trip.get("purpose_list") or ["Work"]

    items = query_all(notion_client, NOTION_PACKING_ITEMS_DB, page_size=None)

    grouped: dict[str, list[str]] = {}
    for page in items:
        props = page.get("properties", {})
        name = extract_title(props.get("Item"))
        if not name:
            continue
        always = props.get("Always", {}).get("checkbox", False)
        fw_tags = extract_multi_select(props.get("Field Work"))
        fw_tag_set = {tag.lower() for tag in fw_tags}
        matches_fw = bool(field_work & fw_tag_set)
        matches_purpose = (
            ("work" in fw_tag_set and "Work" in purpose_list)
            or ("personal" in fw_tag_set and "Personal" in purpose_list)
            or (props.get("Work", {}).get("checkbox", False) and "Work" in purpose_list)
            or (props.get("Personal", {}).get("checkbox", False) and "Personal" in purpose_list)
        )
        if not always and not matches_fw and not matches_purpose:
            continue
        category = extract_select(props.get("Category")) or extract_rich_text(props.get("Category")) or "Other"
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
                adapted[target_name] = rich_text_prop(", ".join(items) if items else "None")
            elif ptype == "select" and items:
                adapted[target_name] = {"select": {"name": items[0]}}
            continue
        if name == "Purpose":
            items = [x.get("name", "") for x in value.get("multi_select", []) if x.get("name")]
            if ptype == "multi_select":
                adapted[target_name] = {"multi_select": [{"name": item} for item in items]}
            elif ptype == "rich_text":
                adapted[target_name] = rich_text_prop(", ".join(items))
            elif ptype == "select" and items:
                adapted[target_name] = {"select": {"name": "Both" if set(items) == {"Work", "Personal"} else items[0]}}
            continue
        if name == "Weather Flags":
            if "multi_select" in value:
                tokens = [x.get("name", "") for x in value.get("multi_select", []) if x.get("name")]
            else:
                raw_text = ""
                if value.get("rich_text"):
                    raw_text = value["rich_text"][0].get("text", {}).get("content", "")
                tokens = [t.strip() for t in raw_text.split(",") if t.strip()]
            if ptype == "multi_select":
                adapted[target_name] = {"multi_select": [{"name": item} for item in tokens]}
            elif ptype == "rich_text":
                adapted[target_name] = rich_text_prop(", ".join(tokens))
            continue
        if name == "Weather Summary":
            raw_text = ""
            if value.get("rich_text"):
                raw_text = value["rich_text"][0].get("text", {}).get("content", "")
            if ptype == "rich_text":
                adapted[target_name] = rich_text_prop(raw_text)
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
) -> tuple[str, list[str]]:
    if not _is_departure_within_forecast_window(departure_date):
        return WEATHER_PLACEHOLDER_SUMMARY, []
    import os; logger.info("trips: key_present=%s dest=%r dep=%r", bool(os.environ.get("OPENWEATHER_KEY","").strip()), destination, departure_date)
    snapshots: list[tuple[str, dict]] = []
    caught_exception: Exception | None = None
    if fetch_trip_weather_range and departure_date and return_date and destination:
        try:
            rows = fetch_trip_weather_range(departure_date, return_date, destination)
        except Exception as exc:
            caught_exception = exc
            rows = []
        for row in rows:
            label = row.get("label") or row.get("date") or "Day"
            snapshots.append((label, row))
    elif fetch_weather:
        for bucket in ("today", "tomorrow"):
            try:
                data = fetch_weather(bucket)
            except Exception as exc:
                caught_exception = exc
                data = None
            if data:
                snapshots.append((bucket, data))
    if not snapshots:
        logger.error(
            "trip_weather_summary: empty summary for destination=%r departure_date=%r return_date=%r exception=%r",
            destination,
            departure_date,
            return_date,
            caught_exception,
        )
        return "", []
    labels: list[str] = []
    flags: list[str] = []
    for bucket, item in snapshots:
        label = bucket.title() if isinstance(bucket, str) else str(bucket)
        condition = item.get("condition", "Unknown")
        condition_l = condition.lower()
        description_l = item.get("description", "").lower()
        precip = int(item.get("precip_chance", 0))
        hi = item.get("temp_high", item.get("temp"))
        lo = item.get("temp_low", item.get("temp"))
        wind_max = item.get("wind_speed_max", 0) or 0

        if precip >= 40 or "rain" in condition_l or "drizzle" in condition_l:
            flags.append("Rain")
        if "thunder" in condition_l or "thunderstorm" in condition_l:
            flags.append("⛈️ Thunderstorm")
        if hi is not None and hi >= 30:
            flags.append("Hot")
        if lo is not None and lo <= 5:
            flags.append("Cold")
        if "snow" in condition_l or "sleet" in condition_l or "blizzard" in condition_l:
            flags.append("🌨️ Snow")
        if "fog" in condition_l or "mist" in condition_l or "haze" in condition_l:
            flags.append("🌫️ Fog / Mist")
        if wind_max >= 11:
            flags.append("💨 High wind")

        labels.append(
            f"{label}: {condition}, {lo}–{hi}°C, {precip}% rain"
        )
    unique_flags = sorted(set(flags))
    raw_data = " | ".join(labels)
    field_work_types = destination  # destination is passed as a string; used for context
    try:
        client = get_claude_client()
        prompt = (
            f"You are a travel assistant helping someone prepare for a work trip to {destination}. "
            f"Summarize this weather forecast in 2-3 concise sentences from a packing and preparation perspective. "
            f"Mention any days with notable conditions. Do not use bullet points or markdown.\n\n"
            f"Forecast: {raw_data}"
        )
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        summary = resp.content[0].text.strip()
    except Exception as exc:
        logger.warning("trip_weather_summary: Claude summarization failed: %s", exc)
        summary = raw_data
    return summary, unique_flags


def _is_departure_within_forecast_window(
    departure_date: str | None,
    *,
    today: date | None = None,
    lookahead_days: int = 5,
) -> bool:
    if not departure_date:
        return True
    try:
        departure = date.fromisoformat(departure_date)
    except ValueError:
        return True
    today = today or date.today()
    days_until_departure = (departure - today).days
    return days_until_departure <= lookahead_days


def refresh_upcoming_trip_weather(
    notion,
    notion_trips_db: str,
    *,
    fetch_trip_weather_range: Callable[[str, str, str], list[dict]] | None,
    lookahead_days: int = 5,
) -> int:
    database_id = _normalize_notion_database_id(notion_trips_db) or notion_trips_db
    if not database_id or not fetch_trip_weather_range:
        return 0
    today = date.today()
    upper = today + timedelta(days=lookahead_days)
    try:
        rows = query_all(
            notion,
            database_id,
            filter={
                "and": [
                    {"property": "Departure Date", "date": {"on_or_after": today.isoformat()}},
                    {"property": "Departure Date", "date": {"on_or_before": upper.isoformat()}},
                    {"or": [
                        {"property": "Weather Summary", "rich_text": {"equals": WEATHER_PLACEHOLDER_SUMMARY}},
                        {"property": "Weather Summary", "rich_text": {"is_empty": True}},
                    ]},
                ]
            },
            page_size=50,
        )
    except Exception:
        return 0
    updated = 0
    for row in rows:
        props = row.get("properties", {})
        dep = props.get("Departure Date", {}).get("date", {}).get("start")
        ret = props.get("Return Date", {}).get("date", {}).get("start")
        dest_parts = props.get("Destination(s)", {}).get("rich_text", [])
        destination = dest_parts[0].get("plain_text", "").strip() if dest_parts else ""
        if not dep or not ret or not destination:
            continue
        # Skip if return date already passed
        if ret and date.fromisoformat(ret) < date.today():
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
            database_id,
            {
                "Trip": title_prop("ignore"),
                "Weather Flags": {"multi_select": [{"name": item} for item in flags]},
                "Weather Summary": rich_text_prop(summary),
            },
        )
        payload.pop("Trip", None)
        if not payload:
            continue
        logger.info("trip_weather_refresh: updating page %s with summary=%r flags=%r", row["id"], summary, flags)
        try:
            notion.pages.update(page_id=row["id"], properties=payload)
            updated += 1
            if summary and summary != WEATHER_PLACEHOLDER_SUMMARY:
                try:
                    import asyncio
                    from second_brain.main import app, MY_CHAT_ID

                    dest_name = destination
                    asyncio.create_task(
                        app.bot.send_message(
                            chat_id=MY_CHAT_ID,
                            text=(
                                f"🌦️ Weather ready for {dest_name} ({dep} → {ret}):\n"
                                f"{summary}\n\n"
                                f"Notion trip updated."
                            ),
                            parse_mode="Markdown",
                        )
                    )
                except Exception as notify_exc:
                    logger.warning("trip_weather_refresh: Telegram notify failed: %s", notify_exc)
        except Exception as exc:
            logger.error("trip_weather_refresh: failed to update page %s: %s", row["id"], exc)
            logger.error("trip_weather_refresh: payload was: %s", payload)
            continue
    return updated

# Telegram trip handlers and scheduling helpers migrated from main.py.

from datetime import datetime

from second_brain.config import MY_CHAT_ID, TZ
from second_brain import weather as wx


async def handle_trip_command(update, context) -> None:
    import second_brain.main as _main  # transition import

    message = update.message
    text = " ".join(context.args).strip()
    if not text:
        await message.reply_text('Send your trip details after the command, e.g.:\n/trip work trip to Austin, site testing, Jun 14-17')
        return
    parsed = parse_trip_message(text, _main.claude)
    destinations = parsed.get("destinations") or []
    destination = destinations[0] if destinations else "Trip"
    dep = parsed.get("departure_date")
    ret = parsed.get("return_date")
    key = str(_main._trip_counter)
    _main._trip_counter += 1
    purpose_list = parsed.get("purpose_list") or ["Work"]
    _main.trip_map[key] = {"destination": destination, "destinations": destinations or [destination], "departure_date": dep, "return_date": ret, "duration_label": "", "nights": 0, "purpose_list": purpose_list, "multiple_cities": bool(parsed.get("multiple_cities")), "field_work_types": [], "multiple_sites": None, "checked_luggage": None}
    if not dep or not ret:
        prompt = await message.reply_text("📅 What dates is the trip? (e.g. Jun 14-17)")
        _main.trip_awaiting_date_map[prompt.message_id] = key
        return
    nights = (date.fromisoformat(ret) - date.fromisoformat(dep)).days
    _main.trip_map[key]["nights"] = nights
    trip_days = nights + 1
    _main.trip_map[key]["duration_label"] = "Overnight" if trip_days <= 1 else ("2-3 Days" if trip_days <= 3 else "4-5 Days")
    purpose_str = " + ".join(_main.trip_map[key]["purpose_list"])
    await message.reply_text(f"✈️ {destination} — {format_trip_dates(dep, ret)} ({nights} night(s), {purpose_str})\n\nWhat field work are you doing?\n(Tap all that apply, then tap ✅ Done)", reply_markup=_main.kb.field_work_keyboard(key, _main.trip_map))


async def fetch_weather(city: str, departure_date: str) -> dict:
    summary, flags = _build_trip_weather_summary(
        departure_date,
        departure_date,
        city,
        fetch_weather=None,
        fetch_trip_weather_range=wx.fetch_trip_weather_range,
    )
    return {"summary": summary or "Weather unavailable", "flags": flags}


def weather_triggered_items(flags: list[str]) -> list[str]:
    additions_by_flag = {
        "Rain": "Umbrella / rain jacket",
        "Cold": "Warm layer",
        "Hot": "Sunscreen",
        "Snow": "Winter boots",
    }
    return [additions_by_flag[flag] for flag in flags if flag in additions_by_flag]


def _scheduler_run_datetime(refresh_date: date) -> datetime:
    refresh_time = datetime.strptime("08:00", "%H:%M").time()
    refresh_dt = datetime.combine(refresh_date, refresh_time)
    if hasattr(TZ, "localize"):
        return TZ.localize(refresh_dt)
    return refresh_dt.replace(tzinfo=TZ)


def schedule_weather_refresh(key: str, trip: dict) -> None:
    import second_brain.main as _main  # transition import

    _scheduler = _main._scheduler
    if _scheduler is None:
        logger.warning("Weather refresh not scheduled because scheduler is unavailable — %s", trip.get("destination"))
        return
    page_id = trip.get("notion_page_id")
    if not page_id:
        logger.warning("Weather refresh not scheduled because trip page ID is unavailable — %s", trip.get("destination"))
        return
    refresh_date = date.fromisoformat(trip["departure_date"]) - timedelta(days=3)
    refresh_dt = _scheduler_run_datetime(refresh_date)
    _scheduler.add_job(
        run_weather_refresh,
        "date",
        run_date=refresh_dt,
        args=[page_id, trip["destination"], trip["departure_date"]],
        id=f"weather_{key}",
        replace_existing=True,
        max_instances=1,
    )
    logger.info("Weather refresh scheduled for %s — %s", refresh_dt, trip["destination"])


async def run_weather_refresh(page_id: str, city: str, departure_date: str) -> None:
    import second_brain.main as _main  # transition import

    weather = await fetch_weather(city, departure_date)
    _main.notion.pages.update(
        page_id=page_id,
        properties={
            "Weather Summary": rich_text_prop(weather["summary"]),
            "Weather Flags": {"multi_select": [{"name": f} for f in weather["flags"]]},
        },
    )
    additions = weather_triggered_items(weather["flags"])
    additions_line = f"\n➕ Weather additions: {', '.join(additions)}" if additions else ""
    bot = _main._app_bot
    if bot is None:
        logger.warning("Weather refresh completed but Telegram bot is unavailable for notification")
        return
    await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=f"🌦️ 3-day weather update for {city}:\n{weather['summary']}{additions_line}\n\nNotion trip updated.",
        parse_mode="Markdown",
    )


async def cmd_refreshweather(update, context) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return
    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text("Usage: /refreshweather {page_id} {city} {departure_date}")
        return
    page_id = args[0]
    departure_date = args[-1]
    city = " ".join(args[1:-1]).strip()
    try:
        await run_weather_refresh(page_id, city, departure_date)
        await update.message.reply_text(f"✅ Weather refreshed for {city} ({departure_date}).")
    except Exception as exc:
        logger.error("Manual weather refresh failed: %s", exc)
        await update.message.reply_text(f"⚠️ Weather refresh failed: {exc}")


def get_upcoming_trips_needing_reminder(within_days: int = 2, *, notion=None, notion_trips_db: str | None = None) -> list[dict]:
    if notion is None:
        import second_brain.main as _main  # transition import

        notion = _main.notion
        notion_trips_db = _main.NOTION_TRIPS_DB
    else:
        notion_trips_db = notion_trips_db or NOTION_TRIPS_DB
    if not notion_trips_db:
        return []

    today = date.today()
    cutoff_date = (today + timedelta(days=within_days)).isoformat()

    try:
        results = notion.databases.query(
            database_id=notion_trips_db,
            filter={
                "and": [
                    {"property": "Departure Date", "date": {"on_or_before": cutoff_date}},
                    {"property": "Departure Date", "date": {"on_or_after": today.isoformat()}},
                    {"property": "Reminder Sent", "checkbox": {"equals": False}},
                ]
            },
        )

        trips: list[dict] = []
        for page in results.get("results", []):
            page_id = page["id"]
            props = page.get("properties", {})

            title_prop = props.get("Trip", {}).get("title", [])
            trip_title = "".join(t.get("plain_text", "") for t in title_prop).strip()

            dep_date_prop = props.get("Departure Date", {}).get("date", {}) or {}
            dep_start = dep_date_prop.get("start")
            if not dep_start:
                continue

            ret_date_prop = props.get("Return Date", {}).get("date", {}) or {}
            ret_start = ret_date_prop.get("start")

            dep = date.fromisoformat(dep_start[:10])
            ret = date.fromisoformat(ret_start[:10]) if ret_start else dep
            days_until = (dep - today).days

            purpose_prop = props.get("Purpose", {})
            if purpose_prop.get("multi_select") is not None:
                purpose_list = [p.get("name", "") for p in purpose_prop.get("multi_select", []) if p.get("name")]
            else:
                purpose_name = (purpose_prop.get("select") or {}).get("name", "Work")
                purpose_list = ["Work", "Personal"] if purpose_name == "Both" else [purpose_name]
            purpose_list = purpose_list or ["Work"]

            field_work_prop = props.get("Field Work", {})
            if field_work_prop.get("type") == "rich_text" or field_work_prop.get("rich_text") is not None:
                field_work_text = "".join(r.get("plain_text", "") for r in field_work_prop.get("rich_text", [])).strip()
                field_work = [item.strip() for item in field_work_text.split(",") if item.strip()]
            else:
                field_work = [fw.get("name", "") for fw in field_work_prop.get("multi_select", []) if fw.get("name")]

            weather_summary_prop = props.get("Weather Summary", {}).get("rich_text", [])
            weather_summary = "".join(r.get("plain_text", "") for r in weather_summary_prop).strip()

            weather_flags_prop = props.get("Weather Flags", {})
            if weather_flags_prop.get("type") == "rich_text" or weather_flags_prop.get("rich_text") is not None:
                weather_flags_text = "".join(r.get("plain_text", "") for r in weather_flags_prop.get("rich_text", [])).strip()
                weather_flags = [item.strip() for item in weather_flags_text.split(",") if item.strip()]
            else:
                weather_flags = [wf.get("name", "") for wf in weather_flags_prop.get("multi_select", []) if wf.get("name")]

            trips.append(
                {
                    "page_id": page_id,
                    "title": trip_title,
                    "departure_date": dep,
                    "return_date": ret,
                    "days_until": days_until,
                    "purpose_list": purpose_list,
                    "field_work": field_work,
                    "weather_summary": weather_summary,
                    "weather_flags": weather_flags,
                }
            )

        return trips

    except Exception as e:
        logger.error("Failed to query upcoming trips: %s", e)
        return []


def mark_trip_reminder_sent(page_id: str, *, notion=None) -> None:
    if notion is None:
        import second_brain.main as _main  # transition import

        notion = _main.notion
    try:
        notion.pages.update(page_id=page_id, properties={"Reminder Sent": {"checkbox": True}})
    except Exception as e:
        logger.error("Failed to mark trip reminder sent for %s: %s", page_id[:8], e)


def format_trip_reminder_block(trip: dict) -> str:
    lines = [
        f"🧳 *{trip['title']}*",
        f"📅 Departing in {trip['days_until']} day{'s' if trip['days_until'] != 1 else ''} ({trip['departure_date'].strftime('%a, %b %d')})",
    ]

    field_work_display = trip["field_work"]
    purpose_str = " + ".join(trip.get("purpose_list") or ["Work"])
    if field_work_display and field_work_display != ["None"]:
        lines.append(f"🎯 {purpose_str} trip · {', '.join(field_work_display)}")
    else:
        lines.append(f"🎯 {purpose_str} trip")

    lines.append("")
    lines.append("🌤️ *Forecast:*")

    weather_summary = trip["weather_summary"]
    if weather_summary and weather_summary not in {"⏳ Weather forecast available 5 days before departure", "Weather unavailable"}:
        lines.append(f"```\n{weather_summary}\n```")
    else:
        lines.append("_Weather data unavailable_")

    if trip["weather_flags"]:
        lines.append(f"⚠️ {', '.join(trip['weather_flags'])}")

    return "\n".join(lines)


def append_trip_reminders_to_text(text: str, within_days: int = 2) -> str:
    import second_brain.main as _main  # transition import

    upcoming_trips = _main.get_upcoming_trips_needing_reminder(within_days=within_days)
    if not upcoming_trips:
        return text

    trip_blocks = [format_trip_reminder_block(trip) for trip in upcoming_trips]
    text = f"{text}\n\n{'─' * 30}\n\n" + "\n\n".join(trip_blocks)
    for trip in upcoming_trips:
        _main.mark_trip_reminder_sent(trip["page_id"])
    return text


async def update_trip_weather_job(application) -> None:
    _ = application
    import second_brain.main as _main  # transition import

    if not NOTION_TRIPS_DB:
        return
    try:
        updated = refresh_upcoming_trip_weather(
            _main.notion,
            NOTION_TRIPS_DB,
            fetch_trip_weather_range=wx.fetch_trip_weather_range,
            lookahead_days=5,
        )
        if updated:
            logger.info("Trip weather refresh updated %d trip(s)", updated)
    except Exception as exc:
        logger.warning("Trip weather refresh failed: %s", exc)


async def refresh_trip_weather_job(bot) -> None:
    await update_trip_weather_job(bot)


async def _run_trip_weather_refresh(bot) -> dict:
    _ = bot
    import second_brain.main as _main  # transition import

    if not NOTION_TRIPS_DB:
        logger.info("trip_weather_refresh: NOTION_TRIPS_DB is not set")
        return {"action": "error", "reason": "NOTION_TRIPS_DB is not set"}

    try:
        updated = refresh_upcoming_trip_weather(
            _main.notion,
            NOTION_TRIPS_DB,
            fetch_trip_weather_range=wx.fetch_trip_weather_range,
            lookahead_days=7,
        )
    except Exception as exc:
        logger.error("trip_weather_refresh: unexpected error: %s", exc)
        return {"action": "error", "reason": str(exc)}

    if not updated:
        logger.info("trip_weather_refresh: no upcoming trips required weather updates")
        return {"action": "no_trips", "updated": 0}

    logger.info("trip_weather_refresh: updated %d upcoming trip(s)", updated)
    return {"action": "ok", "updated": updated}


async def handle_trip_weather_refresh(bot) -> dict:
    return await _run_trip_weather_refresh(bot)
