"""Habit-related Notion helpers."""

from __future__ import annotations

import re
import logging
from datetime import date, datetime, timedelta
from typing import Any

from second_brain.utils import ExpiringDict, get_current_monday
from second_brain.notion.properties import (
    extract_checkbox,
    extract_date,
    extract_formula,
    extract_number,
    extract_plain_text,
    extract_rich_text,
    extract_select,
    extract_title,
    get_property_by_name,
    title_prop,
)


log = logging.getLogger(__name__)
habit_cache: ExpiringDict = ExpiringDict(ttl_seconds=1800)


def _parse_show_after(props: dict[str, Any], habit_name: str | None) -> str | None:
    """Read and validate the Show After HH:MM text property from Notion habit properties."""
    try:
        available_props = list(props.keys())
        log.info("DEBUG: Available properties for habit %s: %s", habit_name, available_props)

        show_after_prop = get_property_by_name(props, "Show After")
        show_after_key = "Show After" if show_after_prop else None
        if not show_after_prop:
            log.warning("DEBUG: 'Show After' property not found in props for habit %s", habit_name)
            return None

        log.info(
            "DEBUG: Show After property found as %r, type=%s, value=%s",
            show_after_key,
            show_after_prop.get("type"),
            show_after_prop,
        )
        show_after_raw = extract_plain_text(show_after_prop).strip()
        if re.match(r"^\d{2}:\d{2}$", show_after_raw):
            log.info("DEBUG: Parsed show_after=%s for habit %s", show_after_raw, habit_name)
            return show_after_raw
        if show_after_raw:
            log.warning(
                "DEBUG: Invalid Show After value %r for habit %s; expected HH:MM",
                show_after_raw,
                habit_name,
            )
    except Exception as e:
        log.error("DEBUG: Error reading Show After: %s", e)
    return None


def extract_habit_frequency(props: dict[str, Any]) -> int | None:
    """Return weekly frequency target from a Notion habit page properties dict."""
    for field in ("Frequency Per Week", "Frequency"):
        text = extract_plain_text(props.get(field))
        if not text:
            continue
        match = re.search(r"\d+", text)
        if match:
            value = int(match.group(0))
            if value > 0:
                return value

    label = extract_plain_text(props.get("Frequency Label"))
    if label:
        match = re.search(r"\d+", label)
        if match:
            value = int(match.group(0))
            if value > 0:
                return value

    return None


def load_habit_cache(*, notion: Any, notion_habit_db: str) -> None:
    """Load active habits into module-level ``habit_cache``."""
    global habit_cache
    try:
        results = notion.databases.query(
            database_id=notion_habit_db,
            filter={"property": "Active", "checkbox": {"equals": True}},
        )
        habit_cache.clear()
        for page in results.get("results", []):
            p = page["properties"]
            name = extract_title(p.get("Habit")) or None
            if not name:
                continue

            def sel(key: str) -> str | None:
                return extract_select(p.get(key)) or None

            def num(key: str) -> int | float | None:
                return extract_number(p.get(key))

            def txt(key: str) -> str | None:
                return extract_rich_text(p.get(key)) or None

            parsed_frequency = extract_habit_frequency(p)
            frequency_label = txt("Frequency Label")
            show_after = _parse_show_after(p, name)
            if not frequency_label and parsed_frequency:
                frequency_label = f"{parsed_frequency}x/week"
            page_icon = page.get("icon") or {}
            icon_emoji = page_icon.get("emoji") if isinstance(page_icon, dict) else None
            habit_cache[name] = {
                "page_id": page["id"],
                "name": name,
                "icon": icon_emoji,
                "color": sel("Color"),
                "freq_per_week": parsed_frequency,
                "frequency_label": frequency_label,
                "description": txt("Description"),
                "show_after": show_after,
                "sort": num("Sort") or 99,
            }
        log.info(
            "Habit cache loaded: %s show_after=%s",
            sorted(habit_cache.keys()),
            {habit["name"]: habit.get("show_after") for habit in habit_cache.values()},
        )
    except Exception as e:
        log.error("Failed to load habit cache: %s", e)


def get_active_habits_for_trigger(
    *,
    notion_query_all: Any,
    notion_habit_db: str,
    parse_time_to_minutes: Any,
    count_habit_completions_this_week: Any,
) -> list[dict]:
    """
    DEPRECATED: Legacy function for evening check-in that reads Time Select field.
    Replaced by pending_habits_for_digest(time_str) with show_after filtering.
    Can be removed in v11 after confirming send_evening_checkin() works correctly.
    """
    try:
        results = notion_query_all(
            database_id=notion_habit_db,
            filter={"property": "Active", "checkbox": {"equals": True}},
        )
    except Exception as e:
        log.error("get_active_habits_for_trigger query error: %s", e)
        return []
    habits: list[dict] = []
    for page in results:
        try:
            props = page.get("properties", {})
            name = extract_title(props.get("Habit")) or "Unknown"
            time_prop = props.get("Time", {})
            time_str = extract_select(time_prop) or extract_rich_text(time_prop)
            time_str = time_str.strip() or "—"
            time_minutes = parse_time_to_minutes(time_str if time_str != "—" else None)
            frequency = extract_habit_frequency(props)
            show_after_raw = extract_plain_text(props.get("Show After"))
            show_after = show_after_raw if (show_after_raw and re.match(r"^\d{2}:\d{2}$", show_after_raw)) else None
            completion_count = count_habit_completions_this_week(page["id"])
            if frequency and frequency > 0 and completion_count >= frequency:
                continue
            habits.append(
                {
                    "page_id": page["id"],
                    "name": name or "Unknown",
                    "time_minutes": time_minutes,
                    "time_str": time_str,
                    "frequency": frequency,
                    "completion_count": completion_count,
                    "show_after": show_after,
                    "weather_gated": extract_checkbox(props.get("Weather Gated")),
                }
            )
        except Exception as e:
            log.error("get_active_habits_for_trigger parse error for %s: %s", page.get("id"), e)
    habits.sort(key=lambda h: (h["time_minutes"] < 0, h["time_minutes"] if h["time_minutes"] >= 0 else 10**9, h["name"].lower()))
    return habits


def get_habits_by_time(
    *,
    time_filter: str,
    notion_query_all: Any,
    notion_habit_db: str,
    parse_time_to_minutes: Any,
    count_habit_completions_this_week: Any,
    habit_capped_this_week: Any,
) -> list[dict]:
    """
    DEPRECATED: Legacy function that filtered by Time Select field (🌅 Morning / 🌙 Evening).
    Replaced by pending_habits_for_digest(time_str) with show_after Text field.
    Kept for potential evening check-in migration, but should be removed in v11.
    """
    habits = get_active_habits_for_trigger(
        notion_query_all=notion_query_all,
        notion_habit_db=notion_habit_db,
        parse_time_to_minutes=parse_time_to_minutes,
        count_habit_completions_this_week=count_habit_completions_this_week,
    )
    return [
        h
        for h in habits
        if h.get("time_str") == time_filter and not habit_capped_this_week(h["page_id"])
    ]


def query_tasks_by_auto_horizon(*, notion: Any, notion_db_id: str, horizons: list[str]) -> list[dict]:
    results = notion.databases.query(
        database_id=notion_db_id,
        filter={
            "and": [
                {"property": "Done", "checkbox": {"equals": False}},
                {"or": [{"property": "Auto Horizon", "formula": {"string": {"equals": h}}} for h in horizons]},
            ]
        },
    )
    tasks = []
    for page in results.get("results", []):
        p = page["properties"]
        tasks.append(
            {
                "page_id": page["id"],
                "name": extract_title(p.get("Name")) or "Untitled",
                "auto_horizon": extract_formula(p.get("Auto Horizon")) or "",
                "context": extract_select(p.get("Context")) or "",
                "deadline": extract_date(p.get("Deadline")),
            }
        )
    return tasks


def log_habit(
    notion, log_db_id: str, habit_page_id: str,
    habit_name: str, source: str = "📱 Telegram"
) -> None:
    today = datetime.now().astimezone().date().isoformat()
    props = {
        "Entry": title_prop(habit_name),
        "Habit": {"relation": [{"id": habit_page_id}]},
        "Completed": {"checkbox": True},
        "Date": {"date": {"start": today}},
        "Source": {"select": {"name": source}},
    }
    try:
        notion.pages.create(
            parent={"database_id": log_db_id},
            properties=props,
        )
    except Exception as e:
        # Some log DBs do not expose/allow Source; retry with core fields only.
        log.warning("Habit log create retrying without Source: %s", e)
        minimal = {k: v for k, v in props.items() if k != "Source"}
        notion.pages.create(
            parent={"database_id": log_db_id},
            properties=minimal,
        )
    log.info("Habit logged: %s on %s via %s", habit_name, today, source)


def already_logged_today(notion, log_db_id: str, habit_page_id: str, tz) -> bool:
    today = datetime.now(tz).date().isoformat()
    try:
        results = notion.databases.query(
            database_id=log_db_id,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"equals": today}},
                ]
            },
        )
        return len(results.get("results", [])) > 0
    except Exception as e:
        # Avoid blocking one-tap habit logs when the dedupe query schema drifts.
        log.warning("already_logged_today query failed for %s: %s", habit_page_id, e)
        return False


def get_week_completion_count(notion, log_db_id: str, habit_page_id: str, tz) -> int:
    try:
        results = notion.databases.query(
            database_id=log_db_id,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after": get_current_monday().isoformat()}},
                ]
            },
        )
        return len(results.get("results", []))
    except Exception as e:
        log.error("Error counting weekly completions for habit %s: %s", habit_page_id, e)
        return 0


def get_habit_frequency(notion, habit_page_id: str) -> int:
    try:
        page = notion.pages.retrieve(page_id=habit_page_id)
        properties = page.get("properties", {})
        frequency = extract_habit_frequency(properties)
        if frequency and frequency > 0:
            return frequency
        return 7
    except Exception as e:
        log.error("Error reading habit frequency for %s: %s", habit_page_id, e)
        return 7


def habit_capped_this_week(notion, log_db_id: str, habit_page_id: str, tz) -> bool:
    return get_week_completion_count(notion, log_db_id, habit_page_id, tz) >= get_habit_frequency(notion, habit_page_id)


def _count_habit_completions_this_week(notion, log_db_id: str, habit_page_id: str, tz) -> int:
    """Count completed logs for a habit from Monday through today (inclusive)."""
    try:
        today = datetime.now(tz).date()
        monday = today - timedelta(days=today.weekday())
        results = notion.databases.query(
            database_id=log_db_id,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after": monday.isoformat()}},
                ]
            },
        )
        count = 0
        for row in results.get("results", []):
            date_prop = row.get("properties", {}).get("Date", {}).get("date", {})
            start = date_prop.get("start")
            if not start:
                continue
            try:
                row_day = date.fromisoformat(start[:10])
            except Exception:
                continue
            if monday <= row_day <= today:
                count += 1
        return count
    except Exception as e:
        log.error("Habit weekly completion count error for %s: %s", habit_page_id, e)
        return 0


def logs_this_week(notion, log_db_id: str, habit_page_id: str, tz) -> int:
    today = datetime.now(tz).date()
    monday = today - timedelta(days=today.weekday())
    results = notion.databases.query(
        database_id=log_db_id,
        filter={
            "and": [
                {"property": "Habit", "relation": {"contains": habit_page_id}},
                {"property": "Completed", "checkbox": {"equals": True}},
                {"property": "Date", "date": {"on_or_after": monday.isoformat()}},
                {"property": "Date", "date": {"on_or_before": today.isoformat()}},
            ]
        },
    )
    return len(results.get("results", []))


def is_on_pace(notion, log_db_id: str, habit: dict, tz) -> bool:
    target = habit.get("freq_per_week")
    if not target:
        return False
    return logs_this_week(notion, log_db_id, habit["page_id"], tz) >= target
