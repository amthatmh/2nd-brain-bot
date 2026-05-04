"""Habit-related Notion helpers."""

from __future__ import annotations

import re
import logging
from typing import Any
import logging


log = logging.getLogger(__name__)

log = logging.getLogger(__name__)
habit_cache: dict[str, dict[str, Any]] = {}


def _plain_text_from_property(prop: dict[str, Any] | None) -> str:
    """Extract readable text from a Notion property payload."""
    if not prop:
        return ""

    prop_type = prop.get("type")
    if prop_type == "select":
        return ((prop.get("select") or {}).get("name") or "").strip()
    if prop_type == "rich_text":
        rich = prop.get("rich_text") or []
        return "".join(
            (item.get("plain_text") or (item.get("text") or {}).get("content") or "")
            for item in rich
            if isinstance(item, dict)
        ).strip()
    if prop_type == "number":
        value = prop.get("number")
        if isinstance(value, (int, float)):
            return str(int(value))
    if prop_type == "formula":
        formula = prop.get("formula") or {}
        if formula.get("type") == "number" and isinstance(formula.get("number"), (int, float)):
            return str(int(formula["number"]))
        if formula.get("type") == "string":
            return (formula.get("string") or "").strip()

    # Legacy / loose payloads in tests and integrations.
    if isinstance(prop.get("number"), (int, float)):
        return str(int(prop["number"]))
    if isinstance(prop.get("name"), str):
        return prop["name"].strip()
    return ""


def extract_habit_frequency(props: dict[str, Any]) -> int | None:
    """Return weekly frequency target from a Notion habit page properties dict."""
    for field in ("Frequency Per Week", "Frequency"):
        text = _plain_text_from_property(props.get(field))
        if not text:
            continue
        match = re.search(r"\d+", text)
        if match:
            value = int(match.group(0))
            if value > 0:
                return value

    label = _plain_text_from_property(props.get("Frequency Label"))
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
        habit_cache = {}
        for page in results.get("results", []):
            p = page["properties"]
            title_parts = p.get("Habit", {}).get("title", [])
            name = title_parts[0]["text"]["content"] if title_parts else None
            if not name:
                continue

            def sel(key: str) -> str | None:
                s = p.get(key, {}).get("select")
                return s["name"] if s else None

            def num(key: str) -> int | float | None:
                return p.get(key, {}).get("number")

            def txt(key: str) -> str | None:
                parts = p.get(key, {}).get("rich_text", [])
                return parts[0]["text"]["content"] if parts else None

            parsed_frequency = extract_habit_frequency(p)
            frequency_label = txt("Frequency Label")
            if not frequency_label and parsed_frequency:
                frequency_label = f"{parsed_frequency}x/week"
            page_icon = page.get("icon") or {}
            icon_emoji = page_icon.get("emoji") if isinstance(page_icon, dict) else None
            habit_cache[name] = {
                "page_id": page["id"],
                "name": name,
                "icon": icon_emoji,
                "time": sel("Time"),
                "color": sel("Color"),
                "freq_per_week": parsed_frequency,
                "frequency_label": frequency_label,
                "description": txt("Description"),
                "sort": num("Sort") or 99,
            }
        log.info("Habit cache loaded: %s", sorted(habit_cache.keys()))
    except Exception as e:
        log.error("Failed to load habit cache: %s", e)


def get_active_habits_for_trigger(
    *,
    notion_query_all: Any,
    notion_habit_db: str,
    parse_time_to_minutes: Any,
    count_habit_completions_this_week: Any,
) -> list[dict]:
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
            title_parts = props.get("Habit", {}).get("title", [])
            name = title_parts[0].get("plain_text") if title_parts else "Unknown"
            time_prop = props.get("Time", {})
            time_str = ""
            if time_prop.get("type") == "select":
                time_str = (time_prop.get("select") or {}).get("name") or ""
            elif time_prop.get("type") == "rich_text":
                rich = time_prop.get("rich_text", [])
                time_str = (rich[0].get("plain_text") if rich else "") or ""
            time_str = time_str.strip() or "—"
            time_minutes = parse_time_to_minutes(time_str if time_str != "—" else None)
            frequency = extract_habit_frequency(props)
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
                    "weather_gated": props.get("Weather Gated", {}).get("checkbox", False),
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
    del time_filter
    habits = get_active_habits_for_trigger(
        notion_query_all=notion_query_all,
        notion_habit_db=notion_habit_db,
        parse_time_to_minutes=parse_time_to_minutes,
        count_habit_completions_this_week=count_habit_completions_this_week,
    )
    return [h for h in habits if not habit_capped_this_week(h["page_id"])]


def query_tasks_by_auto_horizon(*, notion: Any, notion_db_id: str, horizons: list[str]) -> list[dict]:
    from second_brain.notion import tasks as notion_tasks
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
                "name": notion_tasks._get_prop(p, "Name", "title") or "Untitled",
                "auto_horizon": notion_tasks._get_prop(p, "Auto Horizon", "formula") or "",
                "context": notion_tasks._get_prop(p, "Context", "select") or "",
                "deadline": notion_tasks._get_prop(p, "Deadline", "date"),
            }
        )
    return tasks
