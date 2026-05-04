"""Habit-related Notion helpers."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any
import logging


log = logging.getLogger(__name__)


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


def get_week_completion_count_for_week(notion, notion_log_db: str, habit_page_id: str, week_of: date) -> int:
    try:
        results = notion.databases.query(
            database_id=notion_log_db,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after": week_of.isoformat()}},
                    {"property": "Date", "date": {"on_or_before": (week_of + timedelta(days=6)).isoformat()}},
                ]
            },
        )
        return len(results.get("results", []))
    except Exception as e:
        log.error("get_week_completion_count_for_week error for %s: %s", habit_page_id, e)
        return 0


def get_previous_streak(notion, notion_streak_db: str, habit_page_id: str, week_of: date) -> int:
    prior_monday = week_of - timedelta(days=7)
    try:
        results = notion.databases.query(
            database_id=notion_streak_db,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Week Of", "date": {"equals": prior_monday.isoformat()}},
                    {"property": "Goal Met", "checkbox": {"equals": True}},
                ]
            },
        )
        rows = results.get("results", [])
        if rows:
            return int(rows[0].get("properties", {}).get("Current Streak", {}).get("number") or 0)
        return 0
    except Exception as e:
        log.error("get_previous_streak error for %s: %s", habit_page_id, e)
        return 0


def get_existing_streak_record(notion, notion_streak_db: str, habit_page_id: str, week_of: date) -> dict | None:
    try:
        results = notion.databases.query(
            database_id=notion_streak_db,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Week Of", "date": {"equals": week_of.isoformat()}},
                ]
            },
        )
        rows = results.get("results", [])
        if not rows:
            return None
        first = rows[0]
        return {
            "page_id": first["id"],
            "current_streak": first.get("properties", {}).get("Current Streak", {}).get("number") or 0,
        }
    except Exception as e:
        log.error("get_existing_streak_record error for %s: %s", habit_page_id, e)
        return None


def write_streak_record(
    notion,
    notion_streak_db: str,
    habit_page_id: str,
    habit_name: str,
    week_of: date,
    completed: int,
    target: int,
    goal_met: bool,
) -> None:
    try:
        current_streak = (get_previous_streak(notion, notion_streak_db, habit_page_id, week_of) + 1) if goal_met else 0
        week_label = week_of.strftime("%-V")
        name = f"{habit_name} — W{week_label} {week_of.year}"
        props = {
            "Name": {"title": [{"text": {"content": name}}]},
            "Habit": {"relation": [{"id": habit_page_id}]},
            "Week Of": {"date": {"start": week_of.isoformat()}},
            "Target": {"number": target},
            "Completed": {"number": completed},
            "Goal Met": {"checkbox": goal_met},
            "Current Streak": {"number": current_streak},
        }
        existing = get_existing_streak_record(notion, notion_streak_db, habit_page_id, week_of)
        if existing:
            notion.pages.update(page_id=existing["page_id"], properties=props)
        else:
            notion.pages.create(parent={"database_id": notion_streak_db}, properties=props)
    except Exception as e:
        log.error("write_streak_record error for %s: %s", habit_page_id, e)


async def check_and_notify_weekly_goals(
    bot,
    chat_id: int,
    notion,
    notion_log_db: str,
    notion_habit_db: str,
    habit_cache: dict[str, dict[str, Any]],
    notified_goals_this_week: set[str],
    get_week_completion_count,
    get_habit_frequency,
) -> None:
    _ = notion, notion_log_db, notion_habit_db
    for habit_name, habit in habit_cache.items():
        habit_page_id = habit["page_id"]
        if habit_page_id in notified_goals_this_week:
            continue
        count = get_week_completion_count(habit_page_id)
        freq = get_habit_frequency(habit_page_id)
        if count >= freq:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🎯 Weekly goal met: {habit_name} — see you next Monday!",
            )
            notified_goals_this_week.add(habit_page_id)


async def record_weekly_streaks(
    bot,
    notion,
    notion_log_db: str,
    notion_habit_db: str,
    notion_streak_db: str,
    habit_cache: dict[str, dict[str, Any]],
    get_current_monday,
    get_habit_frequency,
) -> None:
    _ = bot, notion_habit_db
    last_monday = get_current_monday() - timedelta(days=7)
    for habit_name, habit in habit_cache.items():
        habit_page_id = habit["page_id"]
        completed = get_week_completion_count_for_week(notion, notion_log_db, habit_page_id, last_monday)
        target = get_habit_frequency(habit_page_id)
        goal_met = completed >= target
        write_streak_record(notion, notion_streak_db, habit_page_id, habit_name, last_monday, completed, target, goal_met)
    log.info("Streak records written for %s habits — week of %s", len(habit_cache), last_monday)
