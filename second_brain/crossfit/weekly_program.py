"""
Weekly Program lookup and auto-linking for workout logs.

These helpers leverage the existing process_pending_programmes scheduler job,
which marks Weekly Programs as processed and creates Workout Day rows.
"""

from __future__ import annotations

import inspect
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

from second_brain.notion import notion_call


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _weekly_programs_db_id() -> str:
    return os.getenv("NOTION_WEEKLY_PROGRAMS_DB") or os.getenv("NOTION_WORKOUT_PROGRAM_DB", "")


async def get_current_week_program_url(notion_client) -> Optional[str]:
    """
    Query Weekly Programs for the current week's processed program.

    The function name is kept for compatibility with the Phase 1 plan, but it
    returns the Notion page ID because relation properties require page IDs.
    """
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    week_string = f"Week of {monday.strftime('%Y-%m-%d')}"
    weekly_programs_db_id = _weekly_programs_db_id()
    if not weekly_programs_db_id or notion_client is None:
        return None

    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=weekly_programs_db_id,
                filter={
                    "and": [
                        {"property": "Name", "title": {"contains": week_string}},
                        {"property": "Processed", "checkbox": {"equals": True}},
                    ]
                },
                page_size=1,
            )
        )
        if results.get("results"):
            return results["results"][0].get("id")
        return None
    except Exception as e:
        print(f"Error fetching current week program: {e}")
        return None


async def get_todays_workout_day(notion_client) -> Optional[Dict]:
    """
    Get today's Workout Day entry for movement and format reference.

    Returns selected Section B/C properties and relations, or None if no row is
    found for the current weekday in the last seven days.
    """
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    day_name = today.strftime("%A")
    workout_days_db_id = os.getenv("NOTION_WORKOUT_DAYS_DB", "")
    if not workout_days_db_id or notion_client is None:
        return None

    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=workout_days_db_id,
                filter={
                    "and": [
                        {"property": "Day", "select": {"equals": day_name}},
                        {"property": "Week Of", "date": {"equals": monday.strftime("%Y-%m-%d")}},
                    ]
                },
                page_size=10,
            )
        )
        if not results.get("results"):
            return None
        pages = results.get("results", [])
        props = pages[0].get("properties", {})

        def select_name(key: str):
            return (props.get(key, {}).get("select") or {}).get("name")

        def relation_ids(key: str):
            return [rel.get("id") for rel in props.get(key, {}).get("relation", []) if rel.get("id")]

        def rich_text(key: str):
            return "".join(x.get("plain_text", "") for x in props.get(key, {}).get("rich_text", [])) or None

        all_b_movements: list[str] = []
        all_c_movements: list[str] = []
        for matched_page in pages:
            props = matched_page.get("properties", {})
            for movement_id in relation_ids("Section B Movements"):
                if movement_id not in all_b_movements:
                    all_b_movements.append(movement_id)
            for movement_id in relation_ids("Section C Movements"):
                if movement_id not in all_c_movements:
                    all_c_movements.append(movement_id)
        props = pages[0].get("properties", {})
        return {
            "Section B Type": select_name("Section B Type"),
            "Section C Format": select_name("Section C Format"),
            "Section B Movements": all_b_movements,
            "Section C Movements": all_c_movements,
            "Section B": rich_text("Section B"),
            "Section C": rich_text("Section C"),
        }
    except Exception as e:
        print(f"Error fetching today's workout: {e}")
        return None


# TESTING CHECKLIST — Phase 1 Weekly Program Auto-Linking
# [ ] Test get_current_week_program_url returns current week page ID
# [ ] Test returns None if no matching week or Processed = false
# [ ] Test get_todays_workout_day returns correct Day (Monday/Tuesday/etc)
# [ ] Test Section C Format matches expected value (AMRAP/For Time/etc)
# [ ] Verify both functions called in handle_strength_log() and handle_wod_log()
