"""Daily Readiness tracking: one entry per day in the Daily Readiness DB."""

from __future__ import annotations

import inspect
import os
from datetime import datetime
from typing import Optional

from second_brain.notion import notion_call
from second_brain.notion.properties import rich_text_prop, title_prop


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def check_readiness_logged_today(notion_client, daily_readiness_db_id: Optional[str] = None) -> bool:
    """Return True if Daily Readiness has already been logged today."""
    today = datetime.now().strftime("%Y-%m-%d")
    daily_readiness_db_id = daily_readiness_db_id or os.getenv("NOTION_DAILY_READINESS_DB", "")
    if not daily_readiness_db_id or notion_client is None:
        return False
    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=daily_readiness_db_id,
                filter={"property": "Date", "date": {"equals": today}},
                page_size=1,
            )
        )
        return len(results.get("results", [])) > 0
    except Exception as e:
        print(f"Error checking readiness: {e}")
        return False


async def log_daily_readiness(
    notion_client,
    sleep_quality: str,
    energy: str,
    mood: str,
    stress: str,
    soreness: str,
    notes: Optional[str] = None,
    hrv: Optional[float] = None,
    resting_hr: Optional[float] = None,
    daily_readiness_db_id: Optional[str] = None,
):
    """Create today's Daily Readiness entry. Call after checking once/day."""
    from .weekly_program import get_current_week_program_url

    weekly_program_id = await get_current_week_program_url(notion_client)
    daily_readiness_db_id = daily_readiness_db_id or os.getenv("NOTION_DAILY_READINESS_DB", "")
    if not daily_readiness_db_id:
        raise ValueError("NOTION_DAILY_READINESS_DB is not configured")

    today = datetime.now().strftime("%Y-%m-%d")
    properties = {
        "Name": title_prop(f"Readiness — {today}"),
        "Date": {"date": {"start": today}},
        "Sleep Quality": {"select": {"name": sleep_quality}},
        "Energy": {"select": {"name": energy}},
        "Mood": {"select": {"name": mood}},
        "Stress": {"select": {"name": stress}},
        "Soreness": {"select": {"name": soreness}},
        "Notes": rich_text_prop(notes) if notes else None,
        "HRV": {"number": hrv} if hrv is not None else None,
        "Resting HR": {"number": resting_hr} if resting_hr is not None else None,
        "Weekly Program": {"relation": [{"id": weekly_program_id}] if weekly_program_id else []},
    }
    return await _maybe_await(
        notion_call(
            notion_client.pages.create,
            parent={"database_id": daily_readiness_db_id},
            properties={k: v for k, v in properties.items() if v is not None},
        )
    )


# TESTING CHECKLIST — Phase 1 Daily Readiness
# [ ] Test check_readiness_logged_today returns False on first run
# [ ] Test log_daily_readiness creates entry in Daily Readiness DB
# [ ] Test subsequent check_readiness_logged_today returns True same day
# [ ] Test CrossFit menu button "📊 Readiness" hides after first log
# [ ] Verify readiness fields removed from Workout Log v2 and WOD Log schemas
