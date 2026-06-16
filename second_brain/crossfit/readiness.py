"""Daily Readiness tracking: one entry per day in the Daily Readiness DB."""

from __future__ import annotations

import inspect
import logging
import os
from typing import Optional

from second_brain.ai.client import VOICE_INSTRUCTION, get_claude_client
from second_brain.config import CLAUDE_MODEL
from second_brain.notion import notion_call
from second_brain.notion.properties import rich_text_prop, title_prop
from second_brain.utils import local_today

log = logging.getLogger(__name__)

_readiness_logged_cache: dict[str, bool] = {}


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def check_readiness_logged_today(notion_client, daily_readiness_db_id: Optional[str] = None) -> bool:
    """Return True if Daily Readiness has already been logged today."""
    today = local_today().isoformat()
    daily_readiness_db_id = daily_readiness_db_id or os.getenv("NOTION_DAILY_READINESS_DB", "")
    if not daily_readiness_db_id or notion_client is None:
        log.debug("check_readiness_logged_today: skipping — db_id=%r client=%r", daily_readiness_db_id, notion_client)
        return False
    if _readiness_logged_cache.get(today):
        return True
    # Primary: query by Date property
    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=daily_readiness_db_id,
                filter={"property": "Date", "date": {"equals": today}},
                page_size=1,
            )
        )
        if results.get("results"):
            _readiness_logged_cache[today] = True
            return True
        log.debug("check_readiness_logged_today: Date filter returned 0 results for %s", today)
    except Exception as e:
        log.warning("check_readiness_logged_today: Date filter failed (%s) — trying title fallback", e)

    # Fallback: query by page title containing today's date
    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=daily_readiness_db_id,
                filter={"property": "Name", "title": {"contains": today}},
                page_size=1,
            )
        )
        if results.get("results"):
            log.warning("check_readiness_logged_today: Date filter missed but title fallback found record for %s", today)
            _readiness_logged_cache[today] = True
            return True
    except Exception as e:
        log.error("check_readiness_logged_today: title fallback also failed for %s: %s", today, e)

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

    today = local_today().isoformat()
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
    result = await _maybe_await(
        notion_call(
            notion_client.pages.create,
            parent={"database_id": daily_readiness_db_id},
            properties={k: v for k, v in properties.items() if v is not None},
        )
    )
    _readiness_logged_cache[today] = True
    return result


def extract_readiness_score(page: dict) -> Optional[float]:
    """Return the value of the Notion formula property named 'Readiness', or None."""
    try:
        prop = page.get("properties", {}).get("Readiness", {})
        formula = prop.get("formula", {})
        value = formula.get("number")
        if value is not None:
            return round(float(value), 2)
    except Exception:
        pass
    return None


def low_readiness_recovery_suggestion(sleep: str, energy: str, soreness: str) -> str:
    try:
        sleep_score = int(sleep)
        energy_score = int(energy)
        soreness_score = int(soreness)
    except Exception:
        return ""
    if sleep_score > 2 and energy_score > 2:
        return ""
    try:
        claude = get_claude_client()
        resp = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=50,
            messages=[{"role": "user", "content": (
                f"{VOICE_INSTRUCTION}\n\n"
                f"Sleep: {sleep_score}/5, Energy: {energy_score}/5, Soreness: {soreness_score}/5.\n"
                "One sentence: recommend full rest, active recovery, or scaled training today. Be direct."
            )}],
        )
        return resp.content[0].text.strip().strip('"')
    except Exception:
        return ""


# TESTING CHECKLIST — Phase 1 Daily Readiness
# [ ] Test check_readiness_logged_today returns False on first run
# [ ] Test log_daily_readiness creates entry in Daily Readiness DB
# [ ] Test subsequent check_readiness_logged_today returns True same day
# [ ] Test CrossFit menu button "📊 Readiness" hides after first log
# [ ] Verify readiness fields removed from Workout Log v2 and WOD Log schemas
