"""
Steps tracking logic for Second Brain health module.

Flow
────
HOURLY SYNC (from Health Auto Export):
  - Receive {steps, date} payload
  - If date == today AND steps >= threshold AND not yet notified today → notify Telegram
  - Store last known step count per date in memory (fast cache)
  - No Notion write on sub-threshold intraday syncs

NIGHTLY 23:59 JOB (final daily stamp):
  - Run every day regardless
  - Use last received step count for that date (could be yesterday if post-midnight)
  - Check Notion for existing entry for that date
      EXISTS  → UPDATE Steps Count field only (preserve Completed status already set)
      MISSING → CREATE entry with Completed=True/False based on threshold

LATE ARRIVAL (phone wakes up after midnight):
  - Health Auto Export fires morning sync including yesterday's final count
  - Payload date is yesterday → backend detects date != today
  - Upserts yesterday's Habit Log entry with final count
  - No duplicate notification (threshold_notified is date-keyed)

State (in-memory, per date key "YYYY-MM-DD"):
  {
    "last_steps": int,
    "threshold_notified": bool,
    "notion_page_id": str | None,  # set after first Notion write for that date
  }
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

# In-memory state keyed by date string "YYYY-MM-DD"
# Survives for the process lifetime; Railway redeploys reset it, but the
# nightly Notion query is the durable source of truth.
_steps_state: dict[str, dict] = {}


def _date_state(date_str: str) -> dict:
    """Get or create mutable state dict for a given date."""
    if date_str not in _steps_state:
        _steps_state[date_str] = {
            "last_steps": 0,
            "threshold_notified": False,
            "notion_page_id": None,
        }
    return _steps_state[date_str]


def _local_today(tz) -> str:
    """Return today's date string in the app timezone."""
    return datetime.now(tz).strftime("%Y-%m-%d")


def _yesterday(tz) -> str:
    """Return yesterday's date string in the app timezone."""
    return (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Notion helpers ────────────────────────────────────────────────────────────

def _find_steps_habit_page_id(notion, habit_db_id: str, habit_name: str) -> str | None:
    """Look up the Steps habit page_id from the Habits DB. Cached via habit_cache upstream."""
    try:
        results = notion.databases.query(
            database_id=habit_db_id,
            filter={
                "and": [
                    {"property": "Habit", "title": {"equals": habit_name}},
                    {"property": "Active", "checkbox": {"equals": True}},
                ]
            },
        )
        pages = results.get("results", [])
        if pages:
            return pages[0]["id"]
        log.warning("steps: habit '%s' not found in Habits DB", habit_name)
        return None
    except Exception as e:
        log.error("steps: error looking up habit page_id: %s", e)
        return None


def _find_existing_log_entry(notion, log_db_id: str, habit_page_id: str, date_str: str) -> str | None:
    """
    Return the Notion page_id of an existing Habit Log entry for this habit+date,
    or None if no entry exists yet.
    """
    try:
        results = notion.databases.query(
            database_id=log_db_id,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Date", "date": {"equals": date_str}},
                ]
            },
        )
        pages = results.get("results", [])
        return pages[0]["id"] if pages else None
    except Exception as e:
        log.error("steps: error querying existing log entry for %s: %s", date_str, e)
        return None


def _create_log_entry(
    notion,
    log_db_id: str,
    habit_page_id: str,
    date_str: str,
    steps: int,
    completed: bool,
    source_label: str,
) -> str | None:
    """Create a new Habit Log entry for steps. Returns the new page_id."""
    try:
        page = notion.pages.create(
            parent={"database_id": log_db_id},
            properties={
                "Entry": {
                    "title": [{"text": {"content": f"Steps — {date_str}"}}]
                },
                "Habit": {"relation": [{"id": habit_page_id}]},
                "Completed": {"checkbox": completed},
                "Date": {"date": {"start": date_str}},
                "Steps Count": {"number": steps},
                "Source": {"select": {"name": source_label}},
            },
        )
        log.info("steps: created log entry %s — %d steps, completed=%s", date_str, steps, completed)
        return page["id"]
    except Exception as e:
        log.error("steps: error creating log entry for %s: %s", date_str, e)
        return None


def _update_log_entry_steps(
    notion,
    page_id: str,
    steps: int,
    completed: bool,
) -> bool:
    """Update Steps Count (and Completed) on an existing Habit Log entry."""
    try:
        notion.pages.update(
            page_id=page_id,
            properties={
                "Steps Count": {"number": steps},
                "Completed": {"checkbox": completed},
            },
        )
        log.info("steps: updated log entry %s — %d steps, completed=%s", page_id, steps, completed)
        return True
    except Exception as e:
        log.error("steps: error updating log entry %s: %s", page_id, e)
        return False


# ── Public API ────────────────────────────────────────────────────────────────

async def handle_steps_sync(
    *,
    steps: int,
    date_str: str,
    notion,
    habit_db_id: str,
    log_db_id: str,
    habit_name: str,
    threshold: int,
    source_label: str,
    tz,
    bot=None,
    chat_id: int | None = None,
    write_intraday_below_threshold: bool = False,
) -> dict:
    """
    Process a steps sync payload from Health Auto Export.

    Called both by:
      - The HTTP webhook handler (real-time hourly syncs)
      - The nightly 23:59 scheduled job (via handle_steps_final_stamp)

    Returns a result dict for logging/debugging:
      {action: "notified"|"updated"|"created"|"skipped", steps, date}
    """
    today = _local_today(tz)
    yesterday = _yesterday(tz)
    is_today = (date_str == today)
    is_yesterday = (date_str == yesterday)

    state = _date_state(date_str)
    state["last_steps"] = steps

    completed = steps >= threshold

    # ── Threshold notification (only for today, only once per day) ──
    if is_today and completed and not state["threshold_notified"] and bot and chat_id:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🎉 10,000 steps hit! 🚶 {steps:,} steps today",
            )
            state["threshold_notified"] = True
            log.info("steps: threshold notification sent (%d steps)", steps)
        except Exception as e:
            log.error("steps: failed to send Telegram notification: %s", e)

    # ── Intraday sub-threshold behavior (configurable) ──
    # Legacy mode only cached intraday counts until threshold/nightly stamp.
    if is_today and not completed and not write_intraday_below_threshold:
        log.debug("steps: sub-threshold intraday sync (%d), caching only", steps)
        return {"action": "skipped", "steps": steps, "date": date_str, "reason": "sub_threshold_intraday"}

    # ── For today above threshold, or any yesterday sync: upsert Notion entry ──
    if not (is_today or is_yesterday):
        log.info("steps: received data for %s (not today/yesterday), skipping", date_str)
        return {"action": "skipped", "steps": steps, "date": date_str, "reason": "old_date"}

    # Resolve habit page_id (check memory first)
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        return {"action": "error", "steps": steps, "date": date_str, "reason": "habit_not_found"}

    # Check for existing Notion entry (check memory cache first to save API calls)
    existing_page_id = state.get("notion_page_id")
    if not existing_page_id:
        existing_page_id = _find_existing_log_entry(notion, log_db_id, habit_page_id, date_str)

    if existing_page_id:
        # UPDATE existing entry (preserve Completed if already True — don't downgrade)
        _update_log_entry_steps(notion, existing_page_id, steps, completed)
        state["notion_page_id"] = existing_page_id
        return {"action": "updated", "steps": steps, "date": date_str, "page_id": existing_page_id}
    else:
        # CREATE new entry
        new_page_id = _create_log_entry(
            notion, log_db_id, habit_page_id, date_str, steps, completed, source_label
        )
        if new_page_id:
            state["notion_page_id"] = new_page_id
        return {"action": "created", "steps": steps, "date": date_str, "page_id": new_page_id}


async def handle_steps_final_stamp(
    *,
    notion,
    habit_db_id: str,
    log_db_id: str,
    habit_name: str,
    threshold: int,
    source_label: str,
    tz,
    bot=None,
    chat_id: int | None = None,
    write_intraday_below_threshold: bool = False,
) -> dict:
    """
    Nightly 23:59 job — write the final daily step count as a permanent record.

    Strategy:
    - Use whatever step count was last received for today (from in-memory state)
    - If no data received today at all (phone off all day), write 0 as Completed=False
    - Also check yesterday for late arrivals (post-midnight scenario)
    - Always runs; creates or updates entries for both today and yesterday if needed
    """
    results = {}
    today = _local_today(tz)
    yesterday = _yesterday(tz)

    # Guard: if we have no cached data at all for today, query Notion first
    # (handles redeploy case where _steps_state was wiped)
    today_str = _local_today(tz)
    if _steps_state.get(today_str, {}).get("last_steps", 0) == 0:
        # Attempt live Notion lookup to avoid writing 0
        habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
        if habit_page_id:
            existing_id = _find_existing_log_entry(notion, log_db_id, habit_page_id, today_str)
            if existing_id:
                try:
                    page = notion.pages.retrieve(page_id=existing_id)
                    steps_raw = page.get("properties", {}).get("Steps Count", {}).get("number") or 0
                    if steps_raw > 0:
                        state = _date_state(today_str)
                        state["last_steps"] = steps_raw
                        state["notion_page_id"] = existing_id
                        log.info(
                            "steps final stamp: recovered %d steps for %s from Notion",
                            steps_raw,
                            today_str,
                        )
                except Exception as e:
                    log.warning("steps final stamp: Notion recovery failed for %s: %s", today_str, e)

    for date_str in (today, yesterday):
        state = _steps_state.get(date_str)
        steps = state["last_steps"] if state else 0

        # For yesterday with no data: skip (we can't know their step count)
        if date_str == yesterday and steps == 0:
            log.info("steps: nightly stamp — no data for yesterday %s, skipping", date_str)
            continue

        log.info("steps: nightly stamp for %s — %d steps", date_str, steps)
        result = await handle_steps_sync(
            steps=steps,
            date_str=date_str,
            notion=notion,
            habit_db_id=habit_db_id,
            log_db_id=log_db_id,
            habit_name=habit_name,
            threshold=threshold,
            source_label=source_label,
            tz=tz,
            bot=bot,
            chat_id=chat_id,
            write_intraday_below_threshold=write_intraday_below_threshold,
        )
        results[date_str] = result

    return results


async def backfill_steps_state_from_notion(
    *,
    notion,
    habit_db_id: str,
    log_db_id: str,
    habit_name: str,
    tz,
) -> None:
    """
    Called once at bot startup. Queries Notion for existing Steps log entries
    for today and yesterday, and pre-populates _steps_state so that a redeploy
    mid-day doesn't cause the 23:59 stamp to write 0.
    """
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        log.warning("steps backfill: habit '%s' not found, skipping", habit_name)
        return

    today = _local_today(tz)
    yesterday = _yesterday(tz)

    for date_str in (today, yesterday):
        try:
            existing_id = _find_existing_log_entry(notion, log_db_id, habit_page_id, date_str)
            if not existing_id:
                continue
            # Fetch the page to read its Steps Count
            page = notion.pages.retrieve(page_id=existing_id)
            steps_prop = page.get("properties", {}).get("Steps Count", {})
            steps = steps_prop.get("number") or 0
            state = _date_state(date_str)
            if steps > state["last_steps"]:  # don't downgrade if webhook already updated
                state["last_steps"] = steps
                state["notion_page_id"] = existing_id
                log.info("steps backfill: %s → %d steps (page %s)", date_str, steps, existing_id)
        except Exception as e:
            log.error("steps backfill: error for %s: %s", date_str, e)


def get_steps_state_summary() -> dict:
    """Return a safe summary of current in-memory state for debugging."""
    return {
        date_str: {
            "last_steps": s["last_steps"],
            "threshold_notified": s["threshold_notified"],
            "has_notion_entry": bool(s.get("notion_page_id")),
        }
        for date_str, s in _steps_state.items()
    }
