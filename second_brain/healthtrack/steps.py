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
    "threshold_message_id": int | None,  # Telegram threshold message for edits
  }
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from second_brain.monitoring import track_job_execution

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
            "threshold_message_id": None,
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
            # Steps entries belong in NOTION_LOG_DB (Habits Log), not the Habits DB.
            parent={"database_id": log_db_id},
            properties={
                "Entry": {
                    "title": [{"text": {"content": "Steps"}}]
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
        current_page = notion.pages.retrieve(page_id=page_id)
        current_completed = (
            current_page
            .get("properties", {})
            .get("Completed", {})
            .get("checkbox", False)
        )
        final_completed = current_completed or completed

        notion.pages.update(
            page_id=page_id,
            properties={
                "Steps Count": {"number": steps},
                "Completed": {"checkbox": final_completed},
            },
        )
        log.info(
            "steps: updated log entry %s — %d steps, completed=%s (was %s)",
            page_id,
            steps,
            final_completed,
            current_completed,
        )
        return True
    except Exception as e:
        log.error("steps: error updating log entry %s: %s", page_id, e)
        return False


def migrate_steps_entry_titles(
    notion,
    log_db_id: str,
    habit_page_id: str,
) -> dict:
    """
    One-time migration: rename legacy Steps entry titles to "Steps".

    Safe to run multiple times. Entries already named "Steps" are skipped,
    while legacy titles such as "Steps — YYYY-MM-DD" and blank Steps titles
    are normalized.
    """
    try:
        renamed = 0
        skipped = 0
        start_cursor = None

        while True:
            query_kwargs = {
                "database_id": log_db_id,
                "filter": {
                    "property": "Habit",
                    "relation": {"contains": habit_page_id},
                },
            }
            if start_cursor:
                query_kwargs["start_cursor"] = start_cursor

            results = notion.databases.query(**query_kwargs)

            for page in results.get("results", []):
                props = page.get("properties", {})
                title_items = props.get("Entry", {}).get("title", [])
                current_title = title_items[0].get("plain_text", "") if title_items else ""

                if current_title == "Steps":
                    skipped += 1
                    continue

                if current_title.startswith("Steps") or not current_title:
                    notion.pages.update(
                        page_id=page["id"],
                        properties={
                            "Entry": {
                                "title": [{"text": {"content": "Steps"}}],
                            },
                        },
                    )
                    renamed += 1
                    log.info(
                        "steps: renamed entry %s from %r to 'Steps'",
                        page["id"],
                        current_title,
                    )
                else:
                    skipped += 1

            if not results.get("has_more"):
                break
            start_cursor = results.get("next_cursor")

        log.info("steps: migration complete — renamed=%d skipped=%d", renamed, skipped)
        return {"renamed": renamed, "skipped": skipped}
    except Exception as e:
        log.error("steps: migration failed: %s", e)
        return {"error": str(e)}


def _persist_threshold_message_id(
    notion,
    env_db_id: str,
    date_str: str,
    message_id: int,
) -> None:
    """Store the threshold notification message_id in Notion ENV DB."""
    try:
        row_name = f"steps_threshold_msg_{date_str}"
        results = {"results": []}
        for filter_type in ("title", "rich_text"):
            results = notion.databases.query(
                database_id=env_db_id,
                filter={"property": "Name", filter_type: {"equals": row_name}},
            )
            if results.get("results"):
                break

        properties = {
            "Value": {"rich_text": [{"text": {"content": str(message_id)}}]},
        }
        if results.get("results"):
            notion.pages.update(
                page_id=results["results"][0]["id"],
                properties=properties,
            )
        else:
            notion.pages.create(
                parent={"database_id": env_db_id},
                properties={
                    "Name": {"title": [{"text": {"content": row_name}}]},
                    **properties,
                },
            )
        log.info("steps: persisted threshold_message_id=%s for %s", message_id, date_str)
    except Exception as e:
        log.warning("steps: could not persist threshold_message_id: %s", e)


def _load_threshold_message_id(notion, env_db_id: str, date_str: str) -> int | None:
    """Load the threshold notification message_id from Notion ENV DB."""
    try:
        row_name = f"steps_threshold_msg_{date_str}"
        results = {"results": []}
        for filter_type in ("title", "rich_text"):
            results = notion.databases.query(
                database_id=env_db_id,
                filter={"property": "Name", filter_type: {"equals": row_name}},
            )
            if results.get("results"):
                break
        if results.get("results"):
            value_prop = results["results"][0].get("properties", {}).get("Value", {})
            items = value_prop.get("rich_text", [])
            if items:
                return int(items[0]["plain_text"])
    except Exception as e:
        log.warning("steps: could not load threshold_message_id: %s", e)
    return None


# ── Public API ────────────────────────────────────────────────────────────────

async def handle_steps_sync(
    *,
    steps: int,
    date_str: str,
    notion,
    habit_db_id: str,
    log_db_id: str,
    env_db_id: str,
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

    def _sync_result(action: str, **extra) -> dict:
        return {
            "action": action,
            "steps": steps,
            "date": date_str,
            "completed": completed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **extra,
        }

    if (
        is_today
        and completed
        and state["threshold_notified"] is False
        and state.get("threshold_message_id") is None
        and env_db_id
    ):
        recovered_id = _load_threshold_message_id(notion, env_db_id, date_str)
        if recovered_id:
            state["threshold_notified"] = True
            state["threshold_message_id"] = recovered_id
            log.info(
                "steps: recovered threshold_message_id=%s from ENV DB for %s",
                recovered_id,
                date_str,
            )

    # ── Threshold notification (send once, then edit with updated count) ──
    if is_today and completed and bot and chat_id:
        notification_text = f"🎉 10,000 steps hit! 🚶 {steps:,} steps today"

        if not state["threshold_notified"]:
            try:
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=notification_text,
                )
                state["threshold_notified"] = True
                state["threshold_message_id"] = sent.message_id
                if env_db_id:
                    asyncio.create_task(
                        asyncio.to_thread(
                            _persist_threshold_message_id,
                            notion,
                            env_db_id,
                            date_str,
                            sent.message_id,
                        )
                    )
                log.info(
                    "steps: threshold notification sent (msg_id=%s, %d steps)",
                    sent.message_id,
                    steps,
                )
            except Exception as e:
                log.error("steps: failed to send threshold notification: %s", e)

        elif state.get("threshold_message_id"):
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=state["threshold_message_id"],
                    text=notification_text,
                )
                log.info(
                    "steps: edited threshold notification (msg_id=%s, %d steps)",
                    state["threshold_message_id"],
                    steps,
                )
            except Exception as e:
                log.warning("steps: edit failed (%s), sending new message", e)
                try:
                    sent = await bot.send_message(
                        chat_id=chat_id,
                        text=notification_text,
                    )
                    state["threshold_message_id"] = sent.message_id
                    if env_db_id:
                        asyncio.create_task(
                            asyncio.to_thread(
                                _persist_threshold_message_id,
                                notion,
                                env_db_id,
                                date_str,
                                sent.message_id,
                            )
                        )
                    log.info("steps: fallback new message sent (msg_id=%s)", sent.message_id)
                except Exception as e2:
                    log.error("steps: fallback send also failed: %s", e2)

    # ── Intraday sub-threshold behavior (configurable) ──
    # Legacy mode only cached intraday counts until threshold/nightly stamp.
    if is_today and not completed and not write_intraday_below_threshold:
        log.debug("steps: sub-threshold intraday sync (%d), caching only", steps)
        return _sync_result("skipped", reason="sub_threshold_intraday")

    # ── For today above threshold, or any yesterday sync: upsert Notion entry ──
    if not (is_today or is_yesterday):
        log.info("steps: received data for %s (not today/yesterday), skipping", date_str)
        return _sync_result("skipped", reason="old_date")

    # Resolve habit page_id (check memory first)
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        return _sync_result("error", reason="habit_not_found")

    # Check for existing Notion entry (check memory cache first to save API calls)
    existing_page_id = state.get("notion_page_id")
    if not existing_page_id:
        existing_page_id = _find_existing_log_entry(notion, log_db_id, habit_page_id, date_str)

    if existing_page_id:
        # UPDATE existing entry (preserve Completed if already True — don't downgrade)
        _update_log_entry_steps(notion, existing_page_id, steps, completed)
        state["notion_page_id"] = existing_page_id
        return _sync_result("updated", page_id=existing_page_id)
    else:
        # CREATE new entry
        new_page_id = _create_log_entry(
            notion, log_db_id, habit_page_id, date_str, steps, completed, source_label
        )
        if new_page_id:
            state["notion_page_id"] = new_page_id
        return _sync_result("created", page_id=new_page_id)


async def handle_steps_final_stamp(
    *,
    notion,
    habit_db_id: str,
    log_db_id: str,
    env_db_id: str,
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
    today_str = _local_today(tz)
    if _steps_state.get(today_str, {}).get("last_steps", 0) == 0:
        habit_page_id = _find_steps_habit_page_id(
            notion, habit_db_id, habit_name
        )
        if habit_page_id:
            existing_id = _find_existing_log_entry(
                notion, log_db_id, habit_page_id, today_str
            )
            if existing_id:
                try:
                    page = notion.pages.retrieve(page_id=existing_id)
                    steps_raw = (
                        page.get("properties", {})
                        .get("Steps Count", {})
                        .get("number")
                        or 0
                    )
                    if steps_raw > 0:
                        state = _date_state(today_str)
                        state["last_steps"] = steps_raw
                        state["notion_page_id"] = existing_id
                        log.info(
                            "steps final stamp: recovered %d steps",
                            steps_raw,
                        )
                except Exception as e:
                    log.warning(
                        "steps final stamp: recovery failed: %s", e
                    )

    yesterday = _yesterday(tz)

    for date_str in (today_str, yesterday):
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
            env_db_id=env_db_id,
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
    """Pre-populate _steps_state from Notion at bot startup so redeploys
    don't cause the 23:59 stamp to write 0.
    """
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        log.warning("steps backfill: habit '%s' not found", habit_name)
        return
    today = _local_today(tz)
    yesterday = _yesterday(tz)
    for date_str in (today, yesterday):
        try:
            existing_id = _find_existing_log_entry(
                notion, log_db_id, habit_page_id, date_str
            )
            if not existing_id:
                continue
            page = notion.pages.retrieve(page_id=existing_id)
            steps = (
                page.get("properties", {})
                .get("Steps Count", {})
                .get("number")
                or 0
            )
            state = _date_state(date_str)
            if steps > state["last_steps"]:
                state["last_steps"] = steps
                state["notion_page_id"] = existing_id
                log.info("steps backfill: %s → %d steps", date_str, steps)
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


@track_job_execution("steps_sync_check")
async def handle_steps_sync_check(bot=None) -> dict:
    """Utility Scheduler job: ensure today's Steps entry exists."""
    from second_brain.main import MY_CHAT_ID, NOTION_HABIT_DB, NOTION_LOG_DB, TZ, notion
    from second_brain.healthtrack import config as health_config
    from second_brain.healthtrack.scheduler import check_and_create_steps_entry

    return await check_and_create_steps_entry(
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        habit_name=health_config.STEPS_HABIT_NAME,
        tz=TZ,
        bot=bot,
        chat_id=MY_CHAT_ID,
    )


@track_job_execution("steps_final_stamp")
async def handle_steps_final_stamp_job(bot=None) -> dict:
    """Utility Scheduler job wrapper for the nightly Steps final stamp."""
    from second_brain.main import MY_CHAT_ID, NOTION_ENV_DB, NOTION_HABIT_DB, NOTION_LOG_DB, TZ, notion
    from second_brain.healthtrack import config as health_config
    from second_brain.healthtrack.config import STEPS_SOURCE_LABEL, STEPS_THRESHOLD

    return await handle_steps_final_stamp(
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        env_db_id=NOTION_ENV_DB,
        habit_name=health_config.STEPS_HABIT_NAME,
        threshold=STEPS_THRESHOLD,
        source_label=STEPS_SOURCE_LABEL,
        tz=TZ,
        bot=bot,
        chat_id=MY_CHAT_ID,
    )
