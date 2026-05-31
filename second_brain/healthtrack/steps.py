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
    "write_lock": asyncio.Lock,  # guards same-date Notion upserts in this process
  }
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from second_brain.monitoring import track_job_execution
from second_brain.utils import local_today
from second_brain.notion.properties import (
    query_all,
    rich_text_prop,
    title_prop,
)

# Compatibility seam for tests that patch the old helper name; runtime call sites use local_today().
_local_today = lambda tz: local_today(tz).isoformat()

log = logging.getLogger(__name__)

# In-memory state keyed by date string "YYYY-MM-DD"
# Survives for the process lifetime; Railway redeploys reset it, but the
# nightly Notion query is the durable source of truth.
# Kept module-level because health tracking wires in via routes and should not couple to BotState's lifecycle.
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
    if "write_lock" not in _steps_state[date_str]:
        _steps_state[date_str]["write_lock"] = asyncio.Lock()
    return _steps_state[date_str]



def _yesterday(tz) -> str:
    """Return yesterday's date string in the app timezone."""
    return (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Notion helpers ────────────────────────────────────────────────────────────

def _threshold_state_key(date_str: str) -> str:
    return f"steps_threshold_{date_str}"


def _persist_threshold_state(notion, env_db_id: str, date_str: str, message_id: int) -> None:
    """Persist today's threshold Telegram message id in the ENV database."""
    if not env_db_id:
        return
    key = _threshold_state_key(date_str)
    props = {
        "Name": title_prop(key),
        "Value": rich_text_prop(str(message_id)),
    }
    try:
        results = notion.databases.query(
            database_id=env_db_id,
            filter={"property": "Name", "title": {"equals": key}},
        )
        pages = results.get("results", [])
        if pages:
            notion.pages.update(page_id=pages[0]["id"], properties={"Value": props["Value"]})
        else:
            notion.pages.create(parent={"database_id": env_db_id}, properties=props)
    except Exception as e:
        log.warning("steps: failed to persist threshold message id for %s: %s", date_str, e)


def _load_threshold_state(notion, env_db_id: str, date_str: str) -> int | None:
    """Load a persisted threshold Telegram message id from the ENV database."""
    if not env_db_id:
        return None
    key = _threshold_state_key(date_str)
    try:
        results = notion.databases.query(
            database_id=env_db_id,
            filter={"property": "Name", "title": {"equals": key}},
        )
        pages = results.get("results", [])
        if not pages:
            return None
        chunks = pages[0].get("properties", {}).get("Value", {}).get("rich_text", [])
        if not chunks:
            return None
        raw = chunks[0].get("plain_text") or chunks[0].get("text", {}).get("content")
        return int(raw) if raw else None
    except Exception as e:
        log.warning("steps: failed to load threshold message id for %s: %s", date_str, e)
        return None

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


def _find_existing_log_entry(notion, log_db_id: str, habit_page_id: str, date_str: str) -> list[str]:
    """
    Return all Notion page_ids for existing Habit Log entries for this habit+date.
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
        return [p["id"] for p in pages if p.get("id")]
    except Exception as e:
        log.error("steps: error querying existing log entry for %s: %s", date_str, e)
        return []


def _normalise_existing_log_ids(value) -> list[str]:
    """Accept legacy test mocks that still return a single page id."""
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value if v]


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
                "Entry": title_prop("Steps"),
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
        properties = current_page.get("properties", {})
        current_completed = (
            properties
            .get("Completed", {})
            .get("checkbox", False)
        )
        final_completed = current_completed or completed

        current_steps = properties.get("Steps Count", {}).get("number") or 0
        if not isinstance(current_steps, (int, float)):
            current_steps = 0
        final_steps = max(steps, current_steps)

        update_properties = {
            "Steps Count": {"number": final_steps},
        }
        if "Completed" in properties:
            update_properties["Completed"] = {"checkbox": final_completed}

        notion.pages.update(
            page_id=page_id,
            properties=update_properties,
        )

        # Best-effort sync for databases that surface a Status column ("OPEN"/"DONE")
        # separate from the Completed checkbox used by dashboards.
        if "Status" in properties:
            try:
                notion.pages.update(
                    page_id=page_id,
                    properties={
                        "Status": {
                            "status": {
                                "name": "Done" if final_completed else "Open",
                            }
                        },
                    },
                )
            except Exception as status_err:
                log.debug("steps: Status field update skipped for %s: %s", page_id, status_err)

        log.info(
            "steps: updated log entry %s — %d steps (was %d), completed=%s (was %s)",
            page_id,
            final_steps,
            current_steps,
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
        pages = query_all(
            notion,
            log_db_id,
            filter={
                "property": "Habit",
                "relation": {"contains": habit_page_id},
            },
            page_size=None,
        )

        for page in pages:
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
                        "Entry": title_prop("Steps"),
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

        log.info("steps: migration complete — renamed=%d skipped=%d", renamed, skipped)
        return {"renamed": renamed, "skipped": skipped}
    except Exception as e:
        log.error("steps: migration failed: %s", e)
        return {"error": str(e)}


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
    env_db_id: str = "",
    bot=None,
    chat_id: int | None = None,
    write_intraday_below_threshold: bool = False,
    force_write: bool = False,
) -> dict:
    """
    Process a steps sync payload from Health Auto Export.

    Called both by:
      - The HTTP webhook handler (real-time hourly syncs)
      - The nightly scheduled job (via handle_steps_final_stamp)

    Returns a result dict for logging/debugging:
      {action: "notified"|"updated"|"created"|"skipped", steps, date}
    """
    today = _local_today(tz)
    yesterday = _yesterday(tz)
    is_today = (date_str == today)
    is_yesterday = (date_str == yesterday)

    state = _date_state(date_str)
    state["last_steps"] = max(state["last_steps"], steps)

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

    # ── Threshold notification (send once, then edit with updated count) ──
    if is_today and completed and bot and chat_id:
        notification_text = f"🎉 10,000 steps hit! 🚶 {steps:,} steps today"
        if not state["threshold_notified"] and not state.get("threshold_message_id") and env_db_id:
            message_id = _load_threshold_state(notion, env_db_id, date_str)
            if message_id:
                state["threshold_notified"] = True
                state["threshold_message_id"] = message_id

        if not state["threshold_notified"]:
            try:
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=notification_text,
                )
                state["threshold_notified"] = True
                state["threshold_message_id"] = sent.message_id
                _persist_threshold_state(notion, env_db_id, date_str, sent.message_id)
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
                err_str = str(e).lower()
                if "message is not modified" in err_str:
                    # Text unchanged — not an error, step count matches previous sync
                    log.debug("steps: edit skipped, message already up to date")
                elif "message to edit not found" in err_str or "message_id_invalid" in err_str:
                    # Original message was deleted; send a fresh one and persist the new id
                    log.warning("steps: original message gone (%s), sending replacement", e)
                    try:
                        sent = await bot.send_message(chat_id=chat_id, text=notification_text)
                        state["threshold_message_id"] = sent.message_id
                        state["threshold_notified"] = True
                        _persist_threshold_state(notion, env_db_id, date_str, sent.message_id)
                        log.info("steps: replacement message sent (msg_id=%s)", sent.message_id)
                    except Exception as e2:
                        log.error("steps: replacement send also failed: %s", e2)
                else:
                    # Transient Telegram error — log and retry on next sync, don't spam
                    log.warning("steps: edit failed, will retry next sync: %s", e)

    # ── Intraday sub-threshold behavior (configurable) ──
    # Legacy mode only cached intraday counts until threshold/nightly stamp.
    if (
        is_today
        and not completed
        and not write_intraday_below_threshold
        and not force_write
    ):
        log.debug("steps: sub-threshold intraday sync (%d), caching only", steps)
        return _sync_result("skipped", reason="sub_threshold_intraday")

    if force_write and not completed:
        log.info(
            "steps: force_write=True — writing %d steps for %s despite sub-threshold",
            steps,
            date_str,
        )

    # ── For today above threshold, or any yesterday sync: upsert Notion entry ──
    if not (is_today or is_yesterday):
        log.info("steps: received data for %s (not today/yesterday), skipping", date_str)
        return _sync_result("skipped", reason="old_date")

    # Resolve habit page_id (check memory first)
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        return _sync_result("error", reason="habit_not_found")

    async with state["write_lock"]:
        existing_page_ids = _normalise_existing_log_ids(
            _find_existing_log_entry(notion, log_db_id, habit_page_id, date_str)
        )
        if not existing_page_ids and state.get("notion_page_id"):
            existing_page_ids = [state["notion_page_id"]]

        if existing_page_ids:
            existing_page_id = existing_page_ids[0]
            for dup_id in existing_page_ids[1:]:
                try:
                    notion.pages.update(page_id=dup_id, archived=True)
                except Exception as e:
                    log.warning("steps: failed to archive duplicate log entry %s for %s: %s", dup_id, date_str, e)
            if len(existing_page_ids) > 1:
                log.warning(
                    "steps: found %d duplicate log entries for %s; kept %s and archived %s",
                    len(existing_page_ids),
                    date_str,
                    existing_page_id,
                    existing_page_ids[1:],
                )
            # UPDATE existing entry (preserve Completed if already True — don't downgrade)
            if not _update_log_entry_steps(notion, existing_page_id, steps, completed):
                return _sync_result("error", reason="notion_update_failed", page_id=existing_page_id)
            state["notion_page_id"] = existing_page_id
            return _sync_result("updated", page_id=existing_page_id)

        # CREATE new entry
        new_page_id = _create_log_entry(
            notion, log_db_id, habit_page_id, date_str, steps, completed, source_label
        )
        if new_page_id:
            state["notion_page_id"] = new_page_id
            return _sync_result("created", page_id=new_page_id)
        return _sync_result("error", reason="notion_create_failed", page_id=None)


async def handle_steps_final_stamp(
    *,
    notion,
    habit_db_id: str,
    log_db_id: str,
    habit_name: str,
    threshold: int,
    source_label: str,
    tz,
    env_db_id: str = "",
    bot=None,
    chat_id: int | None = None,
) -> dict:
    """
    Stamp final daily step count. Called by both:
      - steps_morning_stamp (08:00) — first attempt
      - steps_final_stamp (12:00) — second attempt / override

    Uses last received step count from in-memory state.
    Always writes regardless of threshold (force_write=True).
    Skips today if steps = 0 (day just started, no data yet).
    Silent — no Telegram notification.
    """
    results = {}
    today = _local_today(tz)
    yesterday = _yesterday(tz)

    for date_str in (today, yesterday):
        state = _steps_state.get(date_str)
        steps = state["last_steps"] if state else 0

        # Skip today if no steps data yet (day just started)
        if date_str == today and steps == 0:
            log.info(
                "steps: stamp — skipping today %s (no data yet, day just started)",
                date_str,
            )
            continue

        # Skip yesterday if no data received at all
        if date_str == yesterday and steps == 0:
            log.info(
                "steps: stamp — no data for yesterday %s, skipping",
                date_str,
            )
            continue

        log.info("steps: stamp for %s — %d steps", date_str, steps)

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
            bot=None,          # Silent — no threshold notification from stamp
            chat_id=None,      # Silent — no threshold notification from stamp
            force_write=True,  # Always write regardless of threshold
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
    env_db_id: str = "",
) -> None:
    """Pre-populate _steps_state from Notion at bot startup.

    Restores the latest known step counts and Notion page IDs, plus today's
    threshold notification message_id from the ENV DB so redeploys edit the
    existing Telegram message instead of sending duplicates.
    """
    habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
    if not habit_page_id:
        log.warning("steps backfill: habit '%s' not found", habit_name)
        return
    today = _local_today(tz)
    yesterday = _yesterday(tz)
    for date_str in (today, yesterday):
        try:
            existing_ids = _normalise_existing_log_ids(
                _find_existing_log_entry(notion, log_db_id, habit_page_id, date_str)
            )
            existing_id = existing_ids[0] if existing_ids else None
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
            if date_str == today and env_db_id:
                message_id = _load_threshold_state(notion, env_db_id, date_str)
                if message_id:
                    state["threshold_notified"] = True
                    state["threshold_message_id"] = message_id

            log.info("steps backfill: %s → %d steps", date_str, state["last_steps"])
        except Exception as e:
            log.error("steps backfill: error for %s: %s", date_str, e)


def get_steps_state_summary() -> dict:
    """Return a safe summary of current in-memory state for debugging."""
    return {
        date_str: {
            "last_steps": s["last_steps"],
            "threshold_notified": s["threshold_notified"],
            "threshold_message_id": s.get("threshold_message_id"),
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
