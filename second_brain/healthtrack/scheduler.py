"""Utility Scheduler jobs for health tracking."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from second_brain.healthtrack.steps import (
    _find_existing_log_entry,
    _find_steps_habit_page_id,
)
from second_brain.error_reporting import send_system_log
from second_brain.notion.properties import query_all, title_prop

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


def _today_str(tz: str | Any) -> str:
    """Return today's local date as YYYY-MM-DD for a timezone name/object."""
    tz_obj = ZoneInfo(tz) if isinstance(tz, str) else tz
    return datetime.now(tz_obj).date().isoformat()


async def check_and_create_steps_entry(
    notion,
    habit_db_id: str,
    habit_name: str,
    tz: str | Any,
    chat_id: str | int | None = None,
    bot=None,
    log_db_id: str | None = None,
) -> dict:
    """
    Utility Scheduler job: ensure a Steps entry exists for today.

    Runs every 60 minutes. If today's Steps entry is missing from Habits Log,
    creates a placeholder entry with a blank/null Steps Count.

    Args:
        notion: NotionClient instance.
        habit_db_id: Notion Habits database ID used to resolve the Steps habit.
        habit_name: Name of the Steps habit (for example, "Steps").
        tz: Timezone string or timezone object (for example, "America/Chicago").
        chat_id: Telegram chat ID for alerts (optional).
        bot: Telegram bot instance (optional).
        log_db_id: Notion Habits Log database ID. If omitted, ``habit_db_id`` is
            treated as the log database ID for backward compatibility with the
            original scheduler signature.

    Returns:
        dict: {"ok": bool, "action": "exists"|"created"|"error", "reason": str}
    """
    try:
        today_str = _today_str(tz)
        target_log_db_id = log_db_id or habit_db_id

        log.info("steps_sync_check: checking for Steps entry on %s", today_str)

        habit_page_id = _find_steps_habit_page_id(notion, habit_db_id, habit_name)
        if not habit_page_id:
            reason = f"Steps habit not found: {habit_name}"
            log.error("steps_sync_check: %s", reason)
            await send_system_log(bot, f"🚨 Steps sync check failed\n{reason}")
            return {"ok": False, "action": "error", "reason": reason}

        existing_page_id = _find_existing_log_entry(
            notion,
            target_log_db_id,
            habit_page_id,
            today_str,
        )
        if existing_page_id:
            steps_count = None
            try:
                entry = notion.pages.retrieve(page_id=existing_page_id)
                steps_count = (
                    entry.get("properties", {})
                    .get("Steps Count", {})
                    .get("number")
                )
            except Exception as exc:
                log.warning(
                    "steps_sync_check: found entry %s but could not read Steps Count: %s",
                    existing_page_id,
                    exc,
                )
            log.info(
                "steps_sync_check: Steps entry found for %s (page_id: %s, count: %s)",
                today_str,
                existing_page_id,
                steps_count,
            )
            return {
                "ok": True,
                "action": "exists",
                "reason": f"Entry exists for {today_str}",
                "page_id": existing_page_id,
                "steps_count": steps_count,
            }

        log.warning(
            "steps_sync_check: Steps entry missing for %s, creating placeholder",
            today_str,
        )
        new_entry = notion.pages.create(
            parent={"database_id": target_log_db_id},
            properties={
                "Entry": title_prop("Steps"),
                "Habit": {"relation": [{"id": habit_page_id}]},
                "Date": {"date": {"start": today_str}},
                "Source": {"select": {"name": "Scheduler"}},
            },
        )
        page_id = new_entry["id"]
        log.info(
            "steps_sync_check: created placeholder Steps entry for %s (page_id: %s)",
            today_str,
            page_id,
        )

        log.warning(
            "steps_sync_check: placeholder created for %s — "
            "Auto Export has not synced yet. Will correct when data arrives.",
            today_str,
        )

        return {
            "ok": True,
            "action": "created",
            "reason": f"Created placeholder entry for {today_str}",
            "page_id": page_id,
        }
    except Exception as exc:
        log.error("steps_sync_check: unexpected error: %s", exc)
        await send_system_log(bot, f"🚨 Steps sync check failed\n{type(exc).__name__}: {exc}")
        return {"ok": False, "action": "error", "reason": str(exc)}


def _already_logged_on_date(notion, log_db_id: str, habit_page_id: str, date_str: str) -> bool:
    """Like already_logged_today but for an explicit date string."""
    try:
        pages = query_all(notion, log_db_id, filter={
            "and": [
                {"property": "Habit", "relation": {"contains": habit_page_id}},
                {"property": "Completed", "checkbox": {"equals": True}},
                {"property": "Date", "date": {"equals": date_str}},
            ]
        })
        return len(pages) > 0
    except Exception as e:
        log.warning("_already_logged_on_date failed for %s on %s: %s", habit_page_id, date_str, e)
        return False


def _log_habit_on_date(notion, log_db_id: str, habit_page_id: str, habit_name: str, date_str: str) -> None:
    """Like log_habit but writes a specific historical date instead of today."""
    props = {
        "Entry": title_prop(habit_name),
        "Habit": {"relation": [{"id": habit_page_id}]},
        "Completed": {"checkbox": True},
        "Date": {"date": {"start": date_str}},
        "Source": {"select": {"name": "🛌 Auto"}},
    }
    try:
        notion.pages.create(parent={"database_id": log_db_id}, properties=props)
    except Exception as e:
        log.warning("_log_habit_on_date retrying without Source for %s: %s", date_str, e)
        minimal = {k: v for k, v in props.items() if k != "Source"}
        notion.pages.create(parent={"database_id": log_db_id}, properties=minimal)
    log.info("Backfilled Sleep habit for %s", date_str)


async def sleep_backfill_job(notion, log_db_id, health_metrics_db_id, habit_cache, tz) -> dict:
    """One-time backfill: create Habits Log entries for all past dates where Time in Bed >= goal."""
    del tz
    try:
        from second_brain.healthtrack.config import SLEEP_GOAL_HOURS

        sleep_habit = next(
            (
                h for h in habit_cache.values()
                if "sleep" in h.get("name", "").lower() and h.get("auto_only")
            ),
            None,
        )
        if not sleep_habit:
            return {"status": "skipped", "reason": "Sleep habit not found in cache"}

        try:
            rows = query_all(notion, health_metrics_db_id)
        except Exception as e:
            return {"status": "error", "reason": str(e)}

        logged = 0
        skipped = 0
        for row in rows:
            try:
                props = row["properties"]
                date_val = (props.get("Date") or {}).get("date") or {}
                date_str = date_val.get("start")
                if not date_str:
                    skipped += 1
                    continue
                time_in_bed = (props.get("Time in Bed hrs") or {}).get("number")
                if time_in_bed is None or time_in_bed < SLEEP_GOAL_HOURS:
                    skipped += 1
                    continue
                if _already_logged_on_date(notion, log_db_id, sleep_habit["page_id"], date_str):
                    skipped += 1
                    continue
                _log_habit_on_date(notion, log_db_id, sleep_habit["page_id"], sleep_habit["name"], date_str)
                logged += 1
            except Exception as e:
                log.warning("sleep_backfill_job row error: %s", e)
                skipped += 1

        log.info("sleep_backfill_job complete: logged=%d skipped=%d", logged, skipped)
        return {"status": "done", "logged": logged, "skipped": skipped}
    except Exception as e:
        log.warning("sleep_backfill_job failed: %s", e)
        return {"status": "error", "reason": str(e)}


async def handle_sleep_backfill_job(bot=None) -> dict:
    """Utility Scheduler manager wrapper for the one-time Sleep habit backfill."""
    del bot
    from second_brain.main import NOTION_HEALTH_METRICS_DB, NOTION_LOG_DB, TZ, habit_cache, notion

    return await sleep_backfill_job(
        notion,
        NOTION_LOG_DB,
        NOTION_HEALTH_METRICS_DB,
        habit_cache,
        TZ,
    )


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register health tracking jobs with the Utility Scheduler Manager."""
    from second_brain.healthtrack.insights import handle_weekly_health_insight_job
    from second_brain.healthtrack.sleep import handle_sleep_resync_job, handle_sleep_sync_job
    from second_brain.healthtrack.steps import (
        handle_steps_final_stamp_job,
        handle_steps_sync_check,
    )

    manager.register_handler("steps_sync_check", handle_steps_sync_check)
    manager.register_handler("steps_final_stamp", handle_steps_final_stamp_job)
    manager.register_handler("steps_morning_stamp", handle_steps_final_stamp_job)
    manager.register_handler("sleep_sync", handle_sleep_sync_job)
    manager.register_handler("sleep_resync", handle_sleep_resync_job)
    manager.register_handler("sleep_backfill", handle_sleep_backfill_job)
    manager.register_handler("weekly_health_insight", handle_weekly_health_insight_job)
    log.info(
        "healthtrack: registered scheduler handlers "
        "(steps_sync_check, steps_final_stamp, steps_morning_stamp, sleep_sync, sleep_resync, "
        "sleep_backfill, weekly_health_insight)"
    )
