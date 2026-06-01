"""Utility Scheduler jobs for health tracking."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
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


def _current_monday_str(tz) -> str:
    """Return the Monday of the current ISO week as YYYY-MM-DD."""
    tz_obj = ZoneInfo(tz) if isinstance(tz, str) else tz
    today = datetime.now(tz_obj).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()


def _already_logged_on_date(notion, log_db_id: str, habit_page_id: str, date_str: str) -> bool:
    """Return whether a completed habit log already exists on a specific date."""
    try:
        pages = query_all(notion, log_db_id, filter={
            "and": [
                {"property": "Habit", "relation": {"contains": habit_page_id}},
                {"property": "Completed", "checkbox": {"equals": True}},
                {"property": "Date", "date": {"equals": date_str[:10]}},
            ]
        })
        return len(pages) > 0
    except Exception as exc:
        log.warning(
            "already_logged_on_date query failed for %s on %s: %s",
            habit_page_id,
            date_str,
            exc,
        )
        return True


def _log_habit_on_date(notion, log_db_id: str, habit_page_id: str, habit_name: str, date_str: str) -> None:
    """Create a completed habit log entry on a specific date."""
    props = {
        "Entry": title_prop(habit_name),
        "Habit": {"relation": [{"id": habit_page_id}]},
        "Completed": {"checkbox": True},
        "Date": {"date": {"start": date_str[:10]}},
        "Source": {"select": {"name": "Scheduler"}},
    }
    try:
        notion.pages.create(
            parent={"database_id": log_db_id},
            properties=props,
        )
    except Exception as exc:
        log.warning(
            "Habit log create retrying without Source for %s on %s: %s",
            habit_name,
            date_str,
            exc,
        )
        minimal = {key: value for key, value in props.items() if key != "Source"}
        notion.pages.create(
            parent={"database_id": log_db_id},
            properties=minimal,
        )
    log.info("Habit logged: %s on %s via Scheduler", habit_name, date_str[:10])


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


async def weigh_sync_job(notion, log_db_id, health_metrics_db_id, habit_cache, tz) -> dict:
    """Check if Weight (kg) was recorded this ISO week; log Weigh habit if not yet logged."""
    from second_brain.notion.habits import get_week_completion_count

    weigh_habit = next(
        (h for h in habit_cache.values() if "weigh" in h.get("name", "").lower() and h.get("auto_only")),
        None,
    )
    if not weigh_habit:
        return {"status": "skipped", "reason": "Weigh habit not found in cache"}

    if get_week_completion_count(notion, log_db_id, weigh_habit["page_id"], tz) > 0:
        return {"status": "skipped", "reason": "already logged this week"}

    monday = _current_monday_str(tz)
    try:
        rows = query_all(notion, health_metrics_db_id, filter={
            "and": [
                {"property": "Date", "date": {"on_or_after": monday}},
                {"property": "Weight (kg)", "number": {"is_not_empty": True}},
            ]
        })
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}

    if not rows:
        return {"status": "skipped", "reason": "no weight measurement this week"}

    props = rows[0]["properties"]
    date_val = (props.get("Date") or {}).get("date") or {}
    date_str = date_val.get("start")
    weight = (props.get("Weight (kg)") or {}).get("number")

    if not date_str:
        return {"status": "skipped", "reason": "date missing from health metrics row"}

    try:
        _log_habit_on_date(notion, log_db_id, weigh_habit["page_id"], weigh_habit["name"], date_str)
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}

    log.info("weigh_sync_job: logged for %s (%.1f kg)", date_str[:10], weight or 0)
    return {"status": "logged", "date": date_str[:10], "weight_kg": weight}


async def weigh_backfill_job(notion, log_db_id, health_metrics_db_id, habit_cache, tz) -> dict:
    """One-time backfill: one Habits Log entry per ISO week where Weight (kg) was recorded."""
    from datetime import date as date_cls

    weigh_habit = next(
        (h for h in habit_cache.values() if "weigh" in h.get("name", "").lower() and h.get("auto_only")),
        None,
    )
    if not weigh_habit:
        return {"status": "skipped", "reason": "Weigh habit not found in cache"}

    try:
        rows = query_all(notion, health_metrics_db_id, filter={
            "property": "Weight (kg)", "number": {"is_not_empty": True}
        })
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}

    weeks: dict[str, str] = {}
    for row in rows:
        try:
            props = row["properties"]
            date_val = (props.get("Date") or {}).get("date") or {}
            date_str = date_val.get("start")
            if not date_str:
                continue
            normalized_date = date_str[:10]
            d = date_cls.fromisoformat(normalized_date)
            iso = d.isocalendar()
            week_key = f"{iso.year}-W{iso.week:02d}"
            if week_key not in weeks or normalized_date < weeks[week_key]:
                weeks[week_key] = normalized_date
        except Exception as exc:
            log.warning("weigh_backfill_job row parse error: %s", exc)

    logged = 0
    skipped = 0
    for date_str in weeks.values():
        try:
            if _already_logged_on_date(notion, log_db_id, weigh_habit["page_id"], date_str):
                skipped += 1
                continue
            _log_habit_on_date(notion, log_db_id, weigh_habit["page_id"], weigh_habit["name"], date_str)
            logged += 1
        except Exception as exc:
            log.warning("weigh_backfill_job log error for %s: %s", date_str, exc)
            skipped += 1

    log.info("weigh_backfill_job complete: logged=%d skipped=%d", logged, skipped)
    return {"status": "done", "logged": logged, "skipped": skipped}


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
    manager.register_handler("weekly_health_insight", handle_weekly_health_insight_job)
    log.info(
        "healthtrack: registered scheduler handlers "
        "(steps_sync_check, steps_final_stamp, steps_morning_stamp, sleep_sync, sleep_resync, "
        "weekly_health_insight)"
    )
