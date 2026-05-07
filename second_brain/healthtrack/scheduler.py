"""Utility Scheduler jobs for health tracking."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from second_brain.healthtrack.steps import (
    _find_existing_log_entry,
    _find_steps_habit_page_id,
)

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
                "Entry": {"title": [{"text": {"content": f"Steps — {today_str}"}}]},
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

        if bot and chat_id:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⚠️ Steps entry missing for {today_str}. "
                        "Created placeholder. Check Auto Export."
                    ),
                    parse_mode="HTML",
                )
            except Exception as exc:
                log.error("steps_sync_check: failed to send Telegram alert: %s", exc)

        return {
            "ok": True,
            "action": "created",
            "reason": f"Created placeholder entry for {today_str}",
            "page_id": page_id,
        }
    except Exception as exc:
        log.error("steps_sync_check: unexpected error: %s", exc)
        return {"ok": False, "action": "error", "reason": str(exc)}
