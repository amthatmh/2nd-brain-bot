"""
Health tracker configuration.

Environment variables:
  HEALTH_STEPS_THRESHOLD   — daily step goal (default: 10000)
  STEPS_HABIT_NAME         — exact name of the Steps habit loaded from Notion ENV DB
                             (default: "👟 Steps")
  HEALTH_STEPS_FINAL_HOUR  — hour for nightly final-stamp job in 24h (default: 23)
  HEALTH_STEPS_FINAL_MIN   — minute for nightly final-stamp job (default: 59)
  STEPS_WEBHOOK_SECRET     — required shared secret for Steps Sync webhook auth
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from notion_client import Client

log = logging.getLogger(__name__)

_steps_threshold_default = int(os.environ.get("HEALTH_STEPS_THRESHOLD", "10000"))
STEPS_THRESHOLD: int = _steps_threshold_default

# Default value - will be overridden by Notion ENV DB if available
_steps_habit_name_default = os.environ.get("HEALTH_HABIT_NAME", "👟 Steps")
STEPS_HABIT_NAME: str = _steps_habit_name_default
STEPS_SOURCE_LABEL: str = "📱 Apple Watch"


def _extract_first_plain_text(prop: dict) -> str:
    """Return the first plain_text value from a Notion title/rich_text property."""
    for key in ("rich_text", "title"):
        parts = prop.get(key) or []
        if parts:
            return (
                parts[0].get("plain_text")
                or parts[0].get("text", {}).get("content")
                or ""
            ).strip()
    return ""


def load_steps_config_from_notion_env(
    notion: "Client",
    notion_env_db: str,
    *,
    reset_to_legacy_fallback: bool = False,
) -> None:
    """
    Load STEPS_HABIT_NAME from Notion ENV DB.
    Call this during startup after notion client is initialized.
    Updates the module-level STEPS_HABIT_NAME variable.
    """
    global STEPS_HABIT_NAME

    # TEST: Create STEPS_HABIT_NAME = "👟 Steps" in Notion ENV DB
    # TEST: Verify log shows "config: loaded STEPS_HABIT_NAME = 👟 Steps from Notion ENV DB"
    # TEST: Verify steps webhook can find habit in Habits List
    # TEST: Verify new Steps log entries have Entry = "Steps" (not "Steps — 2026-05-07")
    if not notion_env_db:
        log.warning("STEPS_HABIT_NAME: Notion ENV DB not configured, using default: %s", STEPS_HABIT_NAME)
        if reset_to_legacy_fallback:
            STEPS_HABIT_NAME = "Steps"
        return

    filters = (
        {"property": "Name", "title": {"equals": "STEPS_HABIT_NAME"}},
        {"property": "Name", "rich_text": {"equals": "STEPS_HABIT_NAME"}},
    )

    try:
        results = {}
        last_error: Exception | None = None
        for notion_filter in filters:
            try:
                results = notion.databases.query(
                    database_id=notion_env_db,
                    filter=notion_filter,
                )
                if results.get("results"):
                    break
            except Exception as e:
                last_error = e
                continue

        if not results and last_error:
            raise last_error

        rows = results.get("results", [])
        if not rows:
            if reset_to_legacy_fallback:
                STEPS_HABIT_NAME = "Steps"
            log.info("config: STEPS_HABIT_NAME not found in Notion ENV DB, using default: %s", STEPS_HABIT_NAME)
            return

        props = rows[0]["properties"]
        value = _extract_first_plain_text(props.get("Value", {}))

        if value:
            STEPS_HABIT_NAME = value
            log.info("config: loaded STEPS_HABIT_NAME = %s from Notion ENV DB", value)
        else:
            if reset_to_legacy_fallback:
                STEPS_HABIT_NAME = "Steps"
            log.warning("config: STEPS_HABIT_NAME value empty in Notion ENV, using default: %s", STEPS_HABIT_NAME)
    except Exception as e:
        log.warning("config: failed to load STEPS_HABIT_NAME from Notion ENV DB: %s", e)
        if reset_to_legacy_fallback:
            STEPS_HABIT_NAME = "Steps"


def load_steps_threshold_from_notion_env(notion: "Client", notion_env_db: str) -> None:
    """
    Load STEPS_THRESHOLD from Notion ENV DB.
    Call this during startup after notion client is initialized.
    Updates the module-level STEPS_THRESHOLD variable.
    """
    global STEPS_THRESHOLD

    if not notion_env_db:
        return

    try:
        results = notion.databases.query(
            database_id=notion_env_db,
            filter={"property": "Name", "title": {"equals": "HEALTH_STEPS_THRESHOLD"}},
        )
        rows = results.get("results", [])
        if not rows:
            log.info("config: HEALTH_STEPS_THRESHOLD not found in Notion ENV DB, using default: %d", STEPS_THRESHOLD)
            return

        props = rows[0]["properties"]
        value_str = _extract_first_plain_text(props.get("Value", {}))

        if value_str:
            try:
                STEPS_THRESHOLD = int(value_str)
                log.info("STEPS_THRESHOLD loaded from Notion ENV: %d", STEPS_THRESHOLD)
            except ValueError:
                log.warning(
                    "config: invalid HEALTH_STEPS_THRESHOLD value '%s' in Notion ENV, using default: %d",
                    value_str,
                    STEPS_THRESHOLD,
                )
    except Exception as e:
        log.warning("config: failed to load HEALTH_STEPS_THRESHOLD from Notion ENV DB: %s", e)


async def load_config_from_env_db(notion: "Client", env_db_id: str) -> None:
    """Backward-compatible async wrapper for loading steps config."""
    load_steps_config_from_notion_env(
        notion=notion,
        notion_env_db=env_db_id,
        reset_to_legacy_fallback=True,
    )


_final_h, _final_m = os.environ.get("HEALTH_STEPS_FINAL_TIME", "23:59").split(":")
STEPS_FINAL_HOUR: int = int(_final_h)
STEPS_FINAL_MIN: int = int(_final_m)

WEBHOOK_SECRET: str = os.environ.get("STEPS_WEBHOOK_SECRET", "")

STEPS_WRITE_INTRADAY_BELOW_THRESHOLD: bool = (
    os.environ.get("HEALTH_STEPS_WRITE_INTRADAY", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
