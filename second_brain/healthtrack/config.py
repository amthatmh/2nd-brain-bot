"""
Health tracker configuration.

Environment variables:
  HEALTH_STEPS_THRESHOLD   — daily step goal (default: 10000)
  STEPS_HABIT_NAME         — exact name of the Steps habit loaded from Notion ENV DB
                             (default: "Steps")
  HEALTH_STEPS_FINAL_HOUR  — hour for nightly final-stamp job in 24h (default: 23)
  HEALTH_STEPS_FINAL_MIN   — minute for nightly final-stamp job (default: 59)
  STEPS_WEBHOOK_SECRET     — required shared secret for Steps Sync webhook auth
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

STEPS_THRESHOLD: int = int(os.environ.get("HEALTH_STEPS_THRESHOLD", "10000"))
STEPS_HABIT_NAME: str = "Steps"  # Will be loaded from Notion ENV DB at startup
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


async def load_config_from_env_db(notion, env_db_id: str) -> None:
    """
    Load STEPS_HABIT_NAME from Notion ENV DB on startup.

    Args:
        notion: NotionClient instance
        env_db_id: Notion ENV DB ID (355302e9-131d-8031-b4d3-d6ee59aa440f)
    """
    global STEPS_HABIT_NAME

    if not env_db_id:
        log.warning(
            "config: Notion ENV DB ID not configured; using STEPS_HABIT_NAME fallback"
        )
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
                    database_id=env_db_id,
                    filter=notion_filter,
                )
                if results.get("results"):
                    break
            except Exception as e:
                last_error = e
                continue

        if not results and last_error:
            raise last_error

        if results.get("results"):
            # Extract value from the "Value" property (rich_text field).
            value_prop = results["results"][0].get("properties", {}).get("Value", {})
            STEPS_HABIT_NAME = _extract_first_plain_text(value_prop) or "Steps"
        else:
            STEPS_HABIT_NAME = "Steps"  # Fallback if row doesn't exist

        log.info(
            "config: loaded STEPS_HABIT_NAME = %s from Notion ENV DB",
            STEPS_HABIT_NAME,
        )

    except Exception as e:
        log.error(
            "config: failed to load STEPS_HABIT_NAME from Notion ENV DB: %s",
            e,
        )
        # Graceful fallback
        STEPS_HABIT_NAME = "Steps"


_final_h, _final_m = os.environ.get("HEALTH_STEPS_FINAL_TIME", "23:59").split(":")
STEPS_FINAL_HOUR: int = int(_final_h)
STEPS_FINAL_MIN: int = int(_final_m)

WEBHOOK_SECRET: str = os.environ.get("STEPS_WEBHOOK_SECRET", "")

STEPS_WRITE_INTRADAY_BELOW_THRESHOLD: bool = (
    os.environ.get("HEALTH_STEPS_WRITE_INTRADAY", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
