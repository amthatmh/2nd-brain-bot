"""Operational alert helpers for Second Brain.

The alert_* functions intentionally only format messages and delegate delivery to
send_alert so callers do not need to know Telegram delivery details.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from utils.alerts import send_alert

log = logging.getLogger(__name__)


def _truncate(value: Any, limit: int = 1000) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def alert_startup(version: str, commit: str, status: str = "ok") -> bool:
    """Send deployment alert on startup."""
    logger = logging.getLogger(__name__)
    logger.info("[ALERT_HANDLER] alert_startup() called")

    message = (
        "*Deployment*\n\n"
        f"Status: {status}\n"
        f"Version: {version}\n"
        f"Commit: {commit}\n"
        f"Time: {datetime.now().strftime('%b %d, %I:%M %p %Z')}\n\n"
        "All systems operational ✓"
    )

    result = send_alert(message, level="DEPLOY")
    logger.info("[ALERT_HANDLER] alert_startup() send_alert returned: %s", result)
    return result


def alert_notion_auth_failure(error: str) -> bool:
    return send_alert("*Notion auth failure*\n" f"Error: `{_truncate(error)}`", level="ERROR")


def alert_claude_auth_failure(error: str) -> bool:
    return send_alert("*Claude API/auth failure*\n" f"Error: `{_truncate(error)}`", level="ERROR")


def alert_cinema_sync_complete(
    synced_count: int,
    duplicates_skipped: int,
    duration: float,
    next_run: datetime | str | None = None,
) -> bool:
    """Alert on cinema sync completion."""
    logger = logging.getLogger(__name__)
    logger.info("[ALERT_HANDLER] alert_cinema_sync_complete() called")

    if isinstance(next_run, datetime):
        next_run_text = next_run.strftime("%b %d, %I:%M %p %Z")
    else:
        next_run_text = next_run or "not scheduled"

    message = (
        "*Cinema Sync Completed*\n\n"
        f"✓ {synced_count} new favourites synced\n"
        f"✓ Duplicate guard: {duplicates_skipped} skipped\n\n"
        f"Duration: {duration:.1f}s\n"
        f"Next sync: {next_run_text}"
    )

    result = send_alert(message, level="INFO")
    logger.info("[ALERT_HANDLER] alert_cinema_sync_complete() send_alert returned: %s", result)
    return result


def alert_digest_sent(slot_name: str) -> bool:
    return send_alert("*Digest sent*\n" f"Slot: `{_truncate(slot_name, 120)}`", level="INFO")


def alert_scheduler_event(job_id: str, event_type: str, error: str | None = None) -> bool:
    lines = [
        "*Scheduler event*",
        f"Job: `{_truncate(job_id, 120)}`",
        f"Type: `{_truncate(event_type, 80)}`",
    ]
    if error:
        lines.append(f"Error: `{_truncate(error)}`")
    return send_alert("\n".join(lines), level="WARN")


def alert_weekly_summary(summary: str) -> bool:
    return send_alert("*Weekly summary*\n" f"{_truncate(summary, 3000)}", level="INFO")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
