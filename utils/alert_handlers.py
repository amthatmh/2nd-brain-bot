"""Operational alert helpers for Second Brain.

The alert_* functions intentionally only format messages and delegate delivery to
send_alert so callers do not need to know Telegram delivery details.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)


def _truncate(value: Any, limit: int = 1000) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def send_alert(message: str) -> bool:
    """Send an operational alert to the configured Telegram alert chat."""
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_ALERT_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID", "")
    thread_id = os.environ.get("TELEGRAM_ALERT_THREAD_ID", "").strip()
    if not token or not chat_id:
        log.warning("send_alert skipped: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID is not configured")
        return False

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if thread_id:
        payload["message_thread_id"] = thread_id

    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - alerts must never crash callers
        log.error("send_alert failed: %s", exc)
        return False


def alert_startup(version: str, sha: str, status: str = "ok") -> bool:
    return send_alert(
        "✅ *Second Brain bot started*\n"
        f"Status: `{status}`\n"
        f"Version: `{version}`\n"
        f"SHA: `{sha}`"
    )


def alert_notion_auth_failure(error: str) -> bool:
    return send_alert("🚨 *Notion auth failure*\n" f"Error: `{_truncate(error)}`")


def alert_claude_auth_failure(error: str) -> bool:
    return send_alert("🚨 *Claude API/auth failure*\n" f"Error: `{_truncate(error)}`")


def alert_cinema_sync_complete(synced_count: int, duplicates: int, duration: float, next_run: str | None = None) -> bool:
    lines = [
        "🎬 *Cinema sync complete*",
        f"Synced: `{synced_count}`",
        f"Duplicates/skipped: `{duplicates}`",
        f"Duration: `{duration:.1f}s`",
    ]
    if next_run:
        lines.append(f"Next run: `{next_run}`")
    return send_alert("\n".join(lines))


def alert_digest_sent(slot_name: str) -> bool:
    return send_alert("📰 *Digest sent*\n" f"Slot: `{_truncate(slot_name, 120)}`")


def alert_scheduler_event(job_id: str, event_type: str, error: str | None = None) -> bool:
    lines = [
        "⚠️ *Scheduler event*",
        f"Job: `{_truncate(job_id, 120)}`",
        f"Type: `{_truncate(event_type, 80)}`",
    ]
    if error:
        lines.append(f"Error: `{_truncate(error)}`")
    return send_alert("\n".join(lines))


def alert_weekly_summary(summary: str) -> bool:
    return send_alert("📊 *Weekly summary*\n" f"{_truncate(summary, 3000)}")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
