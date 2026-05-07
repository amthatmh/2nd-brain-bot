"""Telegram delivery for operational alerts."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

ALERT_EMOJIS = {
    "DEPLOY": "🚀",
    "INFO": "ℹ️",
    "WARN": "⚠️",
    "ERROR": "🚨",
    "CRITICAL": "🔥",
    "METRICS": "📊",
}
_COOLDOWNS: dict[str, datetime] = {}


def _alert_channel_id() -> str:
    """Return the configured alert destination without falling back to owner DMs."""
    return (
        os.environ.get("ALERT_CHANNEL_ID", "").strip()
        or os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()
    )


def _check_cooldown(cooldown_key: str, cooldown_hours: int) -> bool:
    last_alert = _COOLDOWNS.get(cooldown_key)
    if last_alert is None:
        return True
    return datetime.now(timezone.utc) - last_alert >= timedelta(hours=cooldown_hours)


def _set_cooldown(cooldown_key: str) -> None:
    _COOLDOWNS[cooldown_key] = datetime.now(timezone.utc)


def send_alert(
    message: str,
    level: str = "INFO",
    cooldown_key: Optional[str] = None,
    cooldown_hours: int = 24,
) -> bool:
    """
    Send alert to the configured channel with optional rate limiting.

    ALERT_CHANNEL_ID/TELEGRAM_ALERT_CHAT_ID are the only alert destinations;
    TELEGRAM_CHAT_ID is intentionally not used as a fallback to avoid routing
    operational alerts to the bot owner's personal DM.
    """
    logger = logging.getLogger(__name__)
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    alert_channel_id = _alert_channel_id()
    thread_id = os.environ.get("TELEGRAM_ALERT_THREAD_ID", "").strip()

    # DEBUG: Trace execution
    logger.info("[ALERT_DEBUG] send_alert() called")
    logger.info("[ALERT_DEBUG] ALERT_CHANNEL_ID from env: %s", os.getenv("ALERT_CHANNEL_ID"))
    logger.info("[ALERT_DEBUG] TELEGRAM_ALERT_CHAT_ID from env: %s", os.getenv("TELEGRAM_ALERT_CHAT_ID"))
    logger.info("[ALERT_DEBUG] ALERT_CHANNEL_ID variable: %s", alert_channel_id)
    logger.info("[ALERT_DEBUG] Level: %s, Cooldown key: %s", level, cooldown_key)
    logger.info("[ALERT_DEBUG] Message preview: %s...", message[:100])

    if not token:
        logger.error("[ALERT_DEBUG] TELEGRAM_TOKEN is None/empty - SKIPPING")
        return False
    if not alert_channel_id:
        logger.error("[ALERT_DEBUG] ALERT_CHANNEL_ID is None/empty - SKIPPING")
        return False

    if cooldown_key and not _check_cooldown(cooldown_key, cooldown_hours):
        logger.info("[ALERT_DEBUG] Skipped due to cooldown: %s", cooldown_key)
        return False

    if level == "DEPLOY":
        header = ""
    else:
        emoji = ALERT_EMOJIS.get(level, "ℹ️")
        header = f"{emoji} {level}\n\n"

    footer = ""
    if cooldown_key:
        next_alert = datetime.now() + timedelta(hours=cooldown_hours)
        footer = f"\n\n_Cooldown: {cooldown_hours}h (next alert: {next_alert.strftime('%b %d, %I:%M %p %Z')})_"

    payload: dict[str, Any] = {
        "chat_id": alert_channel_id,
        "text": f"{header}{message}{footer}",
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if thread_id:
        payload["message_thread_id"] = thread_id

    try:
        logger.info("[ALERT_DEBUG] Attempting bot.send_message to chat_id=%s", alert_channel_id)
        response = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()
        message_id = result.get("result", {}).get("message_id", "unknown")
        logger.info("[ALERT_DEBUG] ✅ Successfully sent! Message ID: %s", message_id)
        if cooldown_key:
            _set_cooldown(cooldown_key)
        return True
    except Exception as exc:  # noqa: BLE001 - alerts must never crash callers
        logger.error("[ALERT_DEBUG] ❌ FAILED to send alert: %s: %s", type(exc).__name__, exc)
        logger.error("[ALERT_DEBUG] Stack trace:", exc_info=True)
        return False
