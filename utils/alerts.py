"""Telegram delivery for operational alerts."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx

ALERT_EMOJIS = {
    "DEPLOY": "🚀",
    "INFO": "ℹ️",
    "WARN": "⚠️",
    "WARNING": "⚠️",
    "ERROR": "🚨",
    "CRITICAL": "🔥",
}


def _alert_channel_id() -> str:
    """Return the configured alert destination without falling back to owner DMs."""
    return (
        os.environ.get("ALERT_CHANNEL_ID", "").strip()
        or os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()
    )


def send_alert(message: str, level: str = "INFO", cooldown_key: Optional[str] = None) -> bool:
    """
    Send alert to System logs channel.

    Args:
        message: Alert message content
        level: Alert level (CRITICAL, WARNING, INFO, METRICS, DEPLOY)
        cooldown_key: Optional cooldown key (6h default cooldown if provided)

    Returns:
        True if sent successfully, False if skipped due to cooldown
    """
    from second_brain.monitoring import check_alert_cooldown, set_alert_cooldown

    logger = logging.getLogger(__name__)
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    alert_channel_id = _alert_channel_id()
    thread_id = os.environ.get("TELEGRAM_ALERT_THREAD_ID", "").strip()
    cooldown_hours = 6

    # DEBUG: Trace execution
    logger.info("[ALERT_DEBUG] send_alert() called")
    logger.info("[ALERT_DEBUG] ALERT_CHANNEL_ID from env: %s", os.getenv("ALERT_CHANNEL_ID"))
    logger.info("[ALERT_DEBUG] TELEGRAM_ALERT_CHAT_ID from env: %s", os.getenv("TELEGRAM_ALERT_CHAT_ID"))
    logger.info("[ALERT_DEBUG] ALERT_CHANNEL_ID variable: %s", alert_channel_id)
    logger.info("[ALERT_DEBUG] Level: %s, Cooldown key: %s", level, cooldown_key)
    logger.info("[ALERT_DEBUG] Message preview: %s...", message[:100])

    if cooldown_key and level != "CRITICAL":
        if not check_alert_cooldown(cooldown_key, cooldown_hours=cooldown_hours):
            logger.info("[ALERT_DEBUG] Skipping alert due to cooldown: %s", cooldown_key)
            return False

    if not token:
        logger.error("[ALERT_DEBUG] TELEGRAM_TOKEN is None/empty - SKIPPING")
        return False
    if not alert_channel_id:
        logger.error("[ALERT_DEBUG] ALERT_CHANNEL_ID is None/empty - SKIPPING")
        return False

    if level == "DEPLOY":
        header = ""
    else:
        emoji = ALERT_EMOJIS.get(level, "ℹ️")
        header = f"{emoji} {level}\n\n"

    footer = ""
    if cooldown_key and level != "CRITICAL":
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
        if cooldown_key and level != "CRITICAL":
            set_alert_cooldown(cooldown_key)
        return True
    except Exception as exc:  # noqa: BLE001 - alerts must never crash callers
        logger.error("[ALERT_DEBUG] ❌ FAILED to send alert: %s: %s", type(exc).__name__, exc)
        logger.error("[ALERT_DEBUG] Stack trace:", exc_info=True)
        return False
