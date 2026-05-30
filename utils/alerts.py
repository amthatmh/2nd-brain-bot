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
ERROR_ALERT_LEVELS = {"WARN", "WARNING", "ERROR", "CRITICAL"}


def _redact_token(value: str, token: str) -> str:
    if token:
        value = value.replace(token, "<telegram-token>")
    return value


def _response_preview(response: httpx.Response | None, token: str) -> str:
    if response is None:
        return ""
    try:
        body = response.text[:500]
    except Exception:
        body = ""
    return _redact_token(body, token)


def _error_channel_id() -> str:
    return (
        os.getenv("error_channel_ID")
        or os.getenv("ERROR_CHANNEL_ID")
        or ""
    ).strip()


def _log_channel_id() -> str:
    return (
        os.getenv("ALERT_CHANNEL_ID")
        or os.getenv("SYSTEM_LOGS_CHAT_ID")
        or ""
    ).strip()


def _alert_channel_id(level: str) -> str:
    """Return the alert destination without falling back to owner DMs."""
    normalized = str(level or "").strip().upper()
    if normalized in ERROR_ALERT_LEVELS:
        return _error_channel_id() or _log_channel_id()
    return _log_channel_id() or _error_channel_id()


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
    alert_channel_id = _alert_channel_id(level)
    thread_id = os.environ.get("TELEGRAM_ALERT_THREAD_ID", "").strip()
    cooldown_hours = 6

    # DEBUG: Trace execution
    logger.info("[ALERT_DEBUG] send_alert() called")
    logger.info("[ALERT_DEBUG] error_channel_ID from env: %s", os.getenv("error_channel_ID"))
    logger.info("[ALERT_DEBUG] ALERT_CHANNEL_ID from env: %s", os.getenv("ALERT_CHANNEL_ID"))
    logger.info("[ALERT_DEBUG] resolved alert channel ID: %s", alert_channel_id)
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
        logger.error("[ALERT_DEBUG] alert/error channel ID is None/empty - SKIPPING")
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
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = _response_preview(exc.response, token).lower()
            if exc.response.status_code == 400 and "parse_mode" in payload and (
                "parse" in body or "entity" in body
            ):
                logger.warning("[ALERT_DEBUG] Telegram rejected Markdown; retrying alert as plain text")
                plain_payload = dict(payload)
                plain_payload.pop("parse_mode", None)
                response = httpx.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json=plain_payload,
                    timeout=10,
                )
                response.raise_for_status()
            else:
                raise
        result = response.json()
        message_id = result.get("result", {}).get("message_id", "unknown")
        logger.info("[ALERT_DEBUG] ✅ Successfully sent! Message ID: %s", message_id)
        if cooldown_key and level != "CRITICAL":
            set_alert_cooldown(cooldown_key)
        return True
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = _response_preview(exc.response, token)
        logger.error("[ALERT_DEBUG] ❌ FAILED to send alert: HTTP %s body=%s", status, body)
        logger.debug("[ALERT_DEBUG] Stack trace:", exc_info=True)
        return False
    except Exception as exc:  # noqa: BLE001 - alerts must never crash callers
        logger.error(
            "[ALERT_DEBUG] ❌ FAILED to send alert: %s: %s",
            type(exc).__name__,
            _redact_token(str(exc), token),
        )
        logger.debug("[ALERT_DEBUG] Stack trace:", exc_info=True)
        return False
