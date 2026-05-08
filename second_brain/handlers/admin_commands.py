"""Admin-only Telegram commands."""

from __future__ import annotations

import logging
import os

from telegram import Bot, Update
from telegram.ext import ContextTypes

from utils.alerts import send_alert

log = logging.getLogger(__name__)


def _admin_chat_id() -> int | None:
    telegram_chat_id_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not telegram_chat_id_raw:
        return None
    try:
        return int(telegram_chat_id_raw)
    except ValueError:
        return None


async def test_alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a test operational alert; requires TELEGRAM_CHAT_ID."""
    telegram_chat_id = _admin_chat_id()
    if telegram_chat_id is None:
        await update.message.reply_text("⚠️ TELEGRAM_CHAT_ID is not configured as an integer.")
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id != telegram_chat_id:
        return

    ok = send_alert("🧪 *Test alert*\nOperational alert delivery is configured correctly.")
    await update.message.reply_text("✅ Test alert sent." if ok else "⚠️ Test alert failed. Check logs and env vars.")


async def test_channel_send(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin command: /testchannelsend
    Directly sends to channel bypassing send_alert() to test permissions.
    """
    user_id = update.effective_user.id if update.effective_user else None
    telegram_chat_id = _admin_chat_id()
    if telegram_chat_id is None:
        await update.message.reply_text("⚠️ TELEGRAM_CHAT_ID is not configured as an integer.")
        return
    if user_id != telegram_chat_id:
        await update.message.reply_text("❌ Unauthorized")
        return

    channel_id = os.getenv("ALERT_CHANNEL_ID")
    bot_token = os.getenv("TELEGRAM_TOKEN")
    token_preview = f"{bot_token[:10]}..." if bot_token else "missing"

    log.info("[TEST_CHANNEL] channel_id=%s, bot_token=%s", channel_id, token_preview)

    if not channel_id:
        await update.message.reply_text("❌ ALERT_CHANNEL_ID is not configured.")
        return
    if not bot_token:
        await update.message.reply_text("❌ TELEGRAM_TOKEN is not configured.")
        return

    await update.message.reply_text(f"Testing direct send to channel ID: {channel_id}")

    try:
        test_bot = Bot(token=bot_token)
        result = await test_bot.send_message(
            chat_id=channel_id,
            text=(
                "🧪 **Direct Channel Test**\n\n"
                "This is a direct send bypassing alert system.\n\n"
                "If you see this, bot CAN post to channel."
            ),
        )
        await update.message.reply_text(f"✅ Success! Message ID: {result.message_id}\n\nCheck System logs channel.")
        log.info("[TEST_CHANNEL] Success - message_id=%s", result.message_id)
    except Exception as e:  # noqa: BLE001 - diagnostic command should report Telegram failures
        await update.message.reply_text(f"❌ Failed: {type(e).__name__}: {e}")
        log.error("[TEST_CHANNEL] Failed: %s", e, exc_info=True)
