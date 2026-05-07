"""Admin-only Telegram commands."""

from __future__ import annotations

import os

from telegram import Update
from telegram.ext import ContextTypes

from utils.alert_handlers import send_alert


async def test_alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a test operational alert; requires TELEGRAM_CHAT_ID."""
    telegram_chat_id_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not telegram_chat_id_raw:
        await update.message.reply_text("⚠️ TELEGRAM_CHAT_ID is not configured.")
        return

    try:
        telegram_chat_id = int(telegram_chat_id_raw)
    except ValueError:
        await update.message.reply_text("⚠️ TELEGRAM_CHAT_ID must be an integer Telegram chat ID.")
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id != telegram_chat_id:
        return

    ok = send_alert("🧪 *Test alert*\nOperational alert delivery is configured correctly.")
    await update.message.reply_text("✅ Test alert sent." if ok else "⚠️ Test alert failed. Check logs and env vars.")
