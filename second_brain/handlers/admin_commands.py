"""Admin-only Telegram commands."""

from __future__ import annotations

import os

from telegram import Update
from telegram.ext import ContextTypes

from utils.alert_handlers import send_alert


async def test_alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a test operational alert; requires TELEGRAM_ADMIN_USER_ID."""
    admin_id_raw = os.environ.get("TELEGRAM_ADMIN_USER_ID", "").strip()
    if not admin_id_raw:
        await update.message.reply_text("⚠️ TELEGRAM_ADMIN_USER_ID is not configured.")
        return

    try:
        admin_id = int(admin_id_raw)
    except ValueError:
        await update.message.reply_text("⚠️ TELEGRAM_ADMIN_USER_ID must be an integer Telegram user ID.")
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id != admin_id:
        return

    ok = send_alert("🧪 *Test alert*\nOperational alert delivery is configured correctly.")
    await update.message.reply_text("✅ Test alert sent." if ok else "⚠️ Test alert failed. Check logs and env vars.")
