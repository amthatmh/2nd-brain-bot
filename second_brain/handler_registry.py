"""Centralized Telegram handler registration for the bot entrypoint."""

from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, filters


def register_core_handlers(
    app,
    *,
    handle_start_command,
    handle_remind_command,
    handle_sync_command,
    handle_sync_status_command,
    handle_done_command,
    cmd_mute,
    cmd_unmute,
    cmd_weather,
    cmd_notes,
    cmd_location,
    cmd_habits,
    cmd_log,
    handle_trip_command,
    cmd_signoff,
    handle_message_text,
    handle_callback,
) -> None:
    """Attach all bot command/message/callback handlers to the application."""
    app.add_handler(CommandHandler("start", handle_start_command))
    app.add_handler(CommandHandler("r", handle_remind_command))
    app.add_handler(CommandHandler("remind", handle_remind_command))
    app.add_handler(CommandHandler("sync", handle_sync_command))
    app.add_handler(CommandHandler("syncstatus", handle_sync_status_command))
    app.add_handler(CommandHandler("done", handle_done_command))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("weather", cmd_weather))
    app.add_handler(CommandHandler("notes", cmd_notes))
    app.add_handler(CommandHandler("location", cmd_location))
    app.add_handler(CommandHandler("habits", cmd_habits))
    app.add_handler(CommandHandler("log", cmd_log))
    app.add_handler(CommandHandler("trip", handle_trip_command))
    app.add_handler(CommandHandler("signoff", cmd_signoff))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_text))
    app.add_handler(CallbackQueryHandler(handle_callback))
