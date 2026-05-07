> ✅ Applied in v11. Kept for reference only.

"""
INTEGRATION PATCH — second_brain/main.py
=========================================

Three surgical additions to wire the health tracker into the existing bot.
Do NOT replace main.py — apply these three changes only.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 1 — Add imports near the top of main.py
(after the existing `from second_brain.config import FEATURES` line)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    from second_brain.healthtrack.routes import register_health_routes
    from second_brain.healthtrack import config as health_config
    from second_brain.healthtrack.steps import handle_steps_final_stamp
    from second_brain.healthtrack.config import (
        STEPS_FINAL_HOUR,
        STEPS_FINAL_MIN,
        STEPS_THRESHOLD,
        STEPS_SOURCE_LABEL,
    )

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 2 — Register HTTP routes in start_http_server()
Find the existing start_http_server() function. It ends with:

    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    runner = web.AppRunner(app)
    ...

Add ONE line after the existing route registrations, before runner setup:

    register_health_routes(
        app,
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        tz=TZ,
        bot_getter=lambda: _app_bot,   # see Change 3 for _app_bot
        chat_id=MY_CHAT_ID,
    )

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 3 — Store bot reference + add scheduled job in post_init()
In post_init(), find:

    scheduler.start()
    _scheduler = scheduler

Add BEFORE scheduler.start():

    # ── Steps final stamp — nightly truth record ──────────────────
    async def _run_steps_final_stamp(bot) -> None:
        await handle_steps_final_stamp(
            notion=notion,
            habit_db_id=NOTION_HABIT_DB,
            log_db_id=NOTION_LOG_DB,
            habit_name=health_config.STEPS_HABIT_NAME,
            threshold=STEPS_THRESHOLD,
            source_label=STEPS_SOURCE_LABEL,
            tz=TZ,
            bot=bot,
            chat_id=MY_CHAT_ID,
        )

    scheduler.add_job(
        _run_steps_final_stamp,
        "cron",
        hour=STEPS_FINAL_HOUR,
        minute=STEPS_FINAL_MIN,
        args=[app.bot],
        id="steps_final_stamp",
    )

And add this line ANYWHERE in post_init() after the app is available
(e.g. right after `load_habit_cache()`):

    global _app_bot
    _app_bot = app.bot

And at module level (near the other globals like `mute_until`), add:

    _app_bot = None  # set during post_init for health route bot access

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 4 — Add env vars to Railway (or .env)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Required:
  - Add `STEPS_HABIT_NAME` = `Steps` to the Notion ENV DB.
  - Keep `STEPS_WEBHOOK_SECRET=your-secret` in Railway.

Optional Railway environment variables:
  HEALTH_STEPS_THRESHOLD=10000
  HEALTH_STEPS_FINAL_TIME=23:59      # default already set

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 5 — Add to GitHub Actions test env (python-package.yml)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

In .github/workflows/python-package.yml, under the `env:` block for pytest, add:

  HEALTH_STEPS_THRESHOLD: "10000"
"""

# This file is documentation only — not imported at runtime.
# Apply the changes above manually to second_brain/main.py.
