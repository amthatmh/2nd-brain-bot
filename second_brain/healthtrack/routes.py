"""
HTTP route for Health Auto Export → steps sync.

Health Auto Export sends a POST to /api/v1/steps-sync with JSON:
  {
    "data": [
      {
        "name": "Step Count",
        "data": [
          {
            "date": "2026-04-28 23:00:00 +0000",   ← or "YYYY-MM-DD" format
            "qty": 11247
          }
        ],
        "units": "count"
      }
    ]
  }

Health Auto Export can also be configured to send a simpler format via
the "REST API" automation. We handle both shapes.

Registration
────────────
In second_brain/main.py → start_http_server(), add:

    from second_brain.healthtrack.routes import register_health_routes
    register_health_routes(app, bot_ref)

Then in post_init(), after the scheduler is started, add the nightly stamp job:

    from second_brain.healthtrack.steps import handle_steps_final_stamp
    from second_brain.healthtrack.config import STEPS_FINAL_HOUR, STEPS_FINAL_MIN, STEPS_HABIT_NAME, STEPS_THRESHOLD, STEPS_SOURCE_LABEL

    scheduler.add_job(
        _run_steps_final_stamp,
        "cron",
        hour=STEPS_FINAL_HOUR,
        minute=STEPS_FINAL_MIN,
        args=[app.bot],
        id="steps_final_stamp",
    )
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from aiohttp import web

from second_brain.healthtrack.config import (
    STEPS_HABIT_NAME,
    STEPS_SOURCE_LABEL,
    STEPS_THRESHOLD,
    STEPS_WRITE_INTRADAY_BELOW_THRESHOLD,
    WEBHOOK_SECRET,
)
from second_brain.healthtrack.steps import handle_steps_sync, get_steps_state_summary
from second_brain.http_utils import cors_headers

log = logging.getLogger(__name__)


def _parse_health_export_payload(body: dict) -> tuple[int, str] | None:
    # Shape 1: flat simple format {"steps": 1234, "date": "YYYY-MM-DD"}
    if "steps" in body and "date" in body:
        try:
            steps = int(body["steps"])
            raw_date = str(body["date"])[:10]
            return steps, raw_date
        except (ValueError, TypeError):
            pass

    # Shape 2: Health Auto Export v2 — body["data"] is a dict with "metrics" key
    # Shape 3: Health Auto Export v1 — body["data"] is a list directly
    data_field = body.get("data", [])
    if isinstance(data_field, dict):
        data_array = data_field.get("metrics", [])
    elif isinstance(data_field, list):
        data_array = data_field
    else:
        return None

    if not data_array:
        return None

    for metric in data_array:
        name = (metric.get("name") or "").lower()
        if "step" not in name:
            continue
        readings = metric.get("data", [])
        if not readings:
            continue

        daily_totals: dict[str, int] = {}
        for reading in readings:
            qty = reading.get("qty") or reading.get("value") or 0
            raw_date = str(reading.get("date") or reading.get("startDate") or "")
            if not raw_date:
                continue
            date_str = raw_date[:10]
            try:
                daily_totals[date_str] = daily_totals.get(date_str, 0) + int(qty)
            except (ValueError, TypeError):
                continue

        if not daily_totals:
            continue

        # Prefer the most recent day in payloads that include multiple dates.
        # Health export batches can include both yesterday and today; choosing the
        # highest total can incorrectly select an older day.
        latest_date = max(daily_totals)
        return daily_totals[latest_date], latest_date

    return None



def register_health_routes(
    app: web.Application,
    notion,
    habit_db_id: str,
    log_db_id: str,
    tz,
    bot_getter,  # callable() → bot, evaluated at request time to avoid circular import
    chat_id: int,
    on_sync_result=None,  # optional callback(result: dict) for telemetry
) -> None:
    """
    Register /api/v1/steps-sync and /api/v1/steps-status routes on the aiohttp app.

    Call this from start_http_server() in second_brain/main.py.

    Args:
        app         — the aiohttp Application
        notion      — NotionClient instance
        habit_db_id — NOTION_HABIT_DB env value
        log_db_id   — NOTION_LOG_DB env value
        tz          — IANA timezone (ZoneInfo from config)
        bot_getter  — zero-arg callable returning the Telegram bot (avoids circular ref)
        chat_id     — MY_CHAT_ID to send threshold notifications
    """

    async def steps_sync_handler(request: web.Request) -> web.Response:
        # Handle CORS preflight
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"))

        # Optional shared secret auth
        if WEBHOOK_SECRET:
            incoming = request.headers.get("X-Health-Secret", "")
            if incoming != WEBHOOK_SECRET:
                log.warning("steps_sync: rejected request — invalid secret")
                return web.Response(
                    status=401,
                    text=json.dumps({"ok": False, "error": "unauthorized"}),
                    content_type="application/json",
                    headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
                )

        try:
            body = await request.json()
        except Exception:
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        parsed = _parse_health_export_payload(body)
        if parsed is None:
            log.warning("steps_sync: could not parse payload: %s", body)
            return web.Response(
                status=422,
                text=json.dumps({"ok": False, "error": "could not parse step count from payload"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        steps, date_str = parsed
        log.info("steps_sync: received %d steps for %s", steps, date_str)

        try:
            bot = bot_getter()
        except Exception:
            bot = None

        result = await handle_steps_sync(
            steps=steps,
            date_str=date_str,
            notion=notion,
            habit_db_id=habit_db_id,
            log_db_id=log_db_id,
            habit_name=STEPS_HABIT_NAME,
            threshold=STEPS_THRESHOLD,
            source_label=STEPS_SOURCE_LABEL,
            tz=tz,
            bot=bot,
            chat_id=chat_id,
            write_intraday_below_threshold=STEPS_WRITE_INTRADAY_BELOW_THRESHOLD,
        )
        if on_sync_result:
            try:
                on_sync_result(result)
            except Exception as e:
                log.warning("steps_sync: telemetry callback failed: %s", e)

        return web.Response(
            text=json.dumps({"ok": True, "result": result}),
            content_type="application/json",
            headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
        )

    async def steps_status_handler(request: web.Request) -> web.Response:
        """Debug endpoint — shows in-memory steps state."""
        return web.Response(
            text=json.dumps({
                "ok": True,
                "threshold": STEPS_THRESHOLD,
                "habit_name": STEPS_HABIT_NAME,
                "state": get_steps_state_summary(),
            }),
            content_type="application/json",
            headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
        )

    app.router.add_post("/api/v1/steps-sync", steps_sync_handler)
    app.router.add_options("/api/v1/steps-sync", steps_sync_handler)
    app.router.add_get("/api/v1/steps-status", steps_status_handler)

    log.info(
        "Health routes registered: POST /api/v1/steps-sync, GET /api/v1/steps-status "
        "(threshold=%d, habit='%s')",
        STEPS_THRESHOLD,
        STEPS_HABIT_NAME,
    )
