"""
HTTP route for Steps Auto Export → steps sync.

Endpoint: POST /api/v1/steps-sync
Header: X-Health-Secret: {STEPS_WEBHOOK_SECRET from Railway ENV}

Request payload (from iOS Health Auto Export):
  {
    "steps": 11247,
    "date": "2026-05-06"
  }
  or
  {
    "data": {
      "metrics": [
        {
          "name": "Step Count",
          "data": [{"date": "2026-05-06", "qty": 11247}],
          "units": "count"
        }
      ]
    }
  }

Response:
  {"ok": true, "result": {"action": "created"|"updated"|"skipped", ...}}
  or
  {"ok": false, "error": "...", "received_keys": [...]}

Observability:
  - Last sync timestamp logged to Notion ENV DB (HEALTH_STEPS_THRESHOLD → Last Sync Time)
  - Failures logged to Railway logs with full error details
  - Threshold notifications sent to Telegram (10,000 steps reached)

Registration
────────────
In second_brain/main.py → start_http_server(), call:

    register_health_routes(
        app,
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        env_db_id=NOTION_ENV_DB,
        tz=TZ,
        bot_getter=lambda: _app_bot,
        chat_id=TELEGRAM_CHAT_ID,
        on_sync_result=lambda result: asyncio.create_task(
            _persist_sync_result_to_env_db(notion, result)
        ),
    )

Future
──────
When adding generic health metrics (/api/v1/health/export for UV, metrics, etc.):
- Create separate register_health_export_routes() with same pattern
- Use HEALTH_EXPORT_WEBHOOK_SECRET for different secret
- Update ENV DB with HEALTH_UV_THRESHOLD, HEALTH_METRICS_SYNC_TIME, etc.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone

from aiohttp import web

from second_brain.healthtrack import config as health_config
from second_brain.healthtrack.config import (
    STEPS_SOURCE_LABEL,
    STEPS_THRESHOLD,
    STEPS_WRITE_INTRADAY_BELOW_THRESHOLD,
    WEBHOOK_SECRET,
)
from second_brain.healthtrack.metrics import (
    MalformedHealthMetricsPayload,
    handle_health_metrics_sync,
)
from second_brain.healthtrack.steps import (
    handle_steps_sync,
    get_steps_state_summary,
    _find_steps_habit_page_id,
    _find_existing_log_entry,
    _create_log_entry,
    _update_log_entry_steps,
)
from second_brain.http_utils import cors_headers
from second_brain.notion.properties import query_all
from second_brain.notion.habits import already_logged_today, log_habit as create_habit_log
from second_brain.services.note_utils import extract_date_only
from second_brain.state import STATE

log = logging.getLogger(__name__)


_last_steps_webhook: dict = {}


def _coerce_step_count(value) -> int | None:
    """Convert Health Auto Export numeric values to an integer step count."""
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _payload_summary(body) -> dict:
    """Return a small, secret-free description of an incoming payload."""
    if not isinstance(body, dict):
        return {"type": type(body).__name__}

    data_field = body.get("data")
    if isinstance(data_field, dict):
        metrics = data_field.get("metrics", [])
        data_shape = "data.metrics"
    elif isinstance(data_field, list):
        metrics = data_field
        data_shape = "data[]"
    elif isinstance(body.get("metrics"), list):
        metrics = body.get("metrics", [])
        data_shape = "metrics[]"
    else:
        metrics = []
        data_shape = type(data_field).__name__ if data_field is not None else "missing"

    metric_names = []
    for metric in metrics[:10]:
        if isinstance(metric, dict):
            metric_names.append(metric.get("name") or metric.get("type") or "<unnamed>")

    return {
        "type": "dict",
        "top_level_keys": sorted(str(key) for key in body.keys())[:20],
        "data_shape": data_shape,
        "metric_count": len(metrics),
        "metric_names": metric_names,
    }


def _record_steps_webhook(request: web.Request, *, status: str, detail: dict | None = None) -> None:
    """Keep the latest webhook attempt visible via /steps-status without storing secrets."""
    headers = request.headers
    _last_steps_webhook.clear()
    _last_steps_webhook.update(
        {
            "at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "method": request.method,
            "content_type": headers.get("Content-Type"),
            "content_length": headers.get("Content-Length"),
            "automation_name": headers.get("automation-name"),
            "automation_id": headers.get("automation-id"),
            "automation_period": headers.get("automation-period"),
            "automation_aggregation": headers.get("automation-aggregation"),
            "session_id": headers.get("session-id"),
        }
    )
    if detail:
        _last_steps_webhook.update(detail)


def _parse_health_export_payload(body: dict) -> tuple[int, str] | None:
    # Shape 1: flat simple format {"steps": 1234, "date": "YYYY-MM-DD"}
    if "steps" in body and "date" in body:
        steps = _coerce_step_count(body.get("steps"))
        if steps is not None:
            raw_date = str(body["date"])[:10]
            return steps, raw_date

    # Shape 2: Health Auto Export v2 — body["data"] is a dict with "metrics" key
    # Shape 3: Health Auto Export v1 — body["data"] is a list directly
    data_field = body.get("data")
    if isinstance(data_field, dict):
        data_array = data_field.get("metrics", [])
    elif isinstance(data_field, list):
        data_array = data_field
    elif isinstance(body.get("metrics"), list):
        # Be liberal for manually exported/test payloads that omit the data wrapper.
        data_array = body.get("metrics", [])
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
            qty = _coerce_step_count(
                reading.get("qty")
                if reading.get("qty") is not None
                else reading.get("value", reading.get("steps"))
            )
            if qty is None:
                continue
            raw_date = str(reading.get("date") or reading.get("startDate") or "")
            if not raw_date:
                continue
            date_str = raw_date[:10]
            daily_totals[date_str] = daily_totals.get(date_str, 0) + qty

        if not daily_totals:
            continue

        # Prefer the most recent day in payloads that include multiple dates.
        # Health export batches can include both yesterday and today; choosing the
        # highest total can incorrectly select an older day.
        latest_date = max(daily_totals)
        return daily_totals[latest_date], latest_date

    return None



def _parse_all_dates_from_payload(body: dict) -> list[tuple[int, str]]:
    """
    Parse ALL date/steps pairs from a Health Auto Export payload.
    Returns list of (steps, date_str) tuples, one per calendar date.
    If multiple readings exist for the same date, sum them (daily total).
    Dates are extracted as YYYY-MM-DD strings.
    Returns empty list if payload cannot be parsed.
    """
    # Shape 1: flat simple format {"steps": 1234, "date": "YYYY-MM-DD"} — single entry
    if "steps" in body and "date" in body:
        steps = _coerce_step_count(body.get("steps"))
        if steps is not None:
            raw_date = str(body["date"])[:10]
            return [(steps, raw_date)]

    # Shape 2: Health Auto Export v2 — body["data"] is a dict with "metrics" key
    # Shape 3: Health Auto Export v1 — body["data"] is a list directly
    data_field = body.get("data")
    if isinstance(data_field, dict):
        data_array = data_field.get("metrics", [])
    elif isinstance(data_field, list):
        data_array = data_field
    elif isinstance(body.get("metrics"), list):
        data_array = body.get("metrics", [])
    else:
        return []

    if not data_array:
        return []

    for metric in data_array:
        name = (metric.get("name") or "").lower()
        if "step" not in name:
            continue
        readings = metric.get("data", [])
        if not readings:
            continue

        daily_totals: dict[str, int] = {}
        for reading in readings:
            qty = _coerce_step_count(
                reading.get("qty")
                if reading.get("qty") is not None
                else reading.get("value", reading.get("steps"))
            )
            if qty is None:
                continue
            raw_date = str(reading.get("date") or reading.get("startDate") or "")
            if not raw_date:
                continue
            date_str = raw_date[:10]
            daily_totals[date_str] = daily_totals.get(date_str, 0) + qty

        if daily_totals:
            return [(steps, date_str) for date_str, steps in sorted(daily_totals.items())]

    return []



async def habits_data_handler(
    request: web.Request,
    *,
    notion,
    habit_cache: dict[str, dict],
    log_db: str,
    habit_db: str,
    streak_db: str,
    tz,
    weeks_history: int,
    query_all_fn=None,
    extract_date_fn=extract_date_only,
    datetime_cls=datetime,
) -> web.Response:
    query_all_fn = query_all_fn or (lambda database_id, **kwargs: query_all(notion, database_id, **kwargs))
    now = datetime_cls.now(tz)

    if STATE.habits_data_cache.get("payload"):
        return web.Response(
            text=json.dumps(STATE.habits_data_cache["payload"]),
            content_type="application/json",
            headers=cors_headers(),
        )

    try:
        habits_sorted = sorted(habit_cache.values(), key=lambda h: h["sort"])
        today    = now.date()
        num_days = weeks_history * 7
        start_dt = today - timedelta(days=num_days - 1)

        results = query_all_fn(
            log_db,
            filter={
                "and": [
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after":  start_dt.isoformat()}},
                    {"property": "Date", "date": {"on_or_before": today.isoformat()}},
                ]
            },
        )

        # Build lookup set — strip dashes from relation IDs (Notion returns them without)
        logged: set[tuple] = set()
        for page in results:
            p        = page["properties"]
            d        = p.get("Date", {}).get("date", {})
            date_str = extract_date_fn(d.get("start") if d else None)
            rels     = p.get("Habit", {}).get("relation", [])
            for rel in rels:
                if date_str:
                    logged.add((rel["id"].replace("-", ""), date_str))

        all_dates  = [(start_dt + timedelta(days=i)).isoformat() for i in range(num_days)]
        habits_out = []
        for habit in habits_sorted:
            pid  = habit["page_id"].replace("-", "")
            days = [1 if (pid, d) in logged else 0 for d in all_dates]
            day_streak = 0
            for done in reversed(days):
                if done != 1:
                    break
                day_streak += 1
            streak_results = query_all_fn(
                streak_db,
                filter={"property": "Habit", "relation": {"contains": habit["page_id"]}},
            )
            streak_weeks_by_date: dict[date, bool] = {}
            for streak_row in streak_results:
                props = streak_row.get("properties", {})
                week_date_raw = extract_date_fn(
                    props.get("Week Of", {}).get("date", {}).get("start"),
                )
                if not week_date_raw:
                    continue
                try:
                    week_date = datetime_cls.fromisoformat(week_date_raw).date()
                except ValueError:
                    continue
                goal_met = bool(props.get("Goal Met", {}).get("checkbox"))
                # Keep one status per week, favoring goal_met=True if duplicates exist.
                streak_weeks_by_date[week_date] = streak_weeks_by_date.get(week_date, False) or goal_met

            target = habit.get("freq_per_week")
            if not isinstance(target, int) or target <= 0:
                label = habit.get("frequency_label") or ""
                match = re.search(r"\d+", label)
                target = int(match.group(0)) if match else None

            weekly_counts: dict[date, int] = {}
            for date_str, done in zip(all_dates, days):
                if done != 1:
                    continue
                try:
                    day_date = datetime_cls.fromisoformat(date_str).date()
                except ValueError:
                    continue
                week_of = day_date - timedelta(days=day_date.weekday())
                weekly_counts[week_of] = weekly_counts.get(week_of, 0) + 1

            current_monday = today - timedelta(days=today.weekday())
            if target and target > 0:
                # For UI display, compute weekly goal attainment directly from logs
                # using the current target. This keeps streaks correct even when
                # streak rows are stale/missing or created before target changes.
                week_cursor = start_dt - timedelta(days=start_dt.weekday())
                while week_cursor < current_monday:
                    completed = weekly_counts.get(week_cursor, 0)
                    streak_weeks_by_date[week_cursor] = completed >= target
                    week_cursor += timedelta(days=7)

            streak_weeks = sorted(
                ((week_date, goal_met) for week_date, goal_met in streak_weeks_by_date.items() if week_date < current_monday),
                key=lambda item: item[0],
                reverse=True,
            )
            week_streak = 0
            expected_week: date = current_monday - timedelta(days=7)
            for week_date, goal_met in streak_weeks:
                if week_date != expected_week:
                    break
                if not goal_met:
                    break
                week_streak += 1
                expected_week = week_date - timedelta(days=7)
            habits_out.append({
                "id":          habit["page_id"],
                "name":        habit["name"],
                "icon":        habit.get("icon"),
                "color":       habit.get("color") or "pink",
                "description": habit.get("description") or "",
                "frequency":   habit.get("frequency_label") or "",
                "sort":        habit.get("sort"),
                "days":        days,
                "todayDone":   days[-1] == 1,
                "dayStreak":   day_streak,
                "weekStreak":  week_streak,
            })

        payload = {
            "generated":    now.isoformat(),
            "habits":       habits_out,
            "dates":        all_dates,
            "todayDate":    today.isoformat(),
            "weeksHistory": weeks_history,
        }
        STATE.habits_data_cache["payload"] = payload
        return web.Response(
            text=json.dumps(payload),
            content_type="application/json",
            headers=cors_headers(),
        )
    except Exception as e:
        log.error(f"/habits-data error: {e}")
        return web.Response(status=500, text=str(e), headers=cors_headers())


async def log_habit_http_handler(
    request: web.Request,
    *,
    notion,
    habit_cache: dict[str, dict],
    log_db: str,
    habit_db: str,
    streak_db: str,
    tz,
    weeks_history: int,
) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=cors_headers())

    try:
        body = await request.json()
        habit_id = (body.get("habitId") or "").strip()
        if not habit_id:
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "habitId is required"}),
                content_type="application/json",
                headers=cors_headers(),
            )

        matched = next((h for h in habit_cache.values() if h["page_id"] == habit_id), None)
        if not matched:
            return web.Response(
                status=404,
                text=json.dumps({"ok": False, "error": "Habit not found"}),
                content_type="application/json",
                headers=cors_headers(),
            )

        if already_logged_today(notion, log_db, matched["page_id"], tz):
            return web.Response(
                text=json.dumps({"ok": True, "alreadyLogged": True, "habitName": matched["name"]}),
                content_type="application/json",
                headers=cors_headers(),
            )

        create_habit_log(notion, log_db, matched["page_id"], matched["name"], source="🌐 HabitKit")
        STATE.habits_data_cache.clear()
        log.info("habits_data_cache: invalidated after HabitKit log")
        return web.Response(
            text=json.dumps({"ok": True, "alreadyLogged": False, "habitName": matched["name"]}),
            content_type="application/json",
            headers=cors_headers(),
        )
    except Exception as e:
        log.error(f"/log-habit error: {e}")
        return web.Response(
            status=500,
            text=json.dumps({"ok": False, "error": str(e)}),
            content_type="application/json",
            headers=cors_headers(),
        )


def register_health_routes(
    app: web.Application,
    notion,
    habit_db_id: str,
    log_db_id: str,
    env_db_id: str,
    tz,
    bot_getter,  # callable() → bot, evaluated at request time to avoid circular import
    chat_id: int,
    on_sync_result=None,  # optional callback(result: dict) for telemetry
    health_metrics_db_id: str = "",
    habit_cache: dict[str, dict] | None = None,
    streak_db_id: str = "",
    weeks_history: int = 52,
) -> None:
    """
    Register health tracking routes on the aiohttp app.

    Call this from start_http_server() in second_brain/main.py.

    Args:
        app         — the aiohttp Application
        notion      — NotionClient instance
        habit_db_id — NOTION_HABIT_DB env value
        log_db_id   — NOTION_LOG_DB env value
        env_db_id   — ENV_DB_ID env value for persisted notification ids
        tz          — IANA timezone (ZoneInfo from config)
        bot_getter  — zero-arg callable returning the Telegram bot (avoids circular ref)
        chat_id     — MY_CHAT_ID to send threshold notifications
        health_metrics_db_id — NOTION_HEALTH_METRICS_DB env value for /api/v1/health-sync
    """


    async def _habits_data(request: web.Request) -> web.Response:
        return await habits_data_handler(
            request,
            notion=notion,
            habit_cache=habit_cache or {},
            log_db=log_db_id,
            habit_db=habit_db_id,
            streak_db=streak_db_id,
            tz=tz,
            weeks_history=weeks_history,
        )

    async def _log_habit(request: web.Request) -> web.Response:
        return await log_habit_http_handler(
            request,
            notion=notion,
            habit_cache=habit_cache or {},
            log_db=log_db_id,
            habit_db=habit_db_id,
            streak_db=streak_db_id,
            tz=tz,
            weeks_history=weeks_history,
        )

    async def steps_sync_handler(request: web.Request) -> web.Response:
        # Handle CORS preflight
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"))

        incoming_secret = request.headers.get("X-Health-Secret", "").strip()
        if not incoming_secret:
            log.warning("steps_sync: missing X-Health-Secret header")
            _record_steps_webhook(request, status="missing_secret")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Missing X-Health-Secret header"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
            log.warning("steps_sync: invalid secret")
            _record_steps_webhook(request, status="unauthorized")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Invalid X-Health-Secret"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        try:
            body = await request.json()
        except Exception as e:
            log.warning("steps_sync: invalid JSON payload: %s", e)
            _record_steps_webhook(request, status="invalid_json")
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        parsed = _parse_health_export_payload(body)
        if parsed is None:
            summary = _payload_summary(body)
            received_keys = list(body.keys()) if isinstance(body, dict) else []
            log.warning("steps_sync: could not parse payload. Body keys: %s", received_keys)
            _record_steps_webhook(
                request,
                status="parse_error",
                detail={"payload_summary": summary},
            )
            return web.Response(
                status=422,
                text=json.dumps(
                    {
                        "ok": False,
                        "error": "Could not parse step count from payload",
                        "received_keys": received_keys,
                        "expected_format": {
                            "simple": {"steps": 1234, "date": "YYYY-MM-DD"},
                            "v2": {
                                "data": {
                                    "metrics": [
                                        {"name": "Step Count", "data": [{"date": "YYYY-MM-DD", "qty": 1234}]}
                                    ]
                                }
                            },
                        },
                    }
                ),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        steps, date_str = parsed
        log.info("steps_sync: received %d steps for %s", steps, date_str)
        _record_steps_webhook(
            request,
            status="parsed",
            detail={
                "payload_summary": _payload_summary(body),
                "parsed": {"steps": steps, "date": date_str},
            },
        )

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
            env_db_id=env_db_id,
            habit_name=health_config.STEPS_HABIT_NAME,
            threshold=STEPS_THRESHOLD,
            source_label=STEPS_SOURCE_LABEL,
            tz=tz,
            bot=bot,
            chat_id=chat_id,
            write_intraday_below_threshold=STEPS_WRITE_INTRADAY_BELOW_THRESHOLD,
            force_write=False,
        )
        if on_sync_result:
            try:
                callback_result = on_sync_result(result)
                if inspect.isawaitable(callback_result):
                    await callback_result
            except Exception as e:
                log.warning("steps_sync: telemetry callback failed: %s", e)

        _last_steps_webhook["status"] = "processed"
        _last_steps_webhook["result"] = result

        return web.Response(
            text=json.dumps({"ok": True, "result": result}),
            content_type="application/json",
            headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
        )



    async def health_sync_handler(request: web.Request) -> web.Response:
        # Handle CORS preflight
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"))

        incoming_secret = request.headers.get("X-Health-Secret", "").strip()
        if not incoming_secret:
            log.warning("health_sync: missing X-Health-Secret header")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Missing X-Health-Secret header"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
            log.warning("health_sync: invalid secret")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Invalid X-Health-Secret"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        if not health_metrics_db_id:
            log.error("health_sync: NOTION_HEALTH_METRICS_DB is not configured")
            return web.Response(
                status=500,
                text=json.dumps({"ok": False, "error": "NOTION_HEALTH_METRICS_DB is not configured"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        try:
            body = await request.json()
        except Exception as e:
            log.warning("health_sync: invalid JSON payload: %s", e)
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        try:
            result = await handle_health_metrics_sync(
                body=body,
                notion=notion,
                metrics_db_id=health_metrics_db_id,
                tz=tz,
            )
        except MalformedHealthMetricsPayload as e:
            received_keys = list(body.keys()) if isinstance(body, dict) else []
            log.warning("health_sync: malformed payload: %s", e)
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": str(e), "received_keys": received_keys}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )
        except Exception as e:
            log.exception("health_sync: Notion sync failed: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"ok": False, "error": "Notion API failure"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

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
                "habit_name": health_config.STEPS_HABIT_NAME,
                "last_webhook": dict(_last_steps_webhook),
                "state": get_steps_state_summary(),
            }),
            content_type="application/json",
            headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
        )

    async def steps_backfill_handler(request: web.Request) -> web.Response:
        if request.method == "OPTIONS":
            return web.Response(status=204, headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"))

        incoming_secret = request.headers.get("X-Health-Secret", "").strip()
        if not incoming_secret:
            log.warning("steps_backfill: missing X-Health-Secret header")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Missing X-Health-Secret header"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
            log.warning("steps_backfill: invalid secret")
            return web.Response(
                status=401,
                text=json.dumps({"ok": False, "error": "Invalid X-Health-Secret"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        try:
            body = await request.json()
        except Exception as e:
            log.warning("steps_backfill: invalid JSON payload: %s", e)
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        date_steps_pairs = _parse_all_dates_from_payload(body)
        if not date_steps_pairs:
            received_keys = list(body.keys()) if isinstance(body, dict) else []
            log.warning("steps_backfill: could not parse any dates from payload. Body keys: %s", received_keys)
            return web.Response(
                status=422,
                text=json.dumps({"ok": False, "error": "could not parse any dates from payload"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        today_str = datetime.now(tz).strftime("%Y-%m-%d")
        total_dates = len(date_steps_pairs)
        log.info("steps_backfill: starting backfill of %d dates", total_dates)

        habit_page_id = await asyncio.to_thread(
            _find_steps_habit_page_id, notion, habit_db_id, health_config.STEPS_HABIT_NAME
        )
        if not habit_page_id:
            log.error("steps_backfill: Steps habit not found in Habits DB")
            return web.Response(
                status=500,
                text=json.dumps({"ok": False, "error": "Steps habit not found in Habits DB"}),
                content_type="application/json",
                headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
            )

        created = 0
        updated = 0
        skipped = 0
        errors = 0
        dates_attempted: list[str] = []

        for i, (steps, date_str) in enumerate(date_steps_pairs, start=1):
            if date_str > today_str:
                log.warning("steps_backfill: skipping future date %s", date_str)
                skipped += 1
                continue

            dates_attempted.append(date_str)
            try:
                completed = steps >= STEPS_THRESHOLD

                existing_page_id = await asyncio.to_thread(
                    _find_existing_log_entry, notion, log_db_id, habit_page_id, date_str
                )

                if existing_page_id:
                    success = await asyncio.to_thread(
                        _update_log_entry_steps, notion, existing_page_id, steps, completed
                    )
                    if success:
                        updated += 1
                    else:
                        errors += 1
                else:
                    new_page_id = await asyncio.to_thread(
                        _create_log_entry,
                        notion,
                        log_db_id,
                        habit_page_id,
                        date_str,
                        steps,
                        completed,
                        STEPS_SOURCE_LABEL,
                    )
                    if new_page_id:
                        created += 1
                    else:
                        errors += 1

            except Exception as e:
                log.error("steps_backfill: error for date %s: %s", date_str, e)
                errors += 1

            if i % 10 == 0:
                log.info("steps_backfill: processed %d/%d dates...", i, total_dates)

            await asyncio.sleep(0.35)

        log.info(
            "steps_backfill: complete — total=%d created=%d updated=%d skipped=%d errors=%d",
            total_dates,
            created,
            updated,
            skipped,
            errors,
        )

        date_range: dict = {}
        if dates_attempted:
            date_range = {"from": min(dates_attempted), "to": max(dates_attempted)}

        return web.Response(
            text=json.dumps({
                "ok": True,
                "summary": {
                    "total_dates": total_dates,
                    "created": created,
                    "updated": updated,
                    "skipped": skipped,
                    "errors": errors,
                    "date_range": date_range,
                },
            }),
            content_type="application/json",
            headers=cors_headers(extra_allow_headers="Content-Type, X-Health-Secret"),
        )

        # TEST: POST /api/v1/steps-backfill with 3-date payload → creates 3 Notion entries
        # TEST: POST same payload again → updates all 3 (no duplicates created)
        # TEST: Mixed payload — some dates exist, some don't → correct created/updated counts
        # TEST: Date with multiple intraday readings → summed into daily total
        # TEST: Future date in payload → skipped, logged as warning
        # TEST: No X-Health-Secret header → 401 response
        # TEST: Malformed payload → 422 response, no partial writes
        # TEST: Single date that already exists with lower step count → updated to new value
        # TEST: Completed flag: 9999 steps → False, 10000 steps → True
        # TEST: No Telegram notification sent for any historical date regardless of step count

    app.router.add_get("/habits-data", _habits_data)
    app.router.add_post("/log-habit", _log_habit)
    app.router.add_options("/log-habit", _log_habit)
    app.router.add_post("/api/v1/steps-sync", steps_sync_handler)
    app.router.add_options("/api/v1/steps-sync", steps_sync_handler)
    app.router.add_get("/api/v1/steps-status", steps_status_handler)
    app.router.add_post("/api/v1/health-sync", health_sync_handler)
    app.router.add_options("/api/v1/health-sync", health_sync_handler)
    app.router.add_post("/api/v1/steps-backfill", steps_backfill_handler)
    app.router.add_options("/api/v1/steps-backfill", steps_backfill_handler)

    log.info(
        "Health routes registered: POST /api/v1/steps-sync, POST /api/v1/health-sync, "
        "POST /api/v1/steps-backfill, GET /api/v1/steps-status (threshold=%d, habit='%s')",
        STEPS_THRESHOLD,
        health_config.STEPS_HABIT_NAME,
    )
