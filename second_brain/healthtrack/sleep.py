"""Fitbit/Google Health sleep sync into the Health Metrics Log."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

from second_brain.healthtrack.metrics import (
    _find_page_by_date,
    _find_page_by_name_and_date,
    _title_property,
)
from second_brain.monitoring import track_job_execution

log = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_SLEEP_RECONCILE_URL = "https://health.googleapis.com/v4/users/me/dataTypes/sleep/dataPoints:reconcile"


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Exchange a Google OAuth refresh token for a short-lived access token."""
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Google Health OAuth credentials are not configured")

    response = httpx.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=20,
    )
    response.raise_for_status()
    access_token = str(response.json().get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Google OAuth token response did not include access_token")
    return access_token


def fetch_sleep_data(access_token: str, query_date_str: str, tz) -> dict | None:
    """Fetch sleep data from Google Health API v4 for the wake date following query_date_str."""
    del tz
    query_date = date.fromisoformat(query_date_str)
    # query_date is the night sleep started; wake day is query_date + 1
    wake_date = query_date + timedelta(days=1)
    next_date = wake_date + timedelta(days=1)

    filter_expr = (
        f'sleep.interval.civil_end_time >= "{wake_date.isoformat()}" '
        f'AND sleep.interval.civil_end_time < "{next_date.isoformat()}"'
    )
    response = httpx.get(
        GOOGLE_SLEEP_RECONCILE_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"filter": filter_expr},
        timeout=30,
    )
    response.raise_for_status()
    points = response.json().get("dataPoints") or []
    if not isinstance(points, list) or not points:
        return None
    first = points[0]
    return first if isinstance(first, dict) else None


def _parse_dt(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("sleep data point is missing startTime/endTime")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _duration_ms(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    if isinstance(value, dict):
        for key in ("totalDurationMs", "durationMs", "durationMillis", "duration_ms", "value"):
            if key in value:
                return _duration_ms(value.get(key))
    return 0.0


def _normalise_stage_name(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", " ").replace("-", " ")


def _stage_minutes(stages_summary: Any, stage_names: set[str], direct_keys: tuple[str, ...]) -> float:
    if not stages_summary:
        return 0.0

    if isinstance(stages_summary, dict):
        total_ms = sum(_duration_ms(stages_summary.get(key)) for key in direct_keys)
        for key, value in stages_summary.items():
            key_name = _normalise_stage_name(key)
            if key_name in stage_names:
                total_ms += _duration_ms(value)
        nested = stages_summary.get("stages") or stages_summary.get("summary")
        if isinstance(nested, list):
            total_ms += _stage_minutes(nested, stage_names, direct_keys) * 60000
        return round(total_ms / 60000, 2)

    if isinstance(stages_summary, list):
        total_ms = 0.0
        for item in stages_summary:
            if not isinstance(item, dict):
                continue
            name = _normalise_stage_name(item.get("stage") or item.get("type") or item.get("name"))
            if name in stage_names:
                total_ms += _duration_ms(item)
        return round(total_ms / 60000, 2)

    return 0.0


def parse_sleep_data_point(point: dict, tz) -> dict:
    """Convert a Google Health sleep data point into Notion-ready values."""
    start_dt = _parse_dt(point.get("startTime"))
    end_dt = _parse_dt(point.get("endTime"))
    local_start = start_dt.astimezone(tz) if tz else start_dt
    local_end = end_dt.astimezone(tz) if tz else end_dt

    sleep_summary = point.get("sleepSummary") or {}
    stages_summary = point.get("stagesSummary") or {}

    total_sleep_min = round(_duration_ms(sleep_summary.get("totalDurationMs")) / 60000, 2)
    deep_min = _stage_minutes(
        stages_summary,
        {"deep", "deep sleep"},
        ("deepDurationMs", "deepSleepDurationMs", "deepMs"),
    )
    rem_min = _stage_minutes(
        stages_summary,
        {"rem", "rem sleep"},
        ("remDurationMs", "remSleepDurationMs", "remMs"),
    )
    light_min = _stage_minutes(
        stages_summary,
        {"light", "light sleep"},
        ("lightDurationMs", "lightSleepDurationMs", "lightMs"),
    )
    awake_min = _stage_minutes(
        stages_summary,
        {"awake", "wake", "awake in bed"},
        ("awakeDurationMs", "awakeInBedDurationMs", "wakeDurationMs", "wakeMs"),
    )
    time_in_bed_min = round((end_dt - start_dt).total_seconds() / 60, 2)
    sleep_efficiency = round((total_sleep_min / time_in_bed_min) * 100, 1) if time_in_bed_min > 0 else 0.0

    return {
        "date_str": local_end.date().isoformat(),
        "bedtime_iso": local_start.isoformat(),
        "wake_time_iso": local_end.isoformat(),
        "total_sleep_min": total_sleep_min,
        "deep_min": deep_min,
        "rem_min": rem_min,
        "light_min": light_min,
        "awake_min": awake_min,
        "time_in_bed_min": time_in_bed_min,
        "sleep_efficiency": sleep_efficiency,
    }


def _sleep_properties(parsed: dict) -> dict[str, dict]:
    return {
        "Bedtime": {"date": {"start": parsed["bedtime_iso"]}},
        "Wake Time": {"date": {"start": parsed["wake_time_iso"]}},
        "Total Sleep (min)": {"number": parsed["total_sleep_min"]},
        "Deep Sleep (min)": {"number": parsed["deep_min"]},
        "REM Sleep (min)": {"number": parsed["rem_min"]},
        "Light Sleep (min)": {"number": parsed["light_min"]},
        "Awake in Bed (min)": {"number": parsed["awake_min"]},
        "Time in Bed (min)": {"number": parsed["time_in_bed_min"]},
        "Sleep Efficiency (%)": {"number": parsed["sleep_efficiency"]},
    }


async def handle_sleep_sync(
    *,
    notion,
    metrics_db_id: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    target_date: date | datetime | str,
    tz,
) -> dict:
    """Refresh Fitbit sleep data and upsert the matching wake-date Notion row."""
    if isinstance(target_date, datetime):
        target_day = target_date.astimezone(tz).date() if tz and target_date.tzinfo else target_date.date()
    elif isinstance(target_date, date):
        target_day = target_date
    else:
        target_day = date.fromisoformat(str(target_date)[:10])

    query_date_str = (target_day - timedelta(days=1)).isoformat()
    access_token = await asyncio.to_thread(refresh_access_token, client_id, client_secret, refresh_token)
    point = await asyncio.to_thread(fetch_sleep_data, access_token, query_date_str, tz)
    if not point:
        log.info("sleep_sync: no sleep data for start date %s", query_date_str)
        return {"action": "no_data", "date": target_day.isoformat(), "page_id": None}

    parsed = parse_sleep_data_point(point, tz)
    date_str = parsed["date_str"]
    title = f"{date_str} Log"
    sleep_props = _sleep_properties(parsed)

    page_id = await asyncio.to_thread(_find_page_by_name_and_date, notion, metrics_db_id, title, date_str)
    if page_id:
        await asyncio.to_thread(notion.pages.update, page_id=page_id, properties=sleep_props)
        action = "updated"
    else:
        page_id = await asyncio.to_thread(_find_page_by_date, notion, metrics_db_id, date_str)
        if page_id:
            await asyncio.to_thread(notion.pages.update, page_id=page_id, properties=sleep_props)
            action = "updated"
        else:
            page = await asyncio.to_thread(
                notion.pages.create,
                parent={"database_id": metrics_db_id},
                properties={
                    "Name": _title_property(title),
                    "Date": {"date": {"start": date_str}},
                    **sleep_props,
                },
            )
            page_id = page["id"]
            action = "created"

    log.info("sleep_sync: %s sleep row for %s page_id=%s", action, date_str, page_id)
    return {"action": action, "date": date_str, "page_id": page_id}


@track_job_execution("sleep_sync")
async def handle_sleep_sync_job(bot=None) -> dict:
    """Utility Scheduler job wrapper for nightly/midday sleep refreshes."""
    from second_brain.config import (
        GOOGLE_HEALTH_CLIENT_ID,
        GOOGLE_HEALTH_CLIENT_SECRET,
        GOOGLE_HEALTH_REFRESH_TOKEN,
    )
    from second_brain.main import NOTION_HEALTH_METRICS_DB, TZ, notion
    from second_brain.error_reporting import send_system_log

    target_date = datetime.now(TZ).date()
    try:
        result = await handle_sleep_sync(
            notion=notion,
            metrics_db_id=NOTION_HEALTH_METRICS_DB,
            client_id=GOOGLE_HEALTH_CLIENT_ID,
            client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
            refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
            target_date=target_date,
            tz=TZ,
        )
        log.info("sleep_sync: scheduler result=%s", result)
        return result
    except Exception as exc:
        log.exception("sleep_sync: scheduler job failed")
        if bot is not None:
            await send_system_log(bot, f"Sleep sync failed: {type(exc).__name__}: {exc}")
        raise


async def handle_sleep_backfill_job(bot, start_date_str: str, end_date_str: str) -> dict:
    """Backfill sleep rows for inclusive wake-date range."""
    del bot
    from second_brain.config import (
        GOOGLE_HEALTH_CLIENT_ID,
        GOOGLE_HEALTH_CLIENT_SECRET,
        GOOGLE_HEALTH_REFRESH_TOKEN,
        NOTION_HEALTH_METRICS_DB,
        TZ,
    )
    from second_brain.main import notion

    start_day = date.fromisoformat(start_date_str)
    end_day = date.fromisoformat(end_date_str)
    if end_day < start_day:
        raise ValueError("end_date must be on or after start_date")

    results: dict[str, dict] = {}
    cursor = start_day
    while cursor <= end_day:
        results[cursor.isoformat()] = await handle_sleep_sync(
            notion=notion,
            metrics_db_id=NOTION_HEALTH_METRICS_DB,
            client_id=GOOGLE_HEALTH_CLIENT_ID,
            client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
            refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
            target_date=cursor,
            tz=TZ,
        )
        cursor += timedelta(days=1)
        if cursor <= end_day:
            await asyncio.sleep(0.35)

    return {"ok": True, "results": results}
