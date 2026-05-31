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


def _sleep_session_duration_s(point: dict) -> float:
    """Return duration in seconds of a sleep data point, used to pick the longest session."""
    sleep_raw = point.get("sleep") or {}
    iv = point.get("interval") or sleep_raw.get("interval") or {}
    start_str = iv.get("startTime") or iv.get("civil_start_time") or point.get("startTime") or ""
    end_str = iv.get("endTime") or iv.get("civil_end_time") or point.get("endTime") or ""
    if not start_str or not end_str:
        return 0.0
    try:
        s = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        return max((e - s).total_seconds(), 0.0)
    except Exception:
        return 0.0


def fetch_sleep_data(access_token: str, query_date_str: str, tz) -> dict | None:
    """Fetch the longest sleep data point ending on the wake day after query_date_str."""
    del tz  # Unused here; Google filters by civil end date and parsing applies local tz.
    query_date = date.fromisoformat(query_date_str)
    wake_date = query_date + timedelta(days=1)
    wake_date_plus_1 = wake_date + timedelta(days=1)
    filter_query = (
        f'sleep.interval.civil_end_time >= "{wake_date.isoformat()}" '
        f'AND sleep.interval.civil_end_time < "{wake_date_plus_1.isoformat()}"'
    )
    response = httpx.get(
        GOOGLE_SLEEP_RECONCILE_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"filter": filter_query},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    points = payload.get("dataPoints") or []
    if not isinstance(points, list) or not points:
        return None
    points = [p for p in points if isinstance(p, dict)]
    if not points:
        return None

    # Pick the longest session — avoids naps being selected over main sleep
    first = max(points, key=_sleep_session_duration_s)

    log.info("sleep_sync: %d dataPoint(s), selected longest — keys=%s", len(points), list(first.keys()))
    sleep_summary_debug = first.get("sleepSummary") or {}
    sleep_raw_debug = first.get("sleep") or {}
    stages_debug = (
        first.get("stagesSummary")
        or first.get("stages")
        or sleep_raw_debug.get("stagesSummary")
        or sleep_raw_debug.get("stages")
        or []
    )
    log.info(
        "sleep_sync: top-level keys=%s sleep keys=%s",
        list(first.keys()),
        list(sleep_raw_debug.keys()) if sleep_raw_debug else "absent",
    )
    log.info(
        "sleep_sync: sleepSummary keys=%s sleepSummary=%s",
        list(sleep_summary_debug.keys())
        if isinstance(sleep_summary_debug, dict)
        else type(sleep_summary_debug).__name__,
        sleep_summary_debug,
    )
    log.info(
        "sleep_sync: stagesSummary/stages type=%s first_item=%s",
        type(stages_debug).__name__,
        stages_debug[0] if stages_debug else "empty",
    )
    log.info("sleep_sync: raw dataPoint=%s", first)

    sleep_raw = first.get("sleep") or {}
    interval = first.get("interval") or sleep_raw.get("interval") or {}
    sleep_summary_raw = (
        first.get("sleepSummary")
        or first.get("summary")
        or sleep_raw.get("sleepSummary")
        or sleep_raw.get("summary")
        or {}
    )
    if not isinstance(sleep_summary_raw, dict):
        sleep_summary_raw = {}
    stages_summary = (
        first.get("stagesSummary")
        or first.get("stages")
        or sleep_raw.get("stages")
        or sleep_raw.get("stagesSummary")
        or sleep_summary_raw.get("stagesSummary")
        or sleep_summary_raw.get("stages")
        or []
    )

    def _pick_time(
        top_key: str,
        interval_key: str,
        interval_civil_key: str,
        top_civil_key: str,
    ) -> str:
        summary_interval = sleep_summary_raw.get("interval") or {}
        return (
            first.get(top_key)
            or interval.get(interval_key)
            or interval.get(interval_civil_key)
            or first.get(top_civil_key)
            or sleep_summary_raw.get(top_key)
            or sleep_summary_raw.get(top_civil_key)
            or summary_interval.get(interval_key)
            or summary_interval.get(interval_civil_key)
            or ""
        )

    return {
        "startTime": _pick_time("startTime", "startTime", "civil_start_time", "civilStartTime"),
        "endTime": _pick_time("endTime", "endTime", "civil_end_time", "civilEndTime"),
        "sleepSummary": sleep_summary_raw,
        "stagesSummary": stages_summary,
    }


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
        total_min = 0.0
        for item in stages_summary:
            if not isinstance(item, dict):
                continue
            name = _normalise_stage_name(item.get("stage") or item.get("type") or item.get("name"))
            if name in stage_names:
                minutes_raw = item.get("minutes")
                if minutes_raw is not None:
                    try:
                        total_min += float(minutes_raw)
                    except (ValueError, TypeError):
                        total_min += _duration_ms(item) / 60000
                else:
                    duration = item.get("durationMs") or item.get("duration") or item
                    total_min += _duration_ms(duration) / 60000
        return round(total_min, 2)

    return 0.0


def _parse_google_stages(stages: list) -> dict[str, float]:
    """
    Parse Google Health Connect stage data into {stage_name: minutes}.

    Handles raw stage segments with start/end timestamps and compact summaries
    with precomputed minutes. Returns empty dict for unrecognised input.
    """
    int_stage_map = {
        0: "sleeping",
        1: "awake",
        2: "sleeping",
        3: None,
        4: "light",
        5: "deep",
        6: "rem",
    }
    str_stage_map = {
        "AWAKE": "awake",
        "LIGHT": "light",
        "DEEP": "deep",
        "REM": "rem",
        "SLEEPING": "sleeping",
        "UNKNOWN": "sleeping",
    }
    totals: dict[str, float] = {}
    for item in stages:
        if not isinstance(item, dict):
            continue
        stage_raw = item.get("stage") if "stage" in item else item.get("type")
        if stage_raw is None:
            continue
        if isinstance(stage_raw, int):
            name = int_stage_map.get(stage_raw)
        else:
            name = str_stage_map.get(str(stage_raw).upper().strip())
        if name is None:
            continue
        minutes_raw = item.get("minutes")
        if minutes_raw is not None:
            try:
                minutes = float(minutes_raw)
            except (ValueError, TypeError):
                continue
        else:
            interval = item.get("interval") or {}
            start_str = interval.get("startTime") or item.get("startTime") or ""
            end_str = interval.get("endTime") or item.get("endTime") or ""
            if not start_str or not end_str:
                continue
            try:
                start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                minutes = max((end - start).total_seconds() / 60, 0.0)
            except Exception:
                continue
        totals[name] = totals.get(name, 0.0) + minutes
    return totals


def parse_sleep_data_point(point: dict, tz) -> dict:
    """Convert a Google Health sleep data point into Notion-ready values."""
    sleep_raw = point.get("sleep") or {}
    if not isinstance(sleep_raw, dict):
        sleep_raw = {}
    sleep_summary = point.get("sleepSummary") or sleep_raw.get("sleepSummary") or sleep_raw.get("summary") or {}
    if not isinstance(sleep_summary, dict):
        sleep_summary = {}
    summary_interval = sleep_summary.get("interval") or {}
    try:
        start_dt = _parse_dt(
            point.get("startTime")
            or sleep_summary.get("startTime")
            or sleep_summary.get("civilStartTime")
            or summary_interval.get("startTime")
            or summary_interval.get("civil_start_time")
        )
        end_dt = _parse_dt(
            point.get("endTime")
            or sleep_summary.get("endTime")
            or sleep_summary.get("civilEndTime")
            or summary_interval.get("endTime")
            or summary_interval.get("civil_end_time")
        )
    except ValueError as exc:
        raise ValueError(
            f"sleep_sync: unparseable time fields - {exc} | point keys={list(point.keys())}"
        ) from exc
    local_start = start_dt.astimezone(tz) if tz else start_dt
    local_end = end_dt.astimezone(tz) if tz else end_dt

    stages_summary = (
        point.get("stagesSummary")
        or point.get("stages")
        or sleep_raw.get("stages")
        or sleep_raw.get("stagesSummary")
        or sleep_summary.get("stagesSummary")
        or sleep_summary.get("stages")
        or {}
    )

    def _minutes_from_summary(key: str) -> float:
        val = sleep_summary.get(key)
        if val is None:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return _duration_ms(val) / 60000

    time_in_bed_min = round((end_dt - start_dt).total_seconds() / 60, 2)
    google_stages = _parse_google_stages(stages_summary) if isinstance(stages_summary, list) else {}
    total_from_stages = 0.0
    if google_stages:
        deep_min = round(google_stages.get("deep", 0.0), 2)
        rem_min = round(google_stages.get("rem", 0.0), 2)
        light_min = round(google_stages.get("light", 0.0), 2)
        awake_min = round(google_stages.get("awake", 0.0), 2)
        total_from_stages = round(
            google_stages.get("deep", 0.0)
            + google_stages.get("rem", 0.0)
            + google_stages.get("light", 0.0)
            + google_stages.get("sleeping", 0.0),
            2,
        )
    else:
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

    total_sleep_min = round(
        _minutes_from_summary("minutesAsleep")
        or _minutes_from_summary("minutesInSleepPeriod")
        or _duration_ms(sleep_summary.get("totalDurationMs")) / 60000
        or (total_from_stages if google_stages else 0)
        or time_in_bed_min,  # fallback: use time-in-bed when no sleep analysis is available
        2,
    )
    sleep_score = (
        sleep_summary.get("sleepScore")
        or sleep_summary.get("overallScore")
        or sleep_summary.get("score")
        or None
    )
    try:
        sleep_score = int(sleep_score) if sleep_score is not None else None
    except (ValueError, TypeError):
        sleep_score = None

    sleep_efficiency = round(total_sleep_min / time_in_bed_min, 4) if time_in_bed_min > 0 else 0.0

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
        "sleep_score": sleep_score,
    }


def _sleep_properties(parsed: dict) -> dict[str, dict]:
    props = {
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
    if parsed.get("sleep_score") is not None:
        props["Sleep Score"] = {"number": parsed["sleep_score"]}
    return props


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

    target_date = datetime.now(TZ).date()
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


@track_job_execution("sleep_resync")
async def handle_sleep_resync_job(bot=None) -> dict:
    """Late-evening retry for yesterday's sleep — catches late Fitbit-to-Google syncs."""
    from second_brain.config import (
        GOOGLE_HEALTH_CLIENT_ID,
        GOOGLE_HEALTH_CLIENT_SECRET,
        GOOGLE_HEALTH_REFRESH_TOKEN,
    )
    from second_brain.main import NOTION_HEALTH_METRICS_DB, TZ, notion

    yesterday = (datetime.now(TZ) - timedelta(days=1)).date()
    result = await handle_sleep_sync(
        notion=notion,
        metrics_db_id=NOTION_HEALTH_METRICS_DB,
        client_id=GOOGLE_HEALTH_CLIENT_ID,
        client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
        refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
        target_date=yesterday,
        tz=TZ,
    )
    log.info("sleep_resync: result=%s", result)
    return result


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
        result = await handle_sleep_sync(
            notion=notion,
            metrics_db_id=NOTION_HEALTH_METRICS_DB,
            client_id=GOOGLE_HEALTH_CLIENT_ID,
            client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
            refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
            target_date=cursor,
            tz=TZ,
        )
        results[cursor.isoformat()] = result
        if result.get("action") == "no_data":
            log.warning("sleep_backfill: no data for wake date %s", cursor.isoformat())
        cursor += timedelta(days=1)
        if cursor <= end_day:
            await asyncio.sleep(0.35)

    return {"ok": True, "results": results}
