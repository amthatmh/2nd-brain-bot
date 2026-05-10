"""Health dashboard JSON endpoint for the static Health frontend."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from aiohttp import web

from second_brain.http_utils import cors_headers
from second_brain.notion import notion_call

log = logging.getLogger(__name__)

VALID_RANGES = {"1m", "3m", "6m", "all"}
CACHE_TTL_SECONDS = 30 * 60
DEFAULT_STEPS_THRESHOLD = 10000
WORKOUT_HABIT_NAME = "💪 Workout"
STEPS_HABIT_NAME = "👟 Steps"

METRIC_PROPERTIES = {
    "weight": ("Weight (kg)", "kg"),
    "body_fat": ("Body Fat %", "%"),
    "lean_mass": ("Lean Body Mass (lbs)", "lbs"),
    "resting_hr": ("Resting Heart Rate (bpm)", "bpm"),
    "hrv": ("HRV (ms)", "ms"),
    "vo2_max": ("VO2 Max", ""),
    "respiratory": ("Respiratory Rate (brpm)", "brpm"),
    "exercise_time": ("Exercise Time (min)", "min"),
    "active_energy": ("Active Energy (kcal)", "kcal"),
    "resting_energy": ("Resting Energy (kcal)", "kcal"),
    "flights": ("Flights Climbed", ""),
    "headphone_db": ("Headphone Audio Exposure (dB)", "dB"),
}

DELTA_UNITS = {
    "weight": "kg",
    "body_fat": "%",
    "lean_mass": "lbs",
    "resting_hr": "bpm",
    "hrv": "ms",
    "vo2_max": "",
}

_health_dashboard_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_steps_threshold_cache: int | None = None


def _date_only(value: str | None) -> str | None:
    if not value:
        return None
    return str(value)[:10]


def _number(prop: dict[str, Any]) -> float | int | None:
    if not prop:
        return None
    value = prop.get("number")
    if value is None:
        value = prop.get("formula", {}).get("number")
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return int(value) if float(value).is_integer() else value
    return None


def _plain_text(prop: dict[str, Any]) -> str:
    for key in ("title", "rich_text"):
        chunks = prop.get(key) or []
        if chunks:
            return "".join(
                chunk.get("plain_text") or chunk.get("text", {}).get("content", "")
                for chunk in chunks
            ).strip()
    formula = prop.get("formula", {})
    if formula.get("type") == "string":
        return (formula.get("string") or "").strip()
    select = prop.get("select")
    if select:
        return (select.get("name") or "").strip()
    return ""


def _range_start(range_value: str, today: date) -> date | None:
    if range_value == "all":
        return None
    months = {"1m": 1, "3m": 3, "6m": 6}[range_value]
    return today - timedelta(days=months * 30)


def _trend_signal(first: float | int | None, last: float | int | None, *, higher_is_better: bool) -> int | None:
    if first is None or last is None:
        return None
    delta = float(last) - float(first)
    if abs(delta) < 1e-9:
        return 50
    improved = delta > 0 if higher_is_better else delta < 0
    return 100 if improved else 0


def _average_score(signals: list[int | None]) -> int | None:
    usable = [signal for signal in signals if signal is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable))


def _metric_delta(points: list[dict[str, Any]], unit: str) -> dict[str, Any]:
    if len(points) < 2:
        return {"value": None, "unit": unit, "direction": "flat"}
    delta = round(float(points[-1]["value"]) - float(points[0]["value"]), 1)
    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
    return {"value": delta, "unit": unit, "direction": direction}


def _latest(points: list[dict[str, Any]]) -> float | int | None:
    return points[-1]["value"] if points else None


def _describe_direction(label: str, delta_info: dict[str, Any], *, higher_is_better: bool) -> str:
    direction = delta_info.get("direction")
    if direction == "flat":
        return f"{label} stable"
    improved = direction == "up" if higher_is_better else direction == "down"
    suffix = "improving" if improved else "worse"
    return f"{label} {suffix}"


def _compute_body_score(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if min(len(metrics["weight"]), len(metrics["body_fat"]), len(metrics["lean_mass"])) < 2:
        return {"value": None, "status": "no_data", "description": "Not enough body data yet"}

    # Body score formula: compare first vs last value for weight (lower is better),
    # body fat (lower is better), and lean mass (higher is better). Each directional
    # signal maps to 100=improved, 50=flat, 0=worse; the score is their average.
    signals = [
        _trend_signal(metrics["weight"][0]["value"], metrics["weight"][-1]["value"], higher_is_better=False),
        _trend_signal(metrics["body_fat"][0]["value"], metrics["body_fat"][-1]["value"], higher_is_better=False),
        _trend_signal(metrics["lean_mass"][0]["value"], metrics["lean_mass"][-1]["value"], higher_is_better=True),
    ]
    deltas = {key: _metric_delta(metrics[key], DELTA_UNITS[key]) for key in ("weight", "body_fat", "lean_mass")}
    description = " · ".join([
        _describe_direction("Weight", deltas["weight"], higher_is_better=False),
        _describe_direction("Fat", deltas["body_fat"], higher_is_better=False),
        _describe_direction("Lean mass", deltas["lean_mass"], higher_is_better=True),
    ])
    return {"value": _average_score(signals), "description": description}


def _compute_cardio_score(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if min(len(metrics["resting_hr"]), len(metrics["hrv"]), len(metrics["vo2_max"])) < 2:
        return {"value": None, "status": "no_data", "description": "Not enough cardio data yet"}

    # Cardio score formula: compare first vs last value for resting HR (lower is
    # better), HRV (higher is better), and VO2 Max (higher is better). Each signal
    # maps to 100=improved, 50=flat, 0=worse; the score is their average.
    signals = [
        _trend_signal(metrics["resting_hr"][0]["value"], metrics["resting_hr"][-1]["value"], higher_is_better=False),
        _trend_signal(metrics["hrv"][0]["value"], metrics["hrv"][-1]["value"], higher_is_better=True),
        _trend_signal(metrics["vo2_max"][0]["value"], metrics["vo2_max"][-1]["value"], higher_is_better=True),
    ]
    deltas = {key: _metric_delta(metrics[key], DELTA_UNITS[key]) for key in ("resting_hr", "hrv", "vo2_max")}
    description = " · ".join([
        _describe_direction("Resting HR", deltas["resting_hr"], higher_is_better=False),
        _describe_direction("HRV", deltas["hrv"], higher_is_better=True),
        _describe_direction("VO2 Max", deltas["vo2_max"], higher_is_better=True),
    ])
    return {"value": _average_score(signals), "description": description}


def _week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _compute_activity_score(weekly_activity: list[dict[str, Any]], steps_threshold: int) -> dict[str, Any]:
    if not weekly_activity:
        return {"value": None, "status": "no_data", "description": "No activity logs yet"}

    avg_workouts = sum(week["workout_days"] for week in weekly_activity) / len(weekly_activity)
    avg_steps = sum(week["steps_days"] for week in weekly_activity) / len(weekly_activity)

    # Activity score formula:
    # - Workout days are 60% of the score, capped at a 3 sessions/week target.
    # - Steps goal days are 40% of the score, capped at a 7 days/week target.
    # The Habit Log Completed flag is trusted for steps; this endpoint does not
    # re-apply the raw steps threshold here.
    workout_pct = min(avg_workouts / 3, 1.0)
    steps_pct = min(avg_steps / 7, 1.0)
    score = round(((workout_pct * 0.6) + (steps_pct * 0.4)) * 100)
    latest_week = weekly_activity[-1]
    return {
        "value": score,
        "description": (
            f"{latest_week['workout_days']}/7 workout days · "
            f"{latest_week['steps_days']}/7 steps days this week · "
            f"target {steps_threshold:,} steps"
        ),
    }


def _scores_no_data() -> dict[str, dict[str, Any]]:
    return {
        "body": {"value": None, "status": "no_data", "description": "No health data in this range"},
        "cardio": {"value": None, "status": "no_data", "description": "No health data in this range"},
        "activity": {"value": None, "status": "no_data", "description": "No activity data in this range"},
        "sleep": {"value": None, "status": "coming_soon", "description": "Sleep tracking coming soon"},
    }


def _parse_health_metric_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    metrics: dict[str, list[dict[str, Any]]] = {key: [] for key in METRIC_PROPERTIES}
    for row in rows:
        props = row.get("properties", {})
        date_str = _date_only(props.get("Date", {}).get("date", {}).get("start"))
        if not date_str:
            continue
        for key, (property_name, _unit) in METRIC_PROPERTIES.items():
            value = _number(props.get(property_name, {}))
            if value is not None:
                metrics[key].append({"date": date_str, "value": value})
    return metrics


def _habit_name_from_log(props: dict[str, Any]) -> str:
    for property_name in ("Habit Name", "Habit", "Entry", "Name"):
        text = _plain_text(props.get(property_name, {}))
        if text:
            return text
    return ""


def _parse_weekly_activity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weekly: dict[date, dict[str, set[str]]] = defaultdict(lambda: {"workouts": set(), "steps": set()})
    for row in rows:
        props = row.get("properties", {})
        if not props.get("Completed", {}).get("checkbox", False):
            continue
        date_str = _date_only(props.get("Date", {}).get("date", {}).get("start"))
        if not date_str:
            continue
        try:
            day = date.fromisoformat(date_str)
        except ValueError:
            continue
        habit_name = _habit_name_from_log(props)
        week = _week_start(day)
        if habit_name == WORKOUT_HABIT_NAME:
            weekly[week]["workouts"].add(date_str)
        elif habit_name == STEPS_HABIT_NAME:
            weekly[week]["steps"].add(date_str)

    return [
        {"week": week.isoformat(), "workout_days": len(counts["workouts"]), "steps_days": len(counts["steps"])}
        for week, counts in sorted(weekly.items())
    ]


def _env_value_from_page(page: dict[str, Any]) -> str:
    return _plain_text(page.get("properties", {}).get("Value", {}))


def load_steps_threshold(notion: Any, env_db_id: str) -> int:
    """Load STEPS_THRESHOLD from the Notion ENV DB once and cache in memory."""
    global _steps_threshold_cache
    if _steps_threshold_cache is not None:
        return _steps_threshold_cache

    if not env_db_id:
        log.warning("health_dashboard: ENV_DB_ID missing; defaulting STEPS_THRESHOLD to %d", DEFAULT_STEPS_THRESHOLD)
        _steps_threshold_cache = DEFAULT_STEPS_THRESHOLD
        return _steps_threshold_cache

    for filter_type in ("title", "rich_text"):
        try:
            response = notion_call(
                notion.databases.query,
                database_id=env_db_id,
                filter={"property": "Name", filter_type: {"equals": "STEPS_THRESHOLD"}},
                page_size=1,
            )
            rows = response.get("results", [])
            if not rows:
                continue
            value = _env_value_from_page(rows[0])
            _steps_threshold_cache = int(value)
            log.info("health_dashboard: loaded STEPS_THRESHOLD=%d from Notion ENV DB", _steps_threshold_cache)
            return _steps_threshold_cache
        except Exception as exc:  # noqa: BLE001 - tolerate alternate ENV DB Name schemas
            log.debug("health_dashboard: STEPS_THRESHOLD lookup with %s failed: %s", filter_type, exc)

    log.warning(
        "health_dashboard: STEPS_THRESHOLD row missing or invalid in ENV DB; defaulting to %d",
        DEFAULT_STEPS_THRESHOLD,
    )
    _steps_threshold_cache = DEFAULT_STEPS_THRESHOLD
    return _steps_threshold_cache


def _health_filter(start: date | None, today: date) -> dict[str, Any] | None:
    if start is None:
        return None
    return {
        "and": [
            {"property": "Date", "date": {"on_or_after": start.isoformat()}},
            {"property": "Date", "date": {"on_or_before": today.isoformat()}},
        ]
    }


def _habit_log_filter(start: date | None, today: date) -> dict[str, Any]:
    date_filters: list[dict[str, Any]] = [{"property": "Date", "date": {"on_or_before": today.isoformat()}}]
    if start is not None:
        date_filters.insert(0, {"property": "Date", "date": {"on_or_after": start.isoformat()}})
    return {
        "and": [
            {"property": "Completed", "checkbox": {"equals": True}},
            *date_filters,
        ]
    }



def _habit_name_query_filter(start: date | None, today: date, filter_type: str) -> dict[str, Any]:
    date_filters: list[dict[str, Any]] = [{"property": "Date", "date": {"on_or_before": today.isoformat()}}]
    if start is not None:
        date_filters.insert(0, {"property": "Date", "date": {"on_or_after": start.isoformat()}})

    if filter_type == "formula":
        name_filters = [
            {"property": "Habit Name", "formula": {"string": {"equals": WORKOUT_HABIT_NAME}}},
            {"property": "Habit Name", "formula": {"string": {"equals": STEPS_HABIT_NAME}}},
        ]
    else:
        name_filters = [
            {"property": "Habit Name", filter_type: {"equals": WORKOUT_HABIT_NAME}},
            {"property": "Habit Name", filter_type: {"equals": STEPS_HABIT_NAME}},
        ]

    return {
        "and": [
            {"property": "Completed", "checkbox": {"equals": True}},
            *date_filters,
            {"or": name_filters},
        ]
    }


def _query_habit_activity_rows(
    *,
    notion_query_all: Callable[..., list[dict[str, Any]]],
    habit_log_db_id: str,
    start: date | None,
    today: date,
) -> list[dict[str, Any]]:
    for filter_type in ("formula", "rich_text", "title"):
        try:
            return notion_query_all(
                database_id=habit_log_db_id,
                filter=_habit_name_query_filter(start, today, filter_type),
                sorts=[{"property": "Date", "direction": "ascending"}],
            )
        except Exception as exc:  # noqa: BLE001 - Habit Name may be formula/rich_text/title depending on schema
            log.debug("health_dashboard: Habit Name %s query failed, trying fallback: %s", filter_type, exc)

    log.warning("health_dashboard: Habit Name filtered query failed; falling back to completed habit logs in range")
    return notion_query_all(
        database_id=habit_log_db_id,
        filter=_habit_log_filter(start, today),
        sorts=[{"property": "Date", "direction": "ascending"}],
    )

def _empty_payload(range_value: str, steps_threshold: int) -> dict[str, Any]:
    return {
        "range": range_value,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "steps_threshold": steps_threshold,
        "scores": _scores_no_data(),
        "metrics": {key: [] for key in METRIC_PROPERTIES},
        "weekly_activity": [],
        "latest": {key: None for key in ("weight", "body_fat", "lean_mass", "resting_hr", "hrv", "vo2_max", "respiratory")},
        "deltas": {key: {"value": None, "unit": unit, "direction": "flat"} for key, unit in DELTA_UNITS.items()},
    }


def build_health_dashboard_payload(
    *,
    range_value: str,
    notion: Any,
    health_metrics_db_id: str,
    habit_log_db_id: str,
    env_db_id: str,
    tz: Any,
    notion_query_all: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    today = datetime.now(tz).date()
    start = _range_start(range_value, today)
    steps_threshold = load_steps_threshold(notion, env_db_id)

    health_kwargs: dict[str, Any] = {"sorts": [{"property": "Date", "direction": "ascending"}]}
    health_filter = _health_filter(start, today)
    if health_filter:
        health_kwargs["filter"] = health_filter

    health_rows = notion_query_all(database_id=health_metrics_db_id, **health_kwargs)
    habit_rows = _query_habit_activity_rows(
        notion_query_all=notion_query_all,
        habit_log_db_id=habit_log_db_id,
        start=start,
        today=today,
    )

    metrics = _parse_health_metric_rows(health_rows)
    weekly_activity = _parse_weekly_activity(habit_rows)

    if not health_rows and not weekly_activity:
        return _empty_payload(range_value, steps_threshold)

    scores = {
        "body": _compute_body_score(metrics),
        "cardio": _compute_cardio_score(metrics),
        "activity": _compute_activity_score(weekly_activity, steps_threshold),
        "sleep": {"value": None, "status": "coming_soon", "description": "Sleep tracking coming soon"},
    }
    latest = {key: _latest(metrics[key]) for key in ("weight", "body_fat", "lean_mass", "resting_hr", "hrv", "vo2_max", "respiratory")}
    deltas = {key: _metric_delta(metrics[key], unit) for key, unit in DELTA_UNITS.items()}

    return {
        "range": range_value,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "steps_threshold": steps_threshold,
        "scores": scores,
        "metrics": metrics,
        "weekly_activity": weekly_activity,
        "latest": latest,
        "deltas": deltas,
    }


def create_health_dashboard_handler(
    *,
    notion: Any,
    health_metrics_db_id: str,
    habit_log_db_id: str,
    env_db_id: str,
    tz: Any,
    notion_query_all: Callable[..., list[dict[str, Any]]],
) -> Callable[[web.Request], Any]:
    if not health_metrics_db_id:
        raise RuntimeError("NOTION_HEALTH_METRICS_DB is required for /api/health-dashboard")
    if not habit_log_db_id:
        raise RuntimeError("NOTION_LOG_DB/NOTION_HABIT_LOG_DB is required for /api/health-dashboard")
    if not env_db_id:
        raise RuntimeError("ENV_DB_ID is required for /api/health-dashboard")

    load_steps_threshold(notion, env_db_id)

    async def health_dashboard_handler(request: web.Request) -> web.Response:
        range_value = (request.query.get("range") or "1m").strip().lower()
        if range_value not in VALID_RANGES:
            range_value = "1m"

        cached = _health_dashboard_cache.get(range_value)
        now = time.time()
        if cached and now - cached[0] < CACHE_TTL_SECONDS:
            return web.Response(
                text=json.dumps(cached[1]),
                content_type="application/json",
                headers=cors_headers(),
            )

        try:
            payload = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: build_health_dashboard_payload(
                    range_value=range_value,
                    notion=notion,
                    health_metrics_db_id=health_metrics_db_id,
                    habit_log_db_id=habit_log_db_id,
                    env_db_id=env_db_id,
                    tz=tz,
                    notion_query_all=notion_query_all,
                ),
            )
            _health_dashboard_cache[range_value] = (now, payload)
            return web.Response(
                text=json.dumps(payload),
                content_type="application/json",
                headers=cors_headers(),
            )
        except Exception as exc:  # noqa: BLE001 - endpoint must return JSON errors
            log.exception("/api/health-dashboard error: %s", exc)
            return web.Response(
                status=500,
                text=json.dumps({"ok": False, "error": "Notion API failure", "detail": str(exc)}),
                content_type="application/json",
                headers=cors_headers(),
            )

    return health_dashboard_handler


def register_health_dashboard_route(
    app: web.Application,
    *,
    notion: Any,
    health_metrics_db_id: str,
    habit_log_db_id: str,
    env_db_id: str,
    tz: Any,
    notion_query_all: Callable[..., list[dict[str, Any]]],
) -> None:
    app.router.add_get(
        "/api/health-dashboard",
        create_health_dashboard_handler(
            notion=notion,
            health_metrics_db_id=health_metrics_db_id,
            habit_log_db_id=habit_log_db_id,
            env_db_id=env_db_id,
            tz=tz,
            notion_query_all=notion_query_all,
        ),
    )
    log.info("Health dashboard route registered: GET /api/health-dashboard")


# TEST: GET /api/health-dashboard (no param) → defaults to 1m range
# TEST: GET /api/health-dashboard?range=3m → returns 3 months of data
# TEST: GET /api/health-dashboard?range=all → returns all rows
# TEST: Response cached — second call within 30min does not re-query Notion
# TEST: range=6m with only 3 weeks of data → scores null, status="no_data"
# TEST: Notion DB unreachable → returns HTTP 500 with JSON error body
# TEST: Workout habit name "💪 Workout" matches correctly (emoji included)
# TEST: Steps habit name "👟 Steps" matches correctly (emoji included)
# TEST: ENV DB lookup on startup — STEPS_THRESHOLD row found → value used in description
# TEST: ENV DB lookup on startup — STEPS_THRESHOLD row missing → defaults to 10000, logs warning
# TEST: Steps completion uses Habit Log Completed flag — does NOT re-check raw step count
# TEST: Activity score with 0 workouts, 7 steps days → score = 0*0.6 + 1.0*0.4 = 40
# TEST: Activity score with 3 workouts, 0 steps days → score = 1.0*0.6 + 0*0.4 = 60
# TEST: Sleep score always returns null + coming_soon regardless of range
# TEST: Delta direction — weight decrease = "down" = green (improving)
# TEST: Delta direction — HRV decrease = "down" = red (worsening)
