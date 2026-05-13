"""Health dashboard JSON endpoint backed by Notion.

This module follows the public-dashboard pattern used by ``/habits-data``:
Railway reads Notion with server-side credentials, returns clean JSON, and caches
responses in memory so browser reloads do not hit Notion every time.
"""

from __future__ import annotations

import logging
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from aiohttp import web

from second_brain.http_utils import cors_headers
from second_brain.notion import notion_call

log = logging.getLogger(__name__)

VALID_RANGES = {"1m", "3m", "6m", "all"}
RANGE_DAYS = {"1m": 30, "3m": 90, "6m": 180}
CACHE_TTL_SECONDS = 30 * 60
DEFAULT_STEPS_THRESHOLD = 10000
STEPS_THRESHOLD = DEFAULT_STEPS_THRESHOLD

_RESPONSE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

METRIC_DEFS: dict[str, dict[str, str]] = {
    "weight": {"property": "Weight (kg)", "unit": "kg", "good": "down"},
    "body_fat": {"property": "Body Fat %", "unit": "%", "good": "down"},
    "lean_mass": {"property": "Lean Body Mass (kg)", "unit": "kg", "good": "up"},
    "resting_hr": {"property": "Resting Heart Rate (bpm)", "unit": "bpm", "good": "down"},
    "hrv": {"property": "HRV (ms)", "unit": "ms", "good": "up"},
    "vo2_max": {"property": "VO2 Max", "unit": "", "good": "up"},
    "respiratory": {"property": "Respiratory Rate (brpm)", "unit": "brpm", "good": "flat"},
    "exercise_time": {"property": "Exercise Time (min)", "unit": "min", "good": "up"},
    "active_energy": {"property": "Active Energy (kcal)", "unit": "kcal", "good": "up"},
    "resting_energy": {"property": "Resting Energy (kcal)", "unit": "kcal", "good": "flat"},
    "flights": {"property": "Flights Climbed", "unit": "", "good": "up"},
    "headphone_db": {"property": "Headphone Audio Exposure (dB)", "unit": "dB", "good": "down"},
}


def _extract_plain_text(prop: dict[str, Any]) -> str:
    for prop_type in ("title", "rich_text"):
        chunks = prop.get(prop_type) or []
        text = "".join(chunk.get("plain_text") or chunk.get("text", {}).get("content", "") for chunk in chunks)
        if text:
            return text.strip()
    if prop.get("formula", {}).get("string"):
        return str(prop["formula"]["string"]).strip()
    if prop.get("select", {}).get("name"):
        return str(prop["select"]["name"]).strip()
    if prop.get("rollup"):
        rollup = prop["rollup"]
        if rollup.get("type") == "array":
            parts = [_extract_plain_text(item) for item in rollup.get("array", [])]
            return " ".join(part for part in parts if part).strip()
    return ""


def _extract_number(prop: dict[str, Any]) -> float | None:
    if not prop:
        return None
    if prop.get("number") is not None:
        return float(prop["number"])
    formula = prop.get("formula", {})
    if formula.get("number") is not None:
        return float(formula["number"])
    text = _extract_plain_text(prop)
    if text:
        try:
            return float(text.replace(",", ""))
        except ValueError:
            return None
    return None


def _extract_date(prop: dict[str, Any]) -> str | None:
    start = (prop.get("date") or {}).get("start")
    return str(start)[:10] if start else None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _date_window(range_value: str, tz) -> tuple[date | None, date]:
    today = datetime.now(tz).date() if tz else datetime.now(timezone.utc).date()
    if range_value == "all":
        return None, today
    return today - timedelta(days=RANGE_DAYS[range_value] - 1), today


def _query_all(notion, database_id: str, **kwargs: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = None
    while True:
        query_args = dict(kwargs)
        if cursor:
            query_args["start_cursor"] = cursor
        response = notion_call(notion.databases.query, database_id=database_id, **query_args)
        rows.extend(response.get("results", []))
        if not response.get("has_more"):
            return rows
        cursor = response.get("next_cursor")


def load_steps_threshold_from_env_db(notion, env_db_id: str) -> int:
    """Load STEPS_THRESHOLD once at startup and keep it in module memory."""
    global STEPS_THRESHOLD
    STEPS_THRESHOLD = DEFAULT_STEPS_THRESHOLD
    if not env_db_id:
        log.warning("health_dashboard: ENV_DB_ID missing; defaulting STEPS_THRESHOLD to %d", STEPS_THRESHOLD)
        return STEPS_THRESHOLD

    try:
        response = notion_call(
            notion.databases.query,
            database_id=env_db_id,
            filter={"property": "Name", "title": {"equals": "STEPS_THRESHOLD"}},
            page_size=1,
        )
        rows = response.get("results", [])
        if not rows:
            log.warning("health_dashboard: STEPS_THRESHOLD row missing; defaulting to %d", STEPS_THRESHOLD)
            return STEPS_THRESHOLD
        value = _extract_plain_text(rows[0].get("properties", {}).get("Value", {}))
        STEPS_THRESHOLD = int(value)
        log.info("health_dashboard: loaded STEPS_THRESHOLD=%d", STEPS_THRESHOLD)
    except Exception as exc:  # noqa: BLE001 - missing config should fall back safely.
        log.warning("health_dashboard: failed to load STEPS_THRESHOLD; defaulting to %d: %s", STEPS_THRESHOLD, exc)
    return STEPS_THRESHOLD


def _range_filter(start: date | None, end: date) -> dict[str, Any] | None:
    filters: list[dict[str, Any]] = [{"property": "Date", "date": {"on_or_before": end.isoformat()}}]
    if start:
        filters.append({"property": "Date", "date": {"on_or_after": start.isoformat()}})
    if not filters:
        return None
    return {"and": filters} if len(filters) > 1 else filters[0]


def _fetch_health_rows(notion, health_db_id: str, start: date | None, end: date) -> list[dict[str, Any]]:
    query: dict[str, Any] = {"sorts": [{"property": "Date", "direction": "ascending"}]}
    date_filter = _range_filter(start, end)
    if date_filter:
        query["filter"] = date_filter
    return _query_all(notion, health_db_id, **query)


def _fetch_habit_rows(notion, habit_log_db_id: str, start: date | None, end: date) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {"property": "Completed", "checkbox": {"equals": True}},
        {"property": "Date", "date": {"on_or_before": end.isoformat()}},
    ]
    if start:
        filters.append({"property": "Date", "date": {"on_or_after": start.isoformat()}})
    return _query_all(
        notion,
        habit_log_db_id,
        filter={"and": filters},
        sorts=[{"property": "Date", "direction": "ascending"}],
    )


def _normalise_habit_name(name: str) -> str:
    return re.sub(r"^[^\w]+\s*", "", name).strip().lower()


def _habit_matches(props: dict[str, Any], expected: str) -> bool:
    candidates = [
        _extract_plain_text(props.get("Habit Name", {})),
        _extract_plain_text(props.get("Entry", {})),
        _extract_plain_text(props.get("Name", {})),
    ]
    expected_plain = _normalise_habit_name(expected)
    for candidate in candidates:
        if not candidate:
            continue
        if candidate == expected or _normalise_habit_name(candidate) == expected_plain:
            return True
    return False


def _build_metrics(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    metrics = {key: [] for key in METRIC_DEFS}
    for row in rows:
        props = row.get("properties", {})
        date_str = _extract_date(props.get("Date", {}))
        if not date_str:
            continue
        for key, meta in METRIC_DEFS.items():
            value = _extract_number(props.get(meta["property"], {}))
            if value is not None and math.isfinite(value):
                metrics[key].append({"date": date_str, "value": round(value, 2)})
    return metrics


def _direction(delta: float, tolerance: float = 0.05) -> str:
    if abs(delta) <= tolerance:
        return "flat"
    return "up" if delta > 0 else "down"


def _delta_for(points: list[dict[str, Any]], unit: str) -> dict[str, Any] | None:
    if len(points) < 2:
        return None
    delta = float(points[-1]["value"]) - float(points[0]["value"])
    return {"value": round(delta, 2), "unit": unit, "direction": _direction(delta)}


def _trend_signal(points: list[dict[str, Any]], positive_direction: str) -> float | None:
    if len(points) < 2:
        return None
    delta = float(points[-1]["value"]) - float(points[0]["value"])
    direction = _direction(delta)
    # Directional signal normalisation: improving trend=100, flat=50, worsening=0.
    if direction == "flat":
        return 50.0
    return 100.0 if direction == positive_direction else 0.0


def _score_from_signals(signals: list[float | None]) -> int | None:
    valid = [signal for signal in signals if signal is not None]
    if len(valid) < 2:
        return None
    return round(sum(valid) / len(valid))


def _trend_word(points: list[dict[str, Any]], positive_direction: str, label: str) -> str:
    if len(points) < 2:
        return f"{label} needs more data"
    direction = _direction(float(points[-1]["value"]) - float(points[0]["value"]))
    if direction == "flat":
        return f"{label} stable"
    improving = direction == positive_direction
    suffix = "improving" if improving else "worsening"
    return f"{label} {suffix}"


def _body_score(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    # Body Score formula: average weight↓, body-fat↓, and lean-mass↑ directional
    # signals, where improving=100, flat=50, worsening=0.
    value = _score_from_signals([
        _trend_signal(metrics["weight"], "down"),
        _trend_signal(metrics["body_fat"], "down"),
        _trend_signal(metrics["lean_mass"], "up"),
    ])
    if value is None:
        return {"value": None, "status": "no_data", "description": "Not enough body data yet"}
    return {
        "value": value,
        "description": " · ".join([
            _trend_word(metrics["weight"], "down", "Weight"),
            _trend_word(metrics["body_fat"], "down", "Fat"),
            _trend_word(metrics["lean_mass"], "up", "Lean mass"),
        ]),
    }


def _cardio_score(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    # Cardio Score formula: average resting-HR↓, HRV↑, and VO2 Max↑ directional
    # signals, where improving=100, flat=50, worsening=0.
    value = _score_from_signals([
        _trend_signal(metrics["resting_hr"], "down"),
        _trend_signal(metrics["hrv"], "up"),
        _trend_signal(metrics["vo2_max"], "up"),
    ])
    if value is None:
        return {"value": None, "status": "no_data", "description": "Not enough cardio data yet"}
    return {
        "value": value,
        "description": " · ".join([
            _trend_word(metrics["resting_hr"], "down", "Resting HR"),
            _trend_word(metrics["hrv"], "up", "HRV"),
            _trend_word(metrics["vo2_max"], "up", "VO2 Max"),
        ]),
    }


def _weekly_activity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weeks: dict[date, dict[str, set[str]]] = defaultdict(lambda: {"workout": set(), "steps": set()})
    for row in rows:
        props = row.get("properties", {})
        date_str = _extract_date(props.get("Date", {}))
        if not date_str:
            continue
        try:
            day = datetime.fromisoformat(date_str).date()
        except ValueError:
            continue
        week = day - timedelta(days=day.weekday())
        if _habit_matches(props, "💪 Workout"):
            weeks[week]["workout"].add(date_str)
        if _habit_matches(props, "👟 Steps"):
            # Steps success trusts the Habit Log Completed flag from the Notion query.
            # Do not re-apply the raw threshold here; the bot already did that.
            weeks[week]["steps"].add(date_str)
    return [
        {"week": week.isoformat(), "workout_days": len(counts["workout"]), "steps_days": len(counts["steps"])}
        for week, counts in sorted(weeks.items())
    ]


def _activity_score(weekly: list[dict[str, Any]]) -> dict[str, Any]:
    if not weekly:
        return {"value": None, "status": "no_data", "description": "No activity data yet"}
    avg_workouts = sum(w["workout_days"] for w in weekly) / len(weekly)
    avg_steps = sum(w["steps_days"] for w in weekly) / len(weekly)
    workout_pct = min(avg_workouts / 3.0, 1.0)
    steps_pct = min(avg_steps / 7.0, 1.0)
    # Activity Score formula weights workout consistency at 60% and steps goal
    # completion at 40%: ((workout_pct * 0.6) + (steps_pct * 0.4)) * 100.
    value = round(((workout_pct * 0.6) + (steps_pct * 0.4)) * 100)
    latest = weekly[-1]
    return {
        "value": value,
        "description": f"{latest['workout_days']}/7 workout days · {latest['steps_days']}/7 steps days this week",
        "steps_threshold": STEPS_THRESHOLD,
    }


def _latest(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    keys = ["weight", "body_fat", "lean_mass", "resting_hr", "hrv", "vo2_max", "respiratory"]
    return {key: points[-1]["value"] for key in keys if (points := metrics.get(key))}


def _deltas(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ("weight", "body_fat", "lean_mass", "resting_hr", "hrv", "vo2_max"):
        delta = _delta_for(metrics[key], METRIC_DEFS[key]["unit"])
        if delta:
            out[key] = delta
    return out


def build_dashboard_payload(
    *,
    notion,
    health_metrics_db_id: str,
    habit_log_db_id: str,
    range_value: str,
    tz,
) -> dict[str, Any]:
    start, end = _date_window(range_value, tz)
    health_rows = _fetch_health_rows(notion, health_metrics_db_id, start, end)
    habit_rows = _fetch_habit_rows(notion, habit_log_db_id, start, end)
    metrics = _build_metrics(health_rows)
    weekly = _weekly_activity(habit_rows)

    has_any_health_data = any(metrics[key] for key in metrics)
    scores = {
        "body": _body_score(metrics) if has_any_health_data else {"value": None, "status": "no_data", "description": "No body data in this range"},
        "cardio": _cardio_score(metrics) if has_any_health_data else {"value": None, "status": "no_data", "description": "No cardio data in this range"},
        "activity": _activity_score(weekly),
        "sleep": {"value": None, "status": "coming_soon", "description": "Sleep tracking coming soon"},
    }

    return {
        "range": range_value,
        "generated_at": _utc_now_iso(),
        "steps_threshold": STEPS_THRESHOLD,
        "scores": scores,
        "metrics": metrics,
        "weekly_activity": weekly,
        "latest": _latest(metrics),
        "deltas": _deltas(metrics),
    }


def create_health_dashboard_handler(*, notion, health_metrics_db_id: str, habit_log_db_id: str, tz):
    if not health_metrics_db_id:
        raise RuntimeError("NOTION_HEALTH_METRICS_DB is required for /api/health-dashboard")
    if not habit_log_db_id:
        raise RuntimeError("NOTION_HABIT_LOG_DB/NOTION_LOG_DB is required for /api/health-dashboard")

    async def health_dashboard_handler(request: web.Request) -> web.Response:
        range_value = request.query.get("range", "1m").lower()
        if range_value not in VALID_RANGES:
            return web.json_response(
                {"error": "invalid_range", "message": "range must be one of 1m, 3m, 6m, all"},
                status=400,
                headers=cors_headers(),
            )

        now = time.time()
        cached = _RESPONSE_CACHE.get(range_value)
        if cached and now - cached[0] < CACHE_TTL_SECONDS:
            return web.json_response(cached[1], headers=cors_headers())

        try:
            payload = build_dashboard_payload(
                notion=notion,
                health_metrics_db_id=health_metrics_db_id,
                habit_log_db_id=habit_log_db_id,
                range_value=range_value,
                tz=tz,
            )
            _RESPONSE_CACHE[range_value] = (now, payload)
            return web.json_response(payload, headers=cors_headers())
        except Exception as exc:  # noqa: BLE001 - HTTP handler returns JSON errors.
            log.exception("/api/health-dashboard error: %s", exc)
            return web.json_response(
                {"error": "notion_api_failure", "message": str(exc)},
                status=500,
                headers=cors_headers(),
            )

    return health_dashboard_handler


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
