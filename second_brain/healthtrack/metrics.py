"""Daily Health Auto Export metrics sync → Notion health metrics database.

Endpoint wiring lives in :mod:`second_brain.healthtrack.routes`; this module owns
payload parsing and Notion upsert logic for body/cardio metrics. It is purposely
separate from steps tracking because these metrics use a different Notion
database and have no habit threshold/notification behavior.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

METRIC_PROPERTY_MAP: dict[str, str] = {
    # Human-readable names sent by older Health Auto Export payloads.
    "Weight": "Weight (kg)",
    "Body Fat Percentage": "Body Fat %",
    "Lean Body Mass": "Lean Body Mass (kg)",
    "Resting Heart Rate": "Resting Heart Rate (bpm)",
    "Heart Rate Variability": "HRV (ms)",
    "VO2 Max": "VO2 Max",
    "Respiratory Rate": "Respiratory Rate (brpm)",
    "Apple Exercise Time": "Exercise Time (min)",
    "Active Energy": "Active Energy (kcal)",
    "Resting Energy": "Resting Energy (kcal)",
    "Flights Climbed": "Flights Climbed",
    "Headphone Audio Exposure": "Headphone Audio Exposure (dB)",
    # Snake-case names sent by Health Auto Export v2.
    "weight_body_mass": "Weight (kg)",
    "body_fat_percentage": "Body Fat %",
    "lean_body_mass": "Lean Body Mass (kg)",
    "resting_heart_rate": "Resting Heart Rate (bpm)",
    "heart_rate_variability": "HRV (ms)",
    "vo2_max": "VO2 Max",
    "respiratory_rate": "Respiratory Rate (brpm)",
    "apple_exercise_time": "Exercise Time (min)",
    "active_energy": "Active Energy (kcal)",
    "basal_energy_burned": "Resting Energy (kcal)",
    "flights_climbed": "Flights Climbed",
    "headphone_audio_exposure": "Headphone Audio Exposure (dB)",
}


class MalformedHealthMetricsPayload(ValueError):
    """Raised when a health metrics webhook payload is missing required shape."""


def _parse_export_datetime(raw_date: Any, tz) -> str:
    """Return YYYY-MM-DD for a Health Auto Export timestamp in the app timezone."""
    raw = str(raw_date or "").strip()
    if not raw:
        raise MalformedHealthMetricsPayload("metric data point is missing date")

    # Health Auto Export commonly sends "2026-05-09 21:00:00 +0000". Also accept
    # plain ISO dates for manual tests/backfills.
    if len(raw) >= 10 and len(raw) < 20:
        return raw[:10]

    parsed: datetime | None = None
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue

    if parsed is None:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw[:10]

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    if tz is not None:
        parsed = parsed.astimezone(tz)
    return parsed.strftime("%Y-%m-%d")


def _coerce_metric_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_health_metrics_payload(body: dict, tz) -> tuple[str, dict[str, float], list[str]]:
    """Parse a Health Auto Export metrics payload.

    Returns ``(date_str, notion_property_values, skipped_metric_names)``.
    Unknown metrics are logged and skipped for forward compatibility.
    """
    if not isinstance(body, dict) or "data" not in body:
        raise MalformedHealthMetricsPayload("payload must include a top-level data array")

    data_field = body.get("data")
    if isinstance(data_field, dict):
        # Health Auto Export v2 with Batch Requests ON sends metrics nested here.
        metrics_array = data_field.get("metrics", [])
    elif isinstance(data_field, list):
        # Health Auto Export v1 or Batch Requests OFF sends the metrics array directly.
        metrics_array = data_field
    else:
        raise MalformedHealthMetricsPayload("payload must include a top-level data array")

    if not metrics_array:
        raise MalformedHealthMetricsPayload("data array is empty")

    date_str: str | None = None
    values: dict[str, float] = {}
    skipped: list[str] = []
    metric_names: list[str] = []

    for metric in metrics_array:
        if not isinstance(metric, dict):
            skipped.append(type(metric).__name__)
            log.warning("health_metrics: skipping non-object metric entry: %r", metric)
            continue

        name = str(metric.get("name") or "").strip()
        metric_names.append(name or "<unnamed>")
        readings = metric.get("data")
        if not isinstance(readings, list) or not readings:
            log.warning("health_metrics: metric %r has no data points; skipped", name)
            skipped.append(name)
            continue

        first = readings[0]
        if not isinstance(first, dict):
            log.warning("health_metrics: metric %r first data point is not an object; skipped", name)
            skipped.append(name)
            continue

        if date_str is None:
            date_str = _parse_export_datetime(first.get("date") or first.get("startDate"), tz)

        notion_property = METRIC_PROPERTY_MAP.get(name)
        if not notion_property:
            skipped.append(name or "<unnamed>")
            log.warning("health_metrics: unknown metric %r skipped", name)
            continue

        qty = _coerce_metric_value(
            first.get("qty")
            if first.get("qty") is not None
            else first.get("value")
        )
        if qty is None:
            log.warning("health_metrics: metric %r has non-numeric qty; skipped", name)
            skipped.append(name)
            continue

        values[notion_property] = qty
        log.info("health_metrics: prepared %s=%s", notion_property, qty)

    if date_str is None:
        raise MalformedHealthMetricsPayload("payload contains no dated metric data points")

    log.info(
        "health_metrics: received payload summary date=%s metrics=%s skipped=%s",
        date_str,
        metric_names,
        skipped,
    )
    return date_str, values, skipped


def _title_property(title: str) -> dict:
    return {"title": [{"text": {"content": title}}]}


def _date_property(date_str: str) -> dict:
    return {"date": {"start": date_str}}


def _number_properties(values: dict[str, float]) -> dict[str, dict]:
    return {name: {"number": value} for name, value in values.items()}


def _find_page_by_name_and_date(notion, db_id: str, title: str, date_str: str) -> str | None:
    results = notion.databases.query(
        database_id=db_id,
        filter={
            "and": [
                {"property": "Name", "title": {"equals": title}},
                {"property": "Date", "date": {"equals": date_str}},
            ]
        },
    )
    pages = results.get("results", [])
    return pages[0]["id"] if pages else None


def _find_page_by_date(notion, db_id: str, date_str: str) -> str | None:
    results = notion.databases.query(
        database_id=db_id,
        filter={"property": "Date", "date": {"equals": date_str}},
    )
    pages = results.get("results", [])
    return pages[0]["id"] if pages else None


async def handle_health_metrics_sync(
    *,
    body: dict,
    notion,
    metrics_db_id: str,
    tz,
) -> dict:
    """Upsert daily body/cardio metrics into the Notion health metrics database."""
    date_str, values, skipped = parse_health_metrics_payload(body, tz)
    title = f"{date_str} Log"
    metric_props = _number_properties(values)

    if not values:
        log.info("health_metrics: no recognized metric values for %s; skipping Notion upsert", date_str)
        return {
            "action": "skipped",
            "date": date_str,
            "page_id": None,
            "metrics_written": [],
            "skipped_metrics": skipped,
        }

    try:
        page_id = _find_page_by_name_and_date(notion, metrics_db_id, title, date_str)
        if page_id:
            notion.pages.update(page_id=page_id, properties=metric_props)
            action = "updated_by_name"
            log.info("health_metrics: updated by name page_id=%s date=%s", page_id, date_str)
        else:
            page_id = _find_page_by_date(notion, metrics_db_id, date_str)
            if page_id:
                notion.pages.update(page_id=page_id, properties=metric_props)
                action = "updated_by_date"
                log.info("health_metrics: updated by date page_id=%s date=%s", page_id, date_str)
            else:
                properties = {
                    "Name": _title_property(title),
                    "Date": _date_property(date_str),
                    **metric_props,
                }
                page = notion.pages.create(
                    parent={"database_id": metrics_db_id},
                    properties=properties,
                )
                page_id = page["id"]
                action = "created"
                log.info("health_metrics: created page_id=%s date=%s", page_id, date_str)
    except Exception:
        log.exception("health_metrics: Notion API failure while upserting date=%s", date_str)
        raise

    for prop_name, value in values.items():
        log.info("health_metrics: wrote %s=%s for %s", prop_name, value, date_str)

    return {
        "action": action,
        "date": date_str,
        "page_id": page_id,
        "metrics_written": sorted(values.keys()),
        "skipped_metrics": skipped,
    }


# TEST: POST /api/v1/health-sync with full 12-metric payload → creates new row
# TEST: POST same date again → updates existing row (name match)
# TEST: POST with row existing but name missing → updates by date fallback
# TEST: POST with unknown metric name → skips gracefully, logs warning
# TEST: POST with partial payload (3 metrics only) → updates only those 3 columns
# TEST: POST with malformed JSON → returns HTTP 400
# TEST: Notion DB ID missing from env → raises clear error on startup, not at runtime
# TEST: Headphone Audio Exposure value arrives as dB SPL float → stored correctly
