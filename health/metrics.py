import os
import logging
from datetime import date, datetime

import pytz
from notion_client import Client as NotionClient

log = logging.getLogger(__name__)

TZ = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))

# Read at import time so a missing var fails on startup, not at first request.
NOTION_HEALTH_METRICS_DB = os.environ["NOTION_HEALTH_METRICS_DB"]

_notion = NotionClient(auth=os.environ["NOTION_TOKEN"])

# Maps Health Auto Export metric names → Notion property names.
METRIC_MAP: dict[str, str] = {
    "Weight":                   "Weight (kg)",
    "Body Fat Percentage":      "Body Fat %",
    "Lean Body Mass":           "Lean Body Mass (lbs)",
    "Resting Heart Rate":       "Resting Heart Rate (bpm)",
    "Heart Rate Variability":   "HRV (ms)",
    "VO2 Max":                  "VO2 Max",
    "Respiratory Rate":         "Respiratory Rate (brpm)",
    "Apple Exercise Time":      "Exercise Time (min)",
    "Active Energy":            "Active Energy (kcal)",
    "Resting Energy":           "Resting Energy (kcal)",
    "Flights Climbed":          "Flights Climbed",
    "Headphone Audio Exposure": "Headphone Audio Exposure (dB)",
}


def parse_log_date(date_str: str) -> date:
    """Parse a Health Auto Export date string to a local date."""
    dt_utc = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
    return pytz.utc.localize(dt_utc).astimezone(TZ).date()


def _build_notion_props(metrics: dict[str, float]) -> dict:
    return {notion_name: {"number": value} for notion_name, value in metrics.items()}


def upsert_metrics(log_date: date, metrics: dict[str, float]) -> str:
    """
    Upsert daily health metrics into the Notion health-metrics database.

    metrics: {notion_property_name: float_value}

    Upsert strategy (in order):
      1. Name match  — page titled '<date> Log'
      2. Date fallback — page where Date property == log_date
      3. Create      — new page with Name + Date + all metric props

    On update, only the properties present in `metrics` are written;
    existing columns not in this payload are left untouched.

    Returns 'created', 'updated_by_name', or 'updated_by_date'.
    """
    title = f"{log_date.isoformat()} Log"
    date_iso = log_date.isoformat()
    props = _build_notion_props(metrics)

    # Strategy 1: name match
    res = _notion.databases.query(
        database_id=NOTION_HEALTH_METRICS_DB,
        filter={"property": "Name", "title": {"equals": title}},
    )
    pages = res.get("results", [])
    if pages:
        _notion.pages.update(page_id=pages[0]["id"], properties=props)
        log.info(
            "Metrics upsert: updated_by_name  date=%s  props=%s",
            log_date, list(metrics),
        )
        return "updated_by_name"

    # Strategy 2: date property fallback
    res = _notion.databases.query(
        database_id=NOTION_HEALTH_METRICS_DB,
        filter={"property": "Date", "date": {"equals": date_iso}},
    )
    pages = res.get("results", [])
    if pages:
        _notion.pages.update(page_id=pages[0]["id"], properties=props)
        log.info(
            "Metrics upsert: updated_by_date  date=%s  props=%s",
            log_date, list(metrics),
        )
        return "updated_by_date"

    # Strategy 3: create new row
    _notion.pages.create(
        parent={"database_id": NOTION_HEALTH_METRICS_DB},
        properties={
            "Name": {"title": [{"text": {"content": title}}]},
            "Date": {"date": {"start": date_iso}},
            **props,
        },
    )
    log.info(
        "Metrics upsert: created  date=%s  props=%s",
        log_date, list(metrics),
    )
    return "created"


# TEST: POST /api/v1/health-sync with full 12-metric payload → creates new row
# TEST: POST same date again → updates existing row (name match)
# TEST: POST with row existing but name missing → updates by date fallback
# TEST: POST with unknown metric name → skips gracefully, logs warning
# TEST: POST with partial payload (3 metrics only) → updates only those 3 columns
# TEST: POST with malformed JSON → returns HTTP 400
# TEST: Notion DB ID missing from env → raises clear error on startup, not at runtime
# TEST: Headphone Audio Exposure value arrives as dB SPL float → stored correctly
