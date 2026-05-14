from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_DEFAULT_TZ = "America/Chicago"
_TZ = ZoneInfo(_DEFAULT_TZ)


DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
]

TRACK_NAMES = {"PERFORMANCE": "Performance", "FITNESS": "Fitness", "HYROX": "Hyrox"}
DAY_CANONICAL = {
    "MONDAY": "Monday",
    "TUESDAY": "Tuesday",
    "WEDNESDAY": "Wednesday",
    "THURSDAY": "Thursday",
    "FRIDAY": "Friday",
    "SATURDAY": "Saturday",
    "SUNDAY": "Sunday",
}
DAY_HEADER_RE = re.compile(r"(?im)^[ \t]*(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)[ \t]*:?.*$")
TRACK_HEADER_RE = re.compile(r"(?im)^[ \t]*(PERFORMANCE|FITNESS|HYROX)[ \t]*:?.*$")
SECTION_HEADER_RE = re.compile(r"(?im)^[ \t]*[*_`]*(?:SECTION[ \t]*)?([BC])[*_`]*[ \t]*[\.)][ \t]+(.*)$")
TIME_MARKER_RE = re.compile(r"(?im)^\s*\w[\w\s]+—\s*\d{1,2}:\d{2}-\d{1,2}:\d{2}\s*$")


def _app_tz() -> ZoneInfo:
    try:
        return ZoneInfo(os.environ.get("TIMEZONE", _DEFAULT_TZ))
    except Exception:
        return ZoneInfo(_DEFAULT_TZ)


def _today_str() -> str:
    return datetime.now(_TZ).strftime("%Y-%m-%d")


def _monday_str() -> str:
    today = datetime.now(_TZ).date()
    return (today - timedelta(days=today.weekday())).isoformat()
