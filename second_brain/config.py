"""Centralized environment-backed configuration for Second Brain."""

from __future__ import annotations

import os
from datetime import timedelta

from zoneinfo import ZoneInfo


def _flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "no", "off"}


def parse_hhmm_env(var_name: str, default: str, logger=None) -> tuple[int, int]:
    raw = os.environ.get(var_name, default).strip()
    try:
        h_str, m_str = raw.split(":")
        hour, minute = int(h_str), int(m_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("out of range")
        return hour, minute
    except Exception:
        if logger is not None:
            logger.warning(
                "Invalid %s=%r (expected HH:MM, 24h). Falling back to %s.",
                var_name,
                raw,
                default,
            )
        h_str, m_str = default.split(":")
        return int(h_str), int(m_str)


TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
ALERT_CHAT_ID_RAW = os.getenv("ALERT_CHANNEL_ID", "").strip()
ALERT_CHAT_ID = int(ALERT_CHAT_ID_RAW) if ALERT_CHAT_ID_RAW else None
ALERT_THREAD_ID = int(os.environ["TELEGRAM_ALERT_THREAD_ID"]) if os.environ.get("TELEGRAM_ALERT_THREAD_ID") else None
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
NOTION_HABIT_DB = os.environ["NOTION_HABIT_DB"]
NOTION_LOG_DB = os.environ["NOTION_LOG_DB"]
NOTION_NOTES_DB = os.environ["NOTION_NOTES_DB"]
NOTION_DIGEST_SELECTOR_DB = os.environ["NOTION_DIGEST_SELECTOR_DB"]
NOTION_UTILITY_SCHEDULER_DB = os.environ.get("NOTION_UTILITY_SCHEDULER_DB", "").strip()
# Utility scheduler reload interval (minutes) — how often to refresh digest schedule from Notion
UTILITY_SCHEDULER_RELOAD_MINUTES: int = int(os.environ.get("UTILITY_SCHEDULER_RELOAD_MINUTES", "10"))
NOTION_DAILY_LOG_DB = os.environ.get("NOTION_DAILY_LOG_DB", "")
NOTION_PACKING_ITEMS_DB = os.environ.get("NOTION_PACKING_ITEMS_DB", "")
NOTION_TRIPS_DB = os.environ.get("NOTION_TRIPS_DB", "")
NOTION_CINEMA_LOG_DB = os.environ.get("NOTION_CINEMA_LOG_DB", os.environ.get("NOTION_CINEMA_DB", "")).strip()
NOTION_PERFORMANCE_LOG_DB = os.environ.get("NOTION_PERFORMANCE_LOG_DB", "").strip()
NOTION_SPORTS_LOG_DB = os.environ.get("NOTION_SPORTS_LOG_DB", os.environ.get("NOTION_SPORTS_DB", "")).strip()
NOTION_FAVE_DB = os.environ.get("NOTION_FAVE_DB", "").strip()
NOTION_HEALTH_METRICS_DB = os.environ.get("NOTION_HEALTH_METRICS_DB", "").strip()
NOTION_STREAK_DB = os.environ["NOTION_STREAK_DB"]
NOTION_ENV_DB = os.environ.get("ENV_DB_ID", "").strip()
NOTION_BOOT_LOG_DB = os.environ.get("NOTION_BOOT_LOG_DB", "").strip()

ASANA_SYNC_INTERVAL: int = int(os.environ.get("ASANA_SYNC_INTERVAL", "60"))
HTTP_PORT: int = int(os.environ.get("PORT", "8080"))
WEEKS_HISTORY: int = int(os.environ.get("WEEKS_HISTORY", "52"))
APP_VERSION: str = os.environ.get("APP_VERSION", "v14.0.0")
UV_THRESHOLD: float = float(os.environ.get("UV_THRESHOLD", "3"))

ASANA_PAT = os.environ.get("ASANA_PAT", "")
ASANA_PROJECT_GID = os.environ.get("ASANA_PROJECT_GID", "")
ASANA_WORKSPACE_GID = os.environ.get("ASANA_WORKSPACE_GID", "")
ASANA_SYNC_SOURCE = os.environ.get("ASANA_SYNC_SOURCE", "project").strip().lower()
ASANA_ARCHIVE_ORPHANS = os.environ.get("ASANA_ARCHIVE_ORPHANS", "0").strip().lower() in {"1", "true", "yes", "on"}

NOTION_WATCHLIST_DB = os.environ.get("NOTION_WATCHLIST_DB", "")
NOTION_WANTSLIST_V2_DB = os.environ.get("NOTION_WANTSLIST_V2_DB", "")
NOTION_PHOTO_DB = os.environ.get("NOTION_PHOTO_DB", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_BASE = "https://api.themoviedb.org/3"

OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "").strip()
WEATHER_LOCATION = os.environ.get("WEATHER_LOCATION", "Chicago,IL").strip()

TZ = ZoneInfo(os.environ.get("TIMEZONE", "America/Chicago"))
RECURRING_CHECK_TIME = parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))
CLAUDE_PARSE_MAX_TOKENS = int(os.environ.get("CLAUDE_PARSE_MAX_TOKENS", "4000"))

NOTION_MOVEMENTS_DB = os.environ.get("NOTION_MOVEMENTS_DB", "ecf5ac8381ce41a98fa804a1694977bb").strip()
NOTION_CYCLES_DB = os.environ.get("NOTION_CYCLES_DB", "")
NOTION_WORKOUT_PROGRAM_DB = os.environ.get("NOTION_WEEKLY_PROGRAMS_DB") or os.environ.get("NOTION_WORKOUT_PROGRAM_DB", "")
NOTION_WORKOUT_DAYS_DB = os.environ.get("NOTION_WORKOUT_DAYS_DB", "")
NOTION_WORKOUT_LOG_DB = os.environ.get("NOTION_WORKOUT_LOG_DB", "")
NOTION_WOD_LOG_DB = os.environ.get("NOTION_WOD_LOG_DB", "f94bd9bc79384b53b18bf3d2afaf9881").strip()
NOTION_PROGRESSIONS_DB = os.environ.get("NOTION_PROGRESSIONS_DB", "")
NOTION_DAILY_READINESS_DB = os.environ.get("NOTION_DAILY_READINESS_DB", "")

FEATURES = {
    "FEATURE_RECURRING": _flag("FEATURE_RECURRING", "1"),
}

PENDING_TTL = timedelta(hours=1)

HORIZON_DEADLINE_OFFSETS = {"t": 0, "w": 6, "m": 30, "b": None}
HORIZON_LABELS = {
    "t": "🔴 Today",
    "w": "🟠 This Week",
    "m": "🟡 This Month",
    "b": "⚪ Backburner",
}
NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
