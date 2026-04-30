"""Centralized environment-backed configuration for Second Brain."""

from __future__ import annotations

import os
from datetime import timedelta

import pytz


def _flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "no", "off"}


def parse_hhmm_env(var_name: str, default: str) -> tuple[int, int]:
    raw = os.environ.get(var_name, default).strip()
    try:
        h_str, m_str = raw.split(":")
        hour, minute = int(h_str), int(m_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("out of range")
        return hour, minute
    except Exception:
        h_str, m_str = default.split(":")
        return int(h_str), int(m_str)


TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
ALERT_CHAT_ID = int(os.environ.get("TELEGRAM_ALERT_CHAT_ID", str(MY_CHAT_ID)))
ALERT_THREAD_ID = int(os.environ["TELEGRAM_ALERT_THREAD_ID"]) if os.environ.get("TELEGRAM_ALERT_THREAD_ID") else None
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
NOTION_HABIT_DB = os.environ["NOTION_HABIT_DB"]
NOTION_LOG_DB = os.environ["NOTION_LOG_DB"]
NOTION_NOTES_DB = os.environ["NOTION_NOTES_DB"]
NOTION_DIGEST_SELECTOR_DB = os.environ["NOTION_DIGEST_SELECTOR_DB"]

TZ = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
RECURRING_CHECK_TIME = parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")
SUNDAY_REVIEW_TIME = parse_hhmm_env("SUNDAY_REVIEW_TIME", "12:00")

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))
CLAUDE_PARSE_MAX_TOKENS = int(os.environ.get("CLAUDE_PARSE_MAX_TOKENS", "4000"))

NOTION_MOVEMENTS_DB = os.environ.get("NOTION_MOVEMENTS_DB", "")
NOTION_CYCLES_DB = os.environ.get("NOTION_CYCLES_DB", "")
NOTION_WORKOUT_PROGRAM_DB = os.environ.get("NOTION_WORKOUT_PROGRAM_DB", "")
NOTION_WORKOUT_LOG_DB = os.environ.get("NOTION_WORKOUT_LOG_DB", "")
NOTION_SUBS_DB = os.environ.get("NOTION_SUBS_DB", "")
NOTION_PRS_DB = os.environ.get("NOTION_PRS_DB", "")
NOTION_WOD_LOG_DB = os.environ.get("NOTION_WOD_LOG_DB", "")

FEATURES = {
    "FEATURE_HABITS": _flag("FEATURE_HABITS", "1"),
    "FEATURE_NOTES": _flag("FEATURE_NOTES", "1"),
    "FEATURE_RECURRING": _flag("FEATURE_RECURRING", "1"),
    "FEATURE_SUNDAY_REVIEW": _flag("FEATURE_SUNDAY_REVIEW", "1"),
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
