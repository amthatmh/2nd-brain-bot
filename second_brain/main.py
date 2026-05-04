#!/usr/bin/env python3
"""Second Brain — Telegram bot entry point and handler wiring."""

import asyncio
import os
import json
import re
import logging
import calendar
import subprocess
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Callable

from zoneinfo import ZoneInfo
from aiohttp import web
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    BotCommand,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import anthropic
import httpx
from notion_client import Client as NotionClient

from second_brain.asana.sync import (
    reconcile,
    AsanaSyncError,
    validate_notion_schema,
    startup_smoke_test,
)
from second_brain.cinema.sync import sync_cinema_log_to_notion, sync_single_cinema_entry
from second_brain.cinema.config import (
    CINEMA_DB_ID,
    FAVE_DB_ID,
    TMDB_API_KEY,
    validate_config as validate_cinema_config,
)
from second_brain.sync_telemetry import init_sync_status, utc_now_iso, format_sync_status_message
from second_brain.scheduler import register_cinema_jobs
from second_brain.notion import notes as notion_notes
from second_brain.notion import daily_log as notion_daily_log
from second_brain.notes.flow import (
    split_kind_keyboard,
    ordered_topics,
    note_topics_keyboard,
    create_note_payload,
)
from second_brain.ai.classify import claude_classify
from second_brain.ai import classify as ai_classify
from second_brain.healthtrack.routes import register_health_routes
from second_brain.healthtrack.steps import handle_steps_final_stamp
from second_brain.healthtrack.config import (
    STEPS_FINAL_HOUR,
    STEPS_FINAL_MIN,
    STEPS_HABIT_NAME,
    STEPS_THRESHOLD,
    STEPS_SOURCE_LABEL,
)
from second_brain.config import FEATURES
from second_brain.notion import notion_call, notion_call_async
from second_brain.notion import habits as notion_habits
from second_brain.notion import tasks as notion_tasks
from second_brain import keyboards as kb
from second_brain import formatters as fmt
from second_brain import weather as wx
from second_brain import watchlist as wl
from second_brain import trips as trips_mod
from second_brain.state import STATE
from second_brain.utils import ExpiringDict, reply_notion_error
from second_brain.http_utils import cors_headers

from second_brain.crossfit.classify import classify_workout_message, parse_programme
from second_brain.crossfit.handlers import (
    handle_cf_callback,
    handle_cf_prs,
    handle_cf_strength_flow,
    handle_cf_subs_flow,
    handle_cf_text_reply,
    handle_cf_upload_programme,
    handle_cf_wod_flow,
)
from second_brain.crossfit.keyboards import crossfit_submenu_keyboard
from second_brain.crossfit.notion import save_programme_from_notion_row
from second_brain.entertainment import log as ent_log

# Backward-compatible entertainment symbols for existing tests/patch targets.
parse_explicit_entertainment_log = ent_log.parse_explicit_entertainment_log
entertainment_schemas = ent_log.entertainment_schemas
pending_sport_competition_map = ent_log.pending_sport_competition_map
_build_common_entertainment_props = ent_log._build_common_entertainment_props
_normalize_entertainment_datetime = ent_log._normalize_entertainment_datetime
_parse_cinema_inline_context = ent_log._parse_cinema_inline_context
_strip_cinema_structured_notes = ent_log._strip_cinema_structured_notes
_strip_datetime_from_notes = ent_log._strip_datetime_from_notes
_strip_seat_from_notes = ent_log._strip_seat_from_notes
_extract_cinema_visit_details = ent_log._extract_cinema_visit_details
_entertainment_save_error_text = ent_log._entertainment_save_error_text
_build_sport_competition_props = ent_log._build_sport_competition_props
_ent_log_create_entertainment_log_entry = ent_log.create_entertainment_log_entry


def _sync_ent_log_runtime() -> None:
    ent_log.notion_call = notion_call
    ent_log.NOTION_CINEMA_LOG_DB = NOTION_CINEMA_LOG_DB
    ent_log.NOTION_PERFORMANCE_LOG_DB = NOTION_PERFORMANCE_LOG_DB
    ent_log.NOTION_SPORTS_LOG_DB = NOTION_SPORTS_LOG_DB
    ent_log.NOTION_FAVE_DB = NOTION_FAVE_DB


def create_entertainment_log_entry(notion, payload: dict) -> tuple[str, bool]:
    _sync_ent_log_runtime()
    return _ent_log_create_entertainment_log_entry(notion, payload)


async def handle_entertainment_log(notion, message, payload: dict) -> None:
    _sync_ent_log_runtime()
    entry_id, fav_saved = create_entertainment_log_entry(notion, payload)
    title = payload.get("title", "Untitled")
    log_type = payload.get("log_type", "cinema")
    venue = payload.get("venue")
    notes = payload.get("notes")
    when_iso = payload.get("date") or date.today().isoformat()

    summary_lines = [
        f"✅ Logged to { {'cinema': 'Cinema', 'performance': 'Performance', 'sport': 'Sports'}.get(log_type, 'Entertainment') }",
        "",
        f"🎫 {title}",
        f"📅 {when_iso}",
    ]
    if venue:
        summary_lines.append(f"📍 {venue}")
    if notes:
        summary_lines.append(f"📝 {notes}")
    if fav_saved and log_type == "cinema":
        summary_lines.append("🎞️ Added to Favourite Films")
    summary_lines.append("")
    summary_lines.append("_Saved to Notion_")
    await message.reply_text("\n".join(summary_lines), parse_mode="Markdown")
    if log_type == "sport":
        ent_log._remember_pending_sport_competition(message, entry_id)
        await message.reply_text("🏆 Logged to Sports Log. Which competition should I set for this one?")
    log.info("Entertainment logged type=%s title=%s page_id=%s", log_type, title, entry_id)


async def _maybe_prompt_explicit_venue(notion, message, payload: dict, raw_text: str) -> bool:
    _sync_ent_log_runtime()
    return await ent_log._maybe_prompt_explicit_venue(notion, message, payload, raw_text)


def load_entertainment_schemas(notion) -> None:
    _sync_ent_log_runtime()
    ent_log.load_entertainment_schemas(notion)



def _resolve_known_cinema_venue(venue: str | None, schema: dict) -> str | None:
    _sync_ent_log_runtime()
    return ent_log._resolve_known_cinema_venue(notion, venue, schema)


def _find_existing_cinema_venue(title: str, schema: dict) -> str | None:
    _sync_ent_log_runtime()
    return ent_log._find_existing_cinema_venue(notion, title, schema)


def _suggest_known_venue(payload: dict) -> tuple[str | None, str | None]:
    _sync_ent_log_runtime()
    return ent_log._suggest_known_venue(notion, payload)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)


def _parse_hhmm_env(var_name: str, default: str) -> tuple[int, int]:
    """Parse HH:MM env var with range checks and safe fallback."""
    raw = os.environ.get(var_name, default).strip()
    try:
        h_str, m_str = raw.split(":")
        hour, minute = int(h_str), int(m_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("out of range")
        return hour, minute
    except Exception:
        log.warning(
            "Invalid %s=%r (expected HH:MM, 24h). Falling back to %s.",
            var_name,
            raw,
            default,
        )
        h_str, m_str = default.split(":")
        return int(h_str), int(m_str)


def _resolve_state_dir() -> Path:
    """
    Pick a durable location for bot state files.

    Priority:
    1) BOT_STATE_DIR env override.
    2) /data (common mounted persistent disk path on PaaS providers).
    3) ~/.second_brain_bot (stable fallback across varying working dirs).
    4) Current working directory.
    """
    override = os.environ.get("BOT_STATE_DIR", "").strip()
    if override:
        state_dir = Path(override).expanduser()
    elif Path("/data").exists():
        state_dir = Path("/data")
    elif Path.home().exists():
        state_dir = Path.home() / ".second_brain_bot"
    else:
        state_dir = Path.cwd()

    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        return state_dir
    except Exception as e:
        log.warning("Unable to use BOT_STATE_DIR=%s (%s). Falling back to cwd.", state_dir, e)
        fallback = Path.cwd()
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

# ── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID      = int(os.environ["TELEGRAM_CHAT_ID"])
ALERT_CHAT_ID   = int(os.environ.get("TELEGRAM_ALERT_CHAT_ID", str(MY_CHAT_ID)))
ALERT_THREAD_ID = int(os.environ["TELEGRAM_ALERT_THREAD_ID"]) if os.environ.get("TELEGRAM_ALERT_THREAD_ID") else None
ANTHROPIC_KEY   = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN    = os.environ["NOTION_TOKEN"]
NOTION_DB_ID    = os.environ["NOTION_DB_ID"]
NOTION_HABIT_DB = os.environ["NOTION_HABIT_DB"]
NOTION_LOG_DB   = os.environ["NOTION_LOG_DB"]
NOTION_STREAK_DB = os.environ["NOTION_STREAK_DB"]
NOTION_CINEMA_LOG_DB = os.environ.get("NOTION_CINEMA_LOG_DB", os.environ.get("NOTION_CINEMA_DB", "")).strip()
NOTION_PERFORMANCE_LOG_DB = os.environ.get("NOTION_PERFORMANCE_LOG_DB", "").strip()
NOTION_SPORTS_LOG_DB = os.environ.get("NOTION_SPORTS_LOG_DB", os.environ.get("NOTION_SPORTS_DB", "")).strip()
NOTION_FAVE_DB = os.environ.get("NOTION_FAVE_DB", "").strip()
NOTION_NOTES_DB = os.environ["NOTION_NOTES_DB"]    # 📒 Notes
NOTION_DIGEST_SELECTOR_DB = os.environ["NOTION_DIGEST_SELECTOR_DB"]
NOTION_DAILY_LOG_DB = os.environ.get("NOTION_DAILY_LOG_DB", "")
NOTION_PACKING_ITEMS_DB = os.environ.get("NOTION_PACKING_ITEMS_DB", "")
NOTION_TRIPS_DB         = os.environ.get("NOTION_TRIPS_DB", "")
OPENWEATHER_KEY     = os.environ.get("OPENWEATHER_KEY", "")

TZ           = ZoneInfo(os.environ.get("TIMEZONE", "America/Chicago"))
_rc_h, _rc_m = _parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")
_sr_h, _sr_m = _parse_hhmm_env("SUNDAY_REVIEW_TIME", "12:00")

CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))
CLAUDE_PARSE_MAX_TOKENS = int(os.environ.get("CLAUDE_PARSE_MAX_TOKENS", "4000"))
NOTION_MOVEMENTS_DB = os.environ.get("NOTION_MOVEMENTS_DB", "")
NOTION_CYCLES_DB = os.environ.get("NOTION_CYCLES_DB", "")
NOTION_WORKOUT_PROGRAM_DB = os.environ.get("NOTION_WORKOUT_PROGRAM_DB", "")
NOTION_WORKOUT_DAYS_DB = os.environ.get("NOTION_WORKOUT_DAYS_DB", "")
NOTION_WORKOUT_LOG_DB = os.environ.get("NOTION_WORKOUT_LOG_DB", "")
NOTION_SUBS_DB = os.environ.get("NOTION_SUBS_DB", "")
NOTION_PRS_DB = os.environ.get("NOTION_PRS_DB", "")
NOTION_WOD_LOG_DB = os.environ.get("NOTION_WOD_LOG_DB", "")
HTTP_PORT      = int(os.environ.get("PORT", "8080"))
WEEKS_HISTORY  = int(os.environ.get("WEEKS_HISTORY", "52"))
APP_VERSION    = os.environ.get("APP_VERSION", "v13.3.0")
OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "").strip()
WEATHER_LOCATION = os.environ.get("WEATHER_LOCATION", "Chicago,IL").strip()
NOTION_ENV_DB = os.environ.get("ENV_DB_ID", "").strip()
UV_THRESHOLD = float(os.environ.get("UV_THRESHOLD", "3"))
SUNDAY_REVIEW_CARD_LIMIT = max(1, int(os.environ.get("SUNDAY_REVIEW_CARD_LIMIT", "6")))

# ── Asana sync config ────────────────────────────────────────────────────────
ASANA_PAT           = os.environ.get("ASANA_PAT", "")
ASANA_PROJECT_GID   = os.environ.get("ASANA_PROJECT_GID", "")
ASANA_WORKSPACE_GID = os.environ.get("ASANA_WORKSPACE_GID", "")  # v9.2: required for my_tasks mode
ASANA_SYNC_SOURCE   = os.environ.get("ASANA_SYNC_SOURCE", "project").strip().lower()
ASANA_SYNC_INTERVAL = max(1, int(os.environ.get("ASANA_SYNC_EVERY_SECONDS", "15")))
ASANA_SYNC_TIMEOUT_SECONDS = max(10, int(os.environ.get("ASANA_SYNC_TIMEOUT_SECONDS", "45")))
ASANA_SYNC_MAX_INSTANCES = max(1, int(os.environ.get("ASANA_SYNC_MAX_INSTANCES", "1")))
ASANA_SYNC_MISFIRE_GRACE_SECONDS = max(1, int(os.environ.get("ASANA_SYNC_MISFIRE_GRACE_SECONDS", "20")))
ASANA_STARTUP_SMOKE = os.environ.get("ASANA_STARTUP_SMOKE", "1").strip().lower() not in {"0", "false", "no", "off"}
ASANA_ARCHIVE_ORPHANS = os.environ.get("ASANA_ARCHIVE_ORPHANS", "0").strip().lower() in {"1", "true", "yes", "on"}
NOTION_WATCHLIST_DB    = os.environ.get("NOTION_WATCHLIST_DB", "")
NOTION_WANTSLIST_V2_DB = os.environ.get("NOTION_WANTSLIST_V2_DB", "")
NOTION_PHOTO_DB        = os.environ.get("NOTION_PHOTO_DB", "")
TMDB_BASE              = "https://api.themoviedb.org/3"


def local_today() -> date:
    """Return today's date in the configured app timezone."""
    return datetime.now(TZ).date()


def get_current_monday() -> date:
    """Return Monday date for the current week in local time."""
    today = datetime.now(TZ).date()
    if today.weekday() == 0:
        return today
    return today - timedelta(days=today.weekday())

def format_reminder_snapshot(mode: str = "priority", limit: int = 8) -> str:
    fmt.local_today = local_today
    fmt.notion = notion
    fmt.NOTION_DB_ID = NOTION_DB_ID
    fmt.TZ = TZ
    fmt.notion_tasks = notion_tasks
    return fmt.format_reminder_snapshot(mode=mode, limit=limit)


# ── Clients ──────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
wx.notion = notion
wx.NOTION_ENV_DB = NOTION_ENV_DB
wx.current_location = WEATHER_LOCATION

# ── In-memory state ──────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = {}
last_digest_msg_id: int | None = None
pending_map: dict[str, dict] = ExpiringDict(ttl_seconds=3600)
capture_map: dict[int, dict] = {}
done_picker_map: dict[str, list[dict]] = ExpiringDict(ttl_seconds=3600)
todo_picker_map: dict[str, list[dict]] = {}
pending_message_map: dict[str, str] = {}
pending_note_map: dict[str, dict] = {}
cf_pending: dict[str, dict] = ExpiringDict(ttl_seconds=3600)
topic_recency_map: dict[str, datetime] = {}
_cf_counter = 0
_done_picker_counter = 0
_todo_picker_counter = 0
_v10_counter = 0
_entertainment_counter = 0
habit_cache: dict[str, dict] = STATE.habit_cache

def _refresh_habit_cache_refs() -> None:
    global habit_cache
    habit_cache = notion_habits.habit_cache
    STATE.habit_cache = habit_cache

notes_pending: set[int] = STATE.notes_pending  # chat_ids currently in note-capture mode
sync_status: dict[str, dict] = init_sync_status()
trip_map: dict[str, dict] = {}
trip_awaiting_date_map: dict[int, str] = {}
awaiting_packing_feedback = False
_trip_counter = 0

_digest_jobs: list = []
_scheduler: AsyncIOScheduler | None = None
_digest_slots_last_load_succeeded = False
_digest_catchup_sent: set[str] = set()
_digest_slot_sent_today: set[str] = set()
notified_goals_this_week: set[str] = set()
mute_until: datetime | None = None
_signoff_note_today: str = ""
_last_daily_log_url: str = ""
_app_bot = None  # set during post_init for health route bot access
STATE_DIR = _resolve_state_dir()
mute_state_file = STATE_DIR / "mute_state.json"

# ── Constants ────────────────────────────────────────────────────────────────
HORIZON_DEADLINE_OFFSETS = {"t": 0, "w": 6, "m": 30, "b": None}
HORIZON_LABELS = {
    "t": "🔴 Today", "w": "🟠 This Week",
    "m": "🟡 This Month", "b": "⚪ Backburner",
}
NUMBER_EMOJIS = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
REPEAT_DAY_TO_WEEKDAY  = {"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}
REPEAT_DAY_TO_MONTHDAY = {
    **{
        f"{d}{'th' if 10 <= d % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')}": d
        for d in range(1, 32)
    },
    "Last": -1,
}
_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)
BTN_REFRESH = "📜Digest"
BTN_ALL_OPEN = "✅To Do"
BTN_HABITS = "🏃 Habits"
BTN_CROSSFIT = "💪 CrossFit"
BTN_NOTES = "📝 Notes"
BTN_WEATHER = "🌤️ Weather"
BTN_MUTE = "🔕 Mute"
ENTERTAINMENT_LOG_LABELS = {
    "cinema": "🍿 Cinema Log",
    "performance": "🎟️ Performances Viewings",
    "sport": "🏟️ Sports Log",
}
LEGACY_BTN_ALL_OPEN = "📋 All Open"
TOPIC_OPTIONS = [
    "🎵 Acoustics", "💼 Work", "🏠 Personal",
    "💪 Health", "🏢 LEED", "✅ WELL", "💡 Ideas", "📚 Research",
]
_URL_RE = re.compile(r"https?://[^\s\)\]>\"']+", re.IGNORECASE)



def next_weekday(weekday: int) -> date:
    today = local_today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


def _has_explicit_personal_or_work_context(text: str) -> bool:
    lower = (text or "").lower()
    return bool(re.search(r"\b(personal|work)\b|🏠|💼", lower))


def next_repeat_day_date(
    recurring: str,
    repeat_day: str | None,
    today: date | None = None,
    *,
    anchor: date | None = None,
) -> date | None:
    """Resolve the next occurrence date for weekly/monthly/quarterly repeat settings."""
    if not repeat_day:
        return None
    today = today or local_today()

    if recurring == "📅 Weekly" and repeat_day in REPEAT_DAY_TO_WEEKDAY:
        weekday = REPEAT_DAY_TO_WEEKDAY[repeat_day]
        days_ahead = (weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return today + timedelta(days=days_ahead)

    if recurring == "🗓️ Monthly":
        for month_offset in (0, 1):
            year = today.year + ((today.month - 1 + month_offset) // 12)
            month = ((today.month - 1 + month_offset) % 12) + 1
            month_last_day = calendar.monthrange(year, month)[1]
            if repeat_day == "Last":
                target_day = month_last_day
            else:
                day_value = REPEAT_DAY_TO_MONTHDAY.get(repeat_day)
                if day_value is None:
                    return None
                target_day = min(day_value, month_last_day)
            target = date(year, month, target_day)
            if target >= today:
                return target
        return None

    if recurring == "📆 Quarterly":
        if repeat_day != "Last" and repeat_day not in REPEAT_DAY_TO_MONTHDAY:
            return None
        if anchor:
            quarter_cycle = (anchor.month - 1) % 3
        else:
            quarter_cycle = (today.month - 1) % 3

        for months_ahead in range(0, 16):
            year = today.year + ((today.month - 1 + months_ahead) // 12)
            month = ((today.month - 1 + months_ahead) % 12) + 1
            if (month - 1) % 3 != quarter_cycle:
                continue
            month_last_day = calendar.monthrange(year, month)[1]
            if repeat_day == "Last":
                target_day = month_last_day
            else:
                day_value = REPEAT_DAY_TO_MONTHDAY.get(repeat_day)
                if day_value is None:
                    return None
                target_day = min(day_value, month_last_day)
            target = date(year, month, target_day)
            if target >= today:
                return target
        return None

    return None


def _parse_time_to_minutes(time_str: str | None) -> int:
    """
    Parse "HH:MM" format to minutes since midnight.
    Returns -1 if parse fails (will sort to end).
    """
    if not time_str:
        return -1
    try:
        hhmm = str(time_str).strip()
        hour_str, minute_str = hhmm.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return -1
        return hour * 60 + minute
    except Exception:
        return -1


def save_mute_state() -> None:
    """Persist mute state to disk."""
    try:
        payload = {"mute_until": mute_until.isoformat() if mute_until else None}
        mute_state_file.write_text(json.dumps(payload))
    except Exception as e:
        log.error("Failed saving mute state: %s", e)


def load_mute_state() -> None:
    """Load mute state from disk and clear expired mute windows."""
    global mute_until
    mute_until = None
    try:
        if not mute_state_file.exists():
            return
        payload = json.loads(mute_state_file.read_text() or "{}")
        raw = payload.get("mute_until")
        if raw:
            parsed = datetime.fromisoformat(raw)
            mute_until = parsed
        if mute_until and datetime.now(TZ) >= mute_until:
            mute_until = None
            save_mute_state()
    except Exception as e:
        log.error("Failed loading mute state: %s", e)
        mute_until = None


def is_muted() -> bool:
    """Return True if digest jobs are currently muted."""
    global mute_until
    if not mute_until:
        return False
    if datetime.now(TZ) >= mute_until:
        mute_until = None
        save_mute_state()
        return False
    return True



# ══════════════════════════════════════════════════════════════════════════════
# HABIT CACHE
# ══════════════════════════════════════════════════════════════════════════════

def notion_query_all(database_id: str, **kwargs) -> list[dict]:
    """Return all rows from a Notion database query (handles pagination)."""
    rows: list[dict] = []
    cursor = None

    while True:
        query_args = dict(kwargs)
        if cursor:
            query_args["start_cursor"] = cursor
        resp = notion.databases.query(database_id=database_id, **query_args)
        rows.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return rows


def load_digest_slots() -> list[dict]:
    """
    Queries Notion Digest Selector DB.
    Returns list of slot dicts.
    """
    context_map = {
        "🏠 Personal": "🏠 Personal",
        "💼 Work": "💼 Work",
        "🏃 Health": "🏃 Health",
        "🤝 HK": "🤝 HK",
    }
    def first_text(prop: dict) -> str:
        rich_text = prop.get("rich_text", [])
        if rich_text:
            return (rich_text[0].get("plain_text") or "").strip()
        title = prop.get("title", [])
        if title:
            return (title[0].get("plain_text") or "").strip()
        select = prop.get("select")
        if select and select.get("name"):
            return (select.get("name") or "").strip()
        date_value = prop.get("date") or {}
        if isinstance(date_value, dict) and date_value.get("start"):
            return str(date_value.get("start")).strip()
        return ""

    def normalize_slot_time(raw: str) -> str | None:
        value = (raw or "").strip()
        if not value:
            return None

        # Accept "HH:MM" / "H:MM", optionally with seconds.
        iso_match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", value)
        if iso_match:
            hh = int(iso_match.group(1))
            mm = int(iso_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
            return None

        # Accept "H:MM AM/PM" formats commonly used in select labels.
        ampm_match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])", value)
        if ampm_match:
            hh = int(ampm_match.group(1))
            mm = int(ampm_match.group(2))
            ampm = ampm_match.group(3).lower()
            if not (1 <= hh <= 12 and 0 <= mm <= 59):
                return None
            if ampm == "am":
                hh = 0 if hh == 12 else hh
            else:
                hh = 12 if hh == 12 else hh + 12
            return f"{hh:02d}:{mm:02d}"

        # If Notion date-time string is provided (e.g. 2026-04-26T09:00:00.000Z), parse time part.
        dt_match = re.search(r"T(\d{2}):(\d{2})", value)
        if dt_match:
            hh = int(dt_match.group(1))
            mm = int(dt_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"

        internal_match = re.search(r"\b(\d{1,2}):(\d{2})\b", value)
        if internal_match:
            hh = int(internal_match.group(1))
            mm = int(internal_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
        return None

    slots: list[dict] = []
    seen_slot_keys: set[tuple[str, bool]] = set()
    rows = notion_query_all(NOTION_DIGEST_SELECTOR_DB)
    for row in rows:
        props = row.get("properties", {})

        slot_time_raw = first_text(props.get("Time", {}))
        slot_time = normalize_slot_time(slot_time_raw)
        if not slot_time:
            log.warning("Skipping digest selector row with invalid Time=%r", slot_time_raw)
            continue
        hh, mm = map(int, slot_time.split(":"))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            log.warning("Skipping digest selector row with out-of-range Time=%r", slot_time_raw)
            continue
        if not slot_time:
            continue

        ww = props.get("Weekday/Weekend", {}).get("select")
        ww_name = (ww.get("name") if ww else "").strip()
        ww_norm = ww_name.lower()
        is_all = ww_norm in {"all", "every day", "daily", "always"}
        is_weekday_val = ww_norm in {"weekday", "weekdays", "mon-fri"}
        is_weekend_val = ww_norm in {"weekend", "weekends", "sat,sun", "sat/sun"}

        if not (is_all or is_weekday_val or is_weekend_val):
            log.warning("Skipping digest selector row with invalid Weekday/Weekend=%r", ww_name)
            continue

        # "All" rows are expanded into two slots: one weekday, one weekend.
        # Build the list of (is_weekday, slot_key) pairs to append.
        weekday_variants: list[bool] = []
        if is_all:
            weekday_variants = [True, False]
        elif is_weekday_val:
            weekday_variants = [True]
        else:
            weekday_variants = [False]

        include_habits = bool(props.get("Habits", {}).get("checkbox", False))
        max_items_raw = props.get("Max Items", {}).get("number")
        max_items = int(max_items_raw) if isinstance(max_items_raw, (int, float)) else None

        selected_contexts = [
            context_label
            for prop_name, context_label in context_map.items()
            if bool(props.get(prop_name, {}).get("checkbox", False))
        ]
        # Keep an explicit empty-list when no context checkboxes are selected so
        # a slot can intentionally send a habits-only digest without task spillover.
        contexts = selected_contexts

        is_signoff = bool(props.get("Signoff", {}).get("checkbox", False))

        for is_weekday in weekday_variants:
            slot_key = (slot_time, is_weekday)
            if slot_key in seen_slot_keys:
                log.warning(
                    "Skipping duplicate digest selector slot %s (%s)",
                    slot_time,
                    "weekday" if is_weekday else "weekend",
                )
                continue
            seen_slot_keys.add(slot_key)
            slots.append(
                {
                    "time": slot_time,
                    "is_weekday": is_weekday,
                    "include_habits": include_habits,
                    "max_items": max_items,
                    "contexts": contexts,
                    "is_signoff": is_signoff,
                }
            )

    log.info("Loaded %d digest selector slot(s) from Notion", len(slots))
    return slots


def extract_date_only(date_str: str | None) -> str | None:
    """Normalize Notion date strings to YYYY-MM-DD for calendar matching."""
    if not date_str:
        return None
    if len(date_str) >= 10 and date_str[4] == "-" and date_str[7] == "-":
        return date_str[:10]
    return date_str


# ══════════════════════════════════════════════════════════════════════════════
# MULTI-TASK PARSING
# ══════════════════════════════════════════════════════════════════════════════

def split_tasks(text: str) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(_BULLET_RE.match(l) for l in lines):
        tasks = [_BULLET_RE.sub("", l).strip() for l in lines if _BULLET_RE.match(l)]
        return tasks if len(tasks) > 1 else [text]
    if len(lines) > 1:
        lower = text.lower()
        if re.search(r"\bschedule\b.*\brecurring\b", lower) and re.search(r"\bevery\b", lower):
            return [text]
        return lines
    return [text]


def looks_like_crossfit_programme(text: str) -> bool:
    lower = text.lower()
    day_hits = len(re.findall(r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)\b", lower))
    section_hits = len(re.findall(r"(?:^|\n)\s*[bc]\.", lower))
    workout_hits = len(re.findall(r"\b(amrap|emom|for time|rounds?|reps?|wod|snatch|clean|jerk|burpee|row|sit ups?|pushups?)\b", lower))
    # Accept both full-week uploads and single-day programme blocks.
    if day_hits >= 2 and (section_hits >= 2 or workout_hits >= 3):
        return True
    return day_hits >= 1 and (section_hits >= 1 or workout_hits >= 4)


def looks_like_task_batch(text: str) -> bool:
    if looks_like_crossfit_programme(text):
        return False
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) <= 1:
        return False
    numbered_or_bulleted = sum(1 for l in lines if _BULLET_RE.match(l))
    if numbered_or_bulleted >= 2:
        return True
    lead = lines[0].lower()
    if lead in {"add", "todo", "to-do", "tasks"}:
        return True
    return False


def infer_batch_overrides(text: str) -> dict:
    lower = text.lower()
    context = None
    context_aliases = [
        ("💼 Work", ["work", "💼"]),
        ("🏠 Personal", ["personal", "🏠"]),
        ("🏃 Health", ["health", "🏃"]),
        ("🤝 Collab", ["collab", "🤝"]),
    ]

    explicit_scope = re.search(r"\b(?:under|for|in)\s+([^\n,.;:]+)", lower)
    scoped_text = explicit_scope.group(1) if explicit_scope else ""
    haystacks = [scoped_text, lower] if scoped_text else [lower]

    for hay in haystacks:
        for notion_context, aliases in context_aliases:
            if any((a in hay) if not a.isalpha() else re.search(rf"\b{re.escape(a)}\b", hay) for a in aliases):
                context = notion_context
                break
        if context:
            break

    deadline_days = None
    if re.search(r"\btomorrow\b", lower):
        deadline_days = 1
    elif re.search(r"\b(?:today|tonight)\b", lower):
        deadline_days = 0
    elif re.search(r"\bthis week\b", lower):
        deadline_days = 5
    elif re.search(r"\bthis month\b", lower):
        deadline_days = 20

    return {"context": context, "deadline_days": deadline_days}


# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

async def start_note_capture_flow(message, text: str) -> None:
    if not NOTION_NOTES_DB:
        await create_or_prompt_task(message, text)
        return

    global _v10_counter
    note_key = str(_v10_counter)
    _v10_counter += 1
    try:
        topics = notion_notes.fetch_note_topics_from_notion(notion, NOTION_NOTES_DB)
    except Exception as e:
        log.error(f"Failed to read note topics from Notion schema: {e}")
        await message.reply_text("⚠️ Couldn't load note topics from Notion. Check the Topic property.")
        return

    ordered = ordered_topics(topics, topic_recency_map)
    pending_note_map[note_key] = {"content": text, "topic_order": ordered}
    if ordered:
        await message.reply_text(
            "📝 Got it — choose a topic tag:",
            reply_markup=note_topics_keyboard(note_key, ordered),
        )
        return

    try:
        notion_notes.create_note_entry(notion, NOTION_NOTES_DB, text)
        await message.reply_text("✅ Note captured!\n_Saved to Notion_", parse_mode="Markdown")
    except Exception as e:
        log.error(f"Notion note error: {e}")
        await message.reply_text("⚠️ Couldn't save note to Notion.")


def extract_url(text: str) -> str | None:
    """Return first URL found in text, or None."""
    m = _URL_RE.search(text)
    return m.group(0) if m else None


def fetch_url_metadata(url: str) -> dict:
    """Fetch page title and meta description. Returns {title, description}."""
    import urllib.request
    import html
    title, description = "", ""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SecondBrainBot/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw = resp.read(32768).decode("utf-8", errors="replace")
        tm = re.search(r"<title[^>]*>(.*?)</title>", raw, re.IGNORECASE | re.DOTALL)
        if tm:
            title = html.unescape(re.sub(r"\s+", " ", tm.group(1))).strip()[:200]
        dm = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{0,300})',
            raw, re.IGNORECASE,
        ) or re.search(
            r'<meta[^>]+content=["\']([^"\']{0,300})[^>]+name=["\']description["\']',
            raw, re.IGNORECASE,
        )
        if dm:
            description = html.unescape(dm.group(1)).strip()
    except Exception as e:
        log.warning(f"fetch_url_metadata failed for {url}: {e}")
    return {"title": title, "description": description}


async def handle_note_input(message, text: str) -> None:
    """Called when user sends content in note-capture mode."""
    chat_id = message.chat_id
    notes_pending.discard(chat_id)
    url = extract_url(text)
    thinking = await message.reply_text("📒 Saving note...")
    try:
        if url:
            parsed = urllib.parse.urlsplit(url)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError("invalid URL")
            meta = await asyncio.get_running_loop().run_in_executor(
                None, fetch_url_metadata, url
            )
            classified = await asyncio.get_running_loop().run_in_executor(
                None, ai_classify.classify_note,
                claude, CLAUDE_MODEL, meta["title"], meta["description"], url, text, TOPIC_OPTIONS,
            )
            note_title = classified["title"]
            topics = classified["topics"]
            content = meta["description"]
            note_type = "🔗 Link/Article"
        else:
            note_title = text[:80]
            topics = ["💡 Ideas"]
            content = text
            note_type = "📝 Quick Note"

        notion_notes.save_note(notion, NOTION_NOTES_DB, note_title, url, content, topics, note_type)
        icon = "🔗" if url else "📝"
        topic_str = "  ".join(topics)
        await thinking.edit_text(
            f"📒 Saved!\n\n{icon} *{note_title}*\n🏷 {topic_str}\n\n_Saved to Notion_",
            parse_mode="Markdown",
        )
    except Exception as e:
        log.error(f"save_note error: {e}")
        await thinking.edit_text(f"⚠️ Couldn't save note to Notion.\n_{e}_", parse_mode="Markdown")


def deadline_days_to_label(days: int | None) -> str:
    if days is None: return "⚪ Backburner"
    if days <= 0:    return "🔴 Today"
    if days <= 7:    return "🟠 This Week"
    if days <= 31:   return "🟡 This Month"
    return "⚪ Backburner"


# ══════════════════════════════════════════════════════════════════════════════
# V10 REFERENCE DATABASE FLOWS
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# NOTION — HABIT LOG
# ══════════════════════════════════════════════════════════════════════════════

def log_habit(habit_page_id: str, habit_name: str, source: str = "📱 Telegram") -> None:
    today = datetime.now(TZ).date().isoformat()
    props = {
        "Entry":     {"title":    [{"text": {"content": habit_name}}]},
        "Habit":     {"relation": [{"id": habit_page_id}]},
        "Completed": {"checkbox": True},
        "Date":      {"date":     {"start": today}},
        "Source":    {"select":   {"name": source}},
    }
    try:
        notion.pages.create(
            parent={"database_id": NOTION_LOG_DB},
            properties=props,
        )
    except Exception as e:
        # Some log DBs do not expose/allow Source; retry with core fields only.
        log.warning("Habit log create retrying without Source: %s", e)
        minimal = {k: v for k, v in props.items() if k != "Source"}
        notion.pages.create(
            parent={"database_id": NOTION_LOG_DB},
            properties=minimal,
        )
    log.info(f"Habit logged: {habit_name} on {today} via {source}")


def already_logged_today(habit_page_id: str) -> bool:
    today = datetime.now(TZ).date().isoformat()
    try:
        results = notion.databases.query(
            database_id=NOTION_LOG_DB,
            filter={
                "and": [
                    {"property": "Habit",     "relation":  {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox":  {"equals": True}},
                    {"property": "Date",      "date":      {"equals": today}},
                ]
            },
        )
        return len(results.get("results", [])) > 0
    except Exception as e:
        # Avoid blocking one-tap habit logs when the dedupe query schema drifts.
        log.warning("already_logged_today query failed for %s: %s", habit_page_id, e)
        return False


def get_week_completion_count(habit_page_id: str) -> int:
    try:
        results = notion.databases.query(
            database_id=NOTION_LOG_DB,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after": get_current_monday().isoformat()}},
                ]
            },
        )
        return len(results.get("results", []))
    except Exception as e:
        log.error("Error counting weekly completions for habit %s: %s", habit_page_id, e)
        return 0


def get_habit_frequency(habit_page_id: str) -> int:
    try:
        page = notion.pages.retrieve(page_id=habit_page_id)
        properties = page.get("properties", {})
        frequency = notion_habits.extract_habit_frequency(properties)
        if frequency and frequency > 0:
            return frequency
        return 7
    except Exception as e:
        log.error("Error reading habit frequency for %s: %s", habit_page_id, e)
        return 7


def habit_capped_this_week(habit_page_id: str) -> bool:
    return get_week_completion_count(habit_page_id) >= get_habit_frequency(habit_page_id)


def _count_habit_completions_this_week(habit_page_id: str) -> int:
    """
    Count completed logs for a habit from Monday through today (inclusive).
    """
    try:
        today = datetime.now(TZ).date()
        monday = today - timedelta(days=today.weekday())
        results = notion.databases.query(
            database_id=NOTION_LOG_DB,
            filter={
                "and": [
                    {"property": "Habit", "relation": {"contains": habit_page_id}},
                    {"property": "Completed", "checkbox": {"equals": True}},
                    {"property": "Date", "date": {"on_or_after": monday.isoformat()}},
                ]
            },
        )
        count = 0
        for row in results.get("results", []):
            date_prop = row.get("properties", {}).get("Date", {}).get("date", {})
            start = date_prop.get("start")
            if not start:
                continue
            try:
                row_day = date.fromisoformat(start[:10])
            except Exception:
                continue
            if monday <= row_day <= today:
                count += 1
        return count
    except Exception as e:
        log.error("Habit weekly completion count error for %s: %s", habit_page_id, e)
        return 0


def logs_this_week(habit_page_id: str) -> int:
    today  = datetime.now(TZ).date()
    monday = today - timedelta(days=today.weekday())
    results = notion.databases.query(
        database_id=NOTION_LOG_DB,
        filter={
            "and": [
                {"property": "Habit",     "relation": {"contains": habit_page_id}},
                {"property": "Completed", "checkbox": {"equals": True}},
                {"property": "Date",      "date":     {"on_or_after":  monday.isoformat()}},
                {"property": "Date",      "date":     {"on_or_before": today.isoformat()}},
            ]
        },
    )
    return len(results.get("results", []))


def is_on_pace(habit: dict) -> bool:
    target = habit.get("freq_per_week")
    if not target:
        return False
    return logs_this_week(habit["page_id"]) >= target


# ══════════════════════════════════════════════════════════════════════════════
# NOTION — TO-DO
# ══════════════════════════════════════════════════════════════════════════════



def store_signoff_note(text: str) -> None:
    global _signoff_note_today
    _signoff_note_today = text.strip()
    log.info("Signoff note stored: %s", text[:80])


def get_and_clear_signoff_note() -> str:
    global _signoff_note_today
    note = _signoff_note_today
    _signoff_note_today = ""
    return note











def _get_today_tasks_for_palette() -> list[dict]:
    """Get today's tasks only (for To Do view)."""
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    today_str = local_today().isoformat()
    return [t for t in tasks if t.get("deadline") == today_str]


def format_digest_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build digest view for today + next 7 calendar days, grouped by date."""
    today = local_today()
    cutoff = today + timedelta(days=7)
    tasks = notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID)
    groups: dict[str, list[dict]] = defaultdict(list)
    beyond_count = 0

    for task in tasks:
        raw_deadline = task.get("deadline")
        parsed_deadline = notion_tasks._parse_deadline(raw_deadline)
        if not parsed_deadline:
            continue
        if parsed_deadline < today:
            continue
        if parsed_deadline <= cutoff:
            groups[parsed_deadline.isoformat()].append(task)
        else:
            beyond_count += 1

    lines = ["📖 Digest — Today + 7 Days", ""]
    if not groups:
        lines.append("✅ Clear for next 7 days!")
    else:
        for d in sorted(groups.keys()):
            day_tasks = sorted(groups[d], key=notion_tasks._task_sort_key)
            date_label = date.fromisoformat(d).strftime("%A, %B %-d")
            lines.append(f"📌 {date_label} ({len(day_tasks)})")
            for task in day_tasks:
                lines.append(f"  • {task.get('name', 'Untitled')}  {notion_tasks._context_label(task)}")
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    if beyond_count:
        lines.extend(["", f"...and {beyond_count} more beyond 7 days (view in Notion)"])

    return "\n".join(lines).strip(), kb.back_to_palette_keyboard()


def format_todo_view(marked_done_indices: set | None = None) -> tuple[str, InlineKeyboardMarkup]:
    """
    Format today's tasks with quick-mark buttons.

    Args:
        marked_done_indices: Set of task indices already marked done.
    """
    marked_done_indices = marked_done_indices or set()
    tasks = _get_today_tasks_for_palette()
    lines = ["✅ Today's Tasks — Mark Complete", ""]
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    if not tasks:
        lines.append("✅ No tasks due today!")
    else:
        for idx, task in enumerate(tasks):
            label = task.get("name", "Untitled")
            if idx in marked_done_indices:
                lines.append(f"✅ {label}")
                continue
            keyboard_rows.append(
                [InlineKeyboardButton(f"{fmt.num_emoji(idx + 1)} {label}", callback_data=f"qp:done:{idx}")]
            )
        if len(marked_done_indices) >= len(tasks):
            lines.append("✅ All today's tasks marked done! 🎉")

    keyboard_rows.append([InlineKeyboardButton("📖 Back to Palette", callback_data="qp:back")])
    return "\n".join(lines).strip(), InlineKeyboardMarkup(keyboard_rows)


def quick_access_keyboard() -> InlineKeyboardMarkup:
    """Keyboard with live This Week and Backlog counts."""
    _, _, this_week, backlog = notion_tasks._get_tasks_by_deadline_horizon(notion, NOTION_DB_ID)
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(f"🟠 This Week ({len(this_week)})", callback_data="qv:week"),
            InlineKeyboardButton(f"⚪ Backlog ({len(backlog)})", callback_data="qv:backlog"),
        ]]
    )





def parse_done_numbers_command(text: str) -> list[int] | None:
    normalized = text.strip().lower()

    # Normalize keycap number emojis (e.g., "3️⃣") into plain digits so users can
    # reply with either "complete 3" or "complete 3️⃣".
    keycap_map = {
        "0️⃣": "0",
        "1️⃣": "1",
        "2️⃣": "2",
        "3️⃣": "3",
        "4️⃣": "4",
        "5️⃣": "5",
        "6️⃣": "6",
        "7️⃣": "7",
        "8️⃣": "8",
        "9️⃣": "9",
        "🔟": "10",
    }
    for keycap, digit in keycap_map.items():
        normalized = normalized.replace(keycap, digit)

    m = re.match(
        r"^(?:done|complete|finish|check(?:\s+off)?)\s+((?:\d+\s*(?:,|\band\b)?\s*)+)$",
        normalized, re.IGNORECASE,
    )
    if not m:
        m = re.match(
            r"^mark\s+(?:done\s+)?((?:\d+\s*(?:,|\band\b)?\s*)+)\s+done$",
            normalized, re.IGNORECASE,
        )
    if not m:
        return None
    nums = [int(n) for n in re.findall(r"\d+", m.group(1))]
    return nums or None


def parse_review_numbers_command(text: str) -> list[int] | None:
    normalized = text.strip().lower()

    # Normalize keycap number emojis (e.g., "3️⃣") into plain digits so users can
    # reply with either "review 3" or "review 3️⃣".
    keycap_map = {
        "0️⃣": "0",
        "1️⃣": "1",
        "2️⃣": "2",
        "3️⃣": "3",
        "4️⃣": "4",
        "5️⃣": "5",
        "6️⃣": "6",
        "7️⃣": "7",
        "8️⃣": "8",
        "9️⃣": "9",
        "🔟": "10",
    }
    for keycap, digit in keycap_map.items():
        normalized = normalized.replace(keycap, digit)

    m = re.match(
        r"^(?:review|reassign|horizon|check(?:\s+off)?)\s+((?:\d+\s*(?:,|\band\b)?\s*)+)$",
        normalized, re.IGNORECASE,
    )
    if not m:
        m = re.match(
            r"^mark\s+(?:review\s+)?((?:\d+\s*(?:,|\band\b)?\s*)+)\s+review$",
            normalized, re.IGNORECASE,
        )
    if not m:
        return None
    nums = [int(n) for n in re.findall(r"\d+", m.group(1))]
    return nums or None


def _resolve_monthly_target_day(repeat_day: str, today: date) -> int | None:
    if repeat_day not in REPEAT_DAY_TO_MONTHDAY:
        return None
    configured_day = REPEAT_DAY_TO_MONTHDAY[repeat_day]
    month_last_day = calendar.monthrange(today.year, today.month)[1]
    if configured_day == -1:
        return month_last_day
    # For days that exceed month length (e.g., 31st in April), run on the month's last day.
    return min(configured_day, month_last_day)

# ══════════════════════════════════════════════════════════════════════════════
# BATCH CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

def _run_capture(raw_text: str, force_create: bool = False,
                 context_override: str | None = None,
                 deadline_override: int | None = None) -> dict:
    try:
        result        = ai_classify.classify_message(claude, CLAUDE_MODEL, raw_text, list(habit_cache.keys()), bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), local_today())
        task_name     = result.get("task_name") or raw_text
        deadline_days = result.get("deadline_days")
        ctx           = context_override or result.get("context", "🏠 Personal")
        recurring     = result.get("recurring", "None") or "None"
        repeat_day    = result.get("repeat_day")
        target_date = next_repeat_day_date(recurring, repeat_day)
        if target_date is not None:
            computed_days = (target_date - local_today()).days
            if deadline_days is None or (deadline_days <= 0 and computed_days > 0):
                deadline_days = computed_days
        if deadline_override is not None:
            deadline_days = deadline_override
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error(f"Claude error for '{raw_text}': {e}")
        return {"status": "error", "name": raw_text, "error": str(e)}

    if not force_create:
        dup = notion_tasks.find_duplicate_active_task(notion, NOTION_DB_ID, task_name)
        if dup:
            return {"status": "duplicate", "name": task_name, "duplicate": dup}

    try:
        page_id = notion_tasks.create_task(notion, NOTION_DB_ID, task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
        return {
            "status": "captured", "name": task_name,
            "horizon_label": horizon_label, "context": ctx,
            "recurring": recurring, "page_id": page_id,
        }
    except Exception as e:
        log.error(f"Notion error for '{task_name}': {e}")
        return {"status": "error", "name": task_name, "error": str(e)}




def pending_habits_for_digest(time_str: str | None = None) -> list[dict]:
    habits = (
        habit_cache.values()
        if time_str is None
        else [h for h in habit_cache.values() if h.get("time") == time_str]
    )
    pending: list[dict] = []
    for habit in sorted(habits, key=lambda h: h["sort"]):
        pid = habit["page_id"]
        if already_logged_today(pid):
            continue
        if is_on_pace(habit):
            continue
        pending.append(habit)
    return pending








async def refresh_quick_actions_keyboard(message) -> None:
    """Force-refresh the reply keyboard to replace legacy layouts (e.g. old Mute button)."""
    await message.reply_text("🔄 Refreshing quick actions…", reply_markup=ReplyKeyboardRemove())
    await message.reply_text(
        "✅ Quick actions updated.",
        reply_markup=kb.quick_actions_keyboard(BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS, BTN_CROSSFIT, BTN_NOTES, BTN_WEATHER),
    )








async def send_quick_reminder(message, mode: str = "priority") -> None:
    await message.reply_text(
        fmt.format_reminder_snapshot(mode=mode),
        parse_mode="Markdown",
        reply_markup=kb.quick_actions_keyboard(BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS, BTN_CROSSFIT, BTN_NOTES, BTN_WEATHER),
    )


# ══════════════════════════════════════════════════════════════════════════════
# INLINE KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# CALLBACK PATTERN REGISTRY (v13.1)
# ══════════════════════════════════════════════════════════════════════════════
#
# FORMAT: Callbacks use colon-separated patterns: {module}:{action}:{data}
#
# ACTIVE MODULES:
# ─ d/dp     Done picker (task completion)
# ─ h        Habits (morning/evening check-in & logging)
# ─ (reserved: cf, n for future CrossFit & Notes modules)
#
# PATTERN REFERENCE:
#
# DONE PICKER (d/dp) — Task completion flow
#   d:{pid}                - Mark a task done (legacy pattern, kept for stability)
#   dp:{key}:{idx}         - Select task from done picker
#   dpp:{key}:{page}       - Navigate done picker to page number
#   dpc:{key}              - Close/cancel done picker
#
# HABITS (h) — Morning/evening habit check-in
#   h:log:{pid}            - Log a habit to Notion (morning or evening)
#   h:check:cancel         - Dismiss habit check-in without logging
#   hpag:{check}:{page}    - Navigate habit paging for check-in lists
#
# FUTURE MODULES (to be implemented):
#   cf:*                   - CrossFit workout logging
#   n:*                    - Notes capture and tagging
#   dg:*                   - Digest interactions (if needed)
#
# NAMING CONVENTIONS:
# ─ Use colons (:) as separators, never underscores
# ─ PIDs (page IDs) are always restored from clean format: _restore_pid(parts[n])
def _restore_pid(pid: str) -> str:
    """Restore compact Notion page IDs to canonical dashed form.

    Accepts either already-dashed IDs or 32-char compact IDs.
    Falls back to the original input for unknown shapes.
    """
    raw = (pid or "").strip()
    if not raw:
        return raw
    if "-" in raw:
        return raw
    if len(raw) == 32:
        return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"
    return raw

# ─ Keys are string counters or message IDs from state maps
# ─ Actions are descriptive: log, select, page, cancel
#
# ══════════════════════════════════════════════════════════════════════════════













# ══════════════════════════════════════════════════════════════════════════════
# CAPTURE ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def complete_task_by_page_id(message, page_id: str, name: str) -> None:
    notion_tasks.mark_done(notion, page_id)
    suffix = "\n↻ Next instance created" if notion_tasks.handle_done_recurring(page_id) else ""
    await message.reply_text(f"✅ Done: {name}{suffix}")


async def create_or_prompt_task(message, raw_text: str, force_create: bool = False) -> None:
    task_texts = split_tasks(raw_text)
    is_multi   = len(task_texts) > 1
    thinking   = await message.reply_text(
        f"🧠 Classifying {len(task_texts)} tasks..." if is_multi else "🧠 Classifying..."
    )

    if is_multi:
        overrides = infer_batch_overrides(raw_text)
        context_override = overrides.get("context")
        deadline_override = overrides.get("deadline_days")
        loop    = asyncio.get_running_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, _run_capture, t, force_create, context_override, deadline_override)
            for t in task_texts
        ])
        await thinking.edit_text(fmt.format_batch_summary(list(results)), parse_mode="Markdown")
        return

    try:
        result        = ai_classify.classify_message(claude, CLAUDE_MODEL, raw_text, list(habit_cache.keys()), bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), local_today())
        task_name     = result.get("task_name") or raw_text
        deadline_days = result.get("deadline_days")
        ctx           = result.get("context", "🏠 Personal")
        confidence    = result.get("confidence", "low")
        recurring     = result.get("recurring", "None") or "None"
        repeat_day    = result.get("repeat_day")
        target_date = next_repeat_day_date(recurring, repeat_day)
        if target_date is not None:
            computed_days = (target_date - local_today()).days
            if deadline_days is None or (deadline_days <= 0 and computed_days > 0):
                deadline_days = computed_days
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error(f"Claude error: {e}")
        await thinking.edit_text("⚠️ Couldn't classify that. Try rephrasing?")
        return

    needs_context_clarification = (
        recurring != "None"
        and not _has_explicit_personal_or_work_context(raw_text)
    )

    if needs_context_clarification:
        ctx = "🏠 Personal"

    if not force_create:
        dup = notion_tasks.find_duplicate_active_task(notion, NOTION_DB_ID, task_name)
        if dup:
            await thinking.edit_text(
                f"⚠️ Already on your list:\n\n📝 {dup['name']}\n🕐 {dup.get('auto_horizon','')}  {dup.get('context','')}\n\nSend `force: {task_name}` to add anyway.",
                parse_mode="Markdown",
            )
            return

    recur_tag = f"\n🔁 {recurring}" if recurring != "None" else ""

    try:
        page_id = notion_tasks.create_task(notion, NOTION_DB_ID, task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
        if confidence == "high":
            await thinking.edit_text(
                f"✅ Captured!\n\n📝 {task_name}\n🕐 {horizon_label}  {ctx}{recur_tag}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        else:
            await thinking.edit_text(
                "📝 Task captured with default deadline, but I'm not 100% sure.\n\n"
                "You can:\n"
                "• Adjust the deadline in Notion\n"
                "• Rephrase and resend for better classification\n"
                "• Use `force: task name` to override",
                parse_mode="Markdown",
            )
        capture_map[thinking.message_id] = {"page_id": page_id, "name": task_name}
    except Exception as e:
        log.error(f"Notion error: {e}")
        await thinking.edit_text("⚠️ Classified but couldn't write to Notion.")


async def open_done_picker(message) -> None:
    global _done_picker_counter
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    if not tasks:
        await message.reply_text("✅ Nothing open in Today or overdue right now.")
        return
    key = str(_done_picker_counter); _done_picker_counter += 1
    done_picker_map[key] = tasks
    await message.reply_text("Which task should be marked done?", reply_markup=done_picker_keyboard(key, page=0))


async def open_habit_picker(message) -> None:
    pending_habits = [
        h for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
        if not already_logged_today(h["page_id"])
    ]
    if not pending_habits:
        await message.reply_text("✅ No habits left to log today.")
        return
    await message.reply_text(
        "🏃 *Which habit did you complete?*",
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(pending_habits, "manual"),
    )


async def cmd_refresh(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    notion_habits.load_habit_cache(notion=notion, notion_habit_db=NOTION_HABIT_DB); _refresh_habit_cache_refs()
    if _scheduler is not None:
        build_digest_schedule(_scheduler, message.get_bot())
    await send_daily_digest(message.get_bot(), include_habits=True)
    if _scheduler is not None:
        build_digest_schedule(_scheduler, message.get_bot())


async def cmd_todo(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    global _todo_picker_counter
    if message.chat_id != MY_CHAT_ID:
        return
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    if not tasks:
        await message.reply_text("✅ Nothing open in Today or overdue right now.")
        return
    key = str(_todo_picker_counter)
    _todo_picker_counter += 1
    todo_picker_map[key] = tasks
    await message.reply_text(
        "✅ *What did you get done?*",
        parse_mode="Markdown",
        reply_markup=todo_picker_keyboard(key),
    )


async def cmd_done_bare(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await open_done_picker(message)


async def cmd_habits_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await send_daily_habits_list(message.get_bot())


async def cmd_habits_picker(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await open_habit_picker(message)


async def cmd_crossfit(message, context=None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    if not (NOTION_WORKOUT_LOG_DB or NOTION_WOD_LOG_DB):
        await message.reply_text("⚠️ CrossFit module isn't configured yet — add the workout DB env vars to Railway.", parse_mode="Markdown")
        return
    await message.reply_text("💪 *CrossFit*\n\nWhat would you like to do?", parse_mode="Markdown", reply_markup=crossfit_submenu_keyboard())


async def cmd_notes_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await message.reply_text(
        "📝 Notes options:",
        reply_markup=kb.notes_options_keyboard(),
    )


async def cmd_weather_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    if not wx.current_location:
        await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
        return
    try:
        await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
    except Exception as e:
        log.error("Weather quick-action failed: %s", e)
        await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")


async def handle_weather(location: str) -> str:
    """Return weather output for a confirmed location."""
    if not location:
        return "📍 I need a location first. Send `weather: <city>` or `/location`."
    return fmt.format_weather_snapshot()


async def cmd_mute_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    if message.chat_id != MY_CHAT_ID:
        return
    if context is not None:
        context.user_data["awaiting_mute_days"] = False
    await message.reply_text(
        "🔕 Mute options for scheduled digests:",
        reply_markup=kb.mute_options_keyboard(),
    )


async def handle_v10_callback(q, parts: list[str]) -> bool:
    if parts[0] == "wl_save" and len(parts) == 2:
        key = parts[1]
        if key not in wl.pending_wantslist_map:
            await q.edit_message_text("⚠️ This confirmation expired — please re-send.")
            return True
        item_data = wl.pending_wantslist_map.pop(key)
        try:
            wl.create_wantslist_entry(notion, item_data["item"], category=item_data["category"])
            await q.edit_message_text(
                f"🎁 Saved!\n\n*{item_data['item']}*\n_{item_data['category']} · Wantslist_\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Wantslist save error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return True

    if parts[0] == "wl_cancel" and len(parts) == 2:
        wl.pending_wantslist_map.pop(parts[1], None)
        await q.edit_message_text("🎁 Cancelled — not saved.")
        return True

    if parts[0] == "tmdb_pick" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in wl.pending_tmdb_map:
            await q.edit_message_text("⚠️ This picker expired — please re-send.")
            return True
        candidates = wl.pending_tmdb_map.pop(key)
        try:
            c = candidates[int(idx_str)]
            wl._save_watchlist_from_candidate(notion, c, c["title"])
            seasons_str = f" · {c['seasons']} seasons" if c.get("seasons") else ""
            episodes_str = f" · {c['episodes']} eps" if c.get("episodes") else ""
            runtime_str = f" · {c['runtime']} min/ep" if c.get("runtime") else ""
            await q.edit_message_text(
                f"📺 Added!\n\n*{c['title']}* ({c['year']}) · {_notion_type_from_tmdb(c['media_type'])}"
                f"{seasons_str}{episodes_str}{runtime_str}\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"TMDB watchlist save error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return True

    if parts[0] == "tmdb_skip" and len(parts) == 2:
        key = parts[1]
        candidates = wl.pending_tmdb_map.pop(key, [])
        title = candidates[0]["title"] if candidates else "Unknown"
        try:
            wl.create_watchlist_entry(notion, title)
            await q.edit_message_text(
                f"📺 Added!\n\n*{title}*\n_Title only — no TMDB metadata_\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Watchlist title-only save error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return True

    if parts[0] == "tmdb_cancel" and len(parts) == 2:
        wl.pending_tmdb_map.pop(parts[1], None)
        await q.edit_message_text("📺 Cancelled — not saved.")
        return True

    return False


async def route_classified_message_v10(message, text: str) -> None:
    thinking = await message.reply_text("🧠 Got it...")
    if NOTION_WORKOUT_LOG_DB or NOTION_WOD_LOG_DB or NOTION_WORKOUT_PROGRAM_DB:
        try:
            workout_result = await asyncio.get_running_loop().run_in_executor(None, lambda: classify_workout_message(text, claude, CLAUDE_MODEL, CLAUDE_MAX_TOK))
        except Exception:
            workout_result = {"type": "none"}
        if workout_result.get("type") == "programme":
            await thinking.delete()
            await handle_cf_upload_programme(message, text, claude, notion, {"NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB, "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB, "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB, "CLAUDE_PARSE_MAX_TOKENS": CLAUDE_PARSE_MAX_TOKENS, "CLAUDE_MODEL": CLAUDE_MODEL})
            return
        if workout_result.get("type") in ("strength", "conditioning") and workout_result.get("confidence") == "high":
            await thinking.delete()
            if workout_result.get("type") == "strength":
                await handle_cf_strength_flow(message, workout_result, claude, notion, {"NOTION_WORKOUT_LOG_DB": NOTION_WORKOUT_LOG_DB, "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB, "NOTION_PRS_DB": NOTION_PRS_DB, "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB, "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB, "NOTION_CYCLES_DB": NOTION_CYCLES_DB}, cf_pending)
            else:
                await handle_cf_wod_flow(message, workout_result, notion, {"NOTION_WOD_LOG_DB": NOTION_WOD_LOG_DB, "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB, "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB, "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB}, cf_pending)
            return
    if await wl.handle_photo_followup(notion, message, text):
        await thinking.delete()
        return
    try:
        result = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, lambda: ai_classify.classify_message(claude, CLAUDE_MODEL, text, list(habit_cache.keys()), bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), local_today())),
            timeout=18,
        )
    except asyncio.TimeoutError:
        log.warning("Claude v10 classify timeout after 18s; falling back to task capture")
        await thinking.delete()
        await create_or_prompt_task(message, text)
        return
    except Exception as e:
        log.error(f"Claude v10 classify error: {e}")
        await thinking.delete()
        await create_or_prompt_task(message, text)
        return

    global _entertainment_counter
    intent = result.get("type")

    if intent == "watchlist":
        await thinking.delete()
        await wl.handle_watchlist_intent(notion, message, title=result.get("title", text), media_type=result.get("media_type", "Series"))
        return

    if intent == "wantslist":
        await thinking.delete()
        await wl.handle_wantslist_intent(message, item=result.get("item", text), category=result.get("category", "Other"))
        return

    if intent == "photo":
        await thinking.delete()
        await wl.handle_photo_intent(notion, message, subject=result.get("subject", text))
        return

    if intent == "note":
        await thinking.delete()
        await start_note_capture_flow(message, result.get("content", text))
        return

    if intent == "habit":
        habit_name = result.get("habit_name")
        confidence = result.get("confidence", "low")
        if habit_name and habit_name in habit_cache and confidence == "high":
            habit = habit_cache[habit_name]
            habit_pid = habit["page_id"]
            if already_logged_today(habit_pid):
                await thinking.edit_text(f"Already logged {habit_name} today! ✅")
            else:
                log_habit(habit_pid, habit_name)
                await thinking.edit_text(f"✅ Logged!\n\n{habit_name}\n📅 {date.today().strftime('%B %-d')}")
                asyncio.create_task(
                    check_and_notify_weekly_goals(
                        message.get_bot(),
                        MY_CHAT_ID,
                        notion,
                        NOTION_LOG_DB,
                        NOTION_HABIT_DB,
                        habit_cache,
                        notified_goals_this_week,
                        get_week_completion_count,
                        get_habit_frequency,
                    )
                )
        else:
            all_habits = [{"page_id": h["page_id"], "name": name} for name, h in habit_cache.items()]
            all_habits.sort(key=lambda h: h["name"].lower())
            await thinking.edit_text("Which habit did you complete?", reply_markup=kb.habit_buttons(all_habits, "manual"))
        return

    if intent == "entertainment_log":
        title = (result.get("title") or "").strip()
        confidence = result.get("confidence", "low")
        result.setdefault("date", date.today().isoformat())
        if confidence == "high" and title:
            try:
                await thinking.delete()
                await ent_log.handle_entertainment_log(notion, message, result)
            except Exception as e:
                log.error("Entertainment save error: %s", e)
                await message.reply_text("⚠️ I understood that as entertainment, but couldn't save to Notion.")
            return

        key = str(_entertainment_counter)
        _entertainment_counter += 1
        pending_map[key] = {"type": "entertainment_log", "payload": result, "raw_text": text}
        preview = title or text
        await thinking.edit_text(
            f"🎬 I think this is an entertainment log:\n\n*{preview}*\n\nSave it?",
            parse_mode="Markdown",
            reply_markup=kb.entertainment_confirm_keyboard(key),
        )
        return

    await thinking.delete()
    await create_or_prompt_task(message, text)






COMMAND_DISPATCH: dict[str, Callable] = {
    "digest": cmd_refresh,
    "📜digest": cmd_refresh,
    "refresh": cmd_refresh,
    "🔄 refresh": cmd_refresh,
    "✅ to do": cmd_todo,
    "✅to do": cmd_todo,
    "📋 all open": cmd_todo,
    "done": cmd_done_bare,
    "/habits": cmd_habits_text,
    "🏃 habits": cmd_habits_picker,
    "💪 crossfit": cmd_crossfit,
    "💪 CrossFit": cmd_crossfit,
    "💪crossfit": cmd_crossfit,
    "📝 notes": cmd_notes_text,
    "notes": cmd_notes_text,
    "🌤️ weather": cmd_weather_text,
    "🌤 weather": cmd_weather_text,
    "⛅ weather": cmd_weather_text,
    "weather": cmd_weather_text,
    "🔕 mute": cmd_mute_text,
}


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════


async def handle_trip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _trip_counter
    message = update.message
    text = " ".join(context.args).strip()
    if not text:
        await message.reply_text('Send your trip details after the command, e.g.:\n/trip work trip to Austin, site testing, Jun 14-17')
        return
    parsed = trips_mod.parse_trip_message(text, claude)
    destinations = parsed.get("destinations") or []
    destination = destinations[0] if destinations else "Trip"
    dep = parsed.get("departure_date")
    ret = parsed.get("return_date")
    key = str(_trip_counter); _trip_counter += 1
    trip_map[key] = {"destination": destination, "destinations": destinations or [destination], "departure_date": dep, "return_date": ret, "duration_label": "", "nights": 0, "purpose": parsed.get("purpose") or "Work", "multiple_cities": bool(parsed.get("multiple_cities")), "field_work_types": [], "multiple_sites": None, "checked_luggage": None}
    if not dep or not ret:
        prompt = await message.reply_text("📅 What dates is the trip? (e.g. Jun 14-17)")
        trip_awaiting_date_map[prompt.message_id] = key
        return
    nights = (date.fromisoformat(ret) - date.fromisoformat(dep)).days
    trip_map[key]["nights"] = nights
    trip_map[key]["duration_label"] = "Overnight" if nights == 1 else ("2-3 Days" if nights <= 3 else "4-5 Days")
    await message.reply_text(f"✈️ {destination} — {trips_mod.format_trip_dates(dep, ret)} ({nights} night(s), {trip_map[key]['purpose']})\n\nWhat field work are you doing?\n(Tap all that apply, then tap ✅ Done)", reply_markup=field_work_keyboard(key))

async def handle_message_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return
    message = update.message
    text    = (message.text or "").strip()
    if not text:
        return
    lower = text.lower().strip()

    if lower == "cancel":
        if message.reply_to_message and message.reply_to_message.message_id in digest_map:
            digest_map.pop(message.reply_to_message.message_id, None)
            await message.reply_text("✅ Dismissed")
            return
        await message.reply_text("Reply to a digest message with `cancel` to dismiss it.")
        return
    command_head = lower.split(maxsplit=1)[0] if lower else ""
    command_arg_text = text[len(text.split(maxsplit=1)[0]):].strip() if text.split(maxsplit=1) else ""

    global awaiting_packing_feedback
    if message.reply_to_message and message.reply_to_message.message_id in trip_awaiting_date_map:
        key = trip_awaiting_date_map.pop(message.reply_to_message.message_id)
        parsed = trips_mod.parse_trip_message(text, claude)
        dep, ret = parsed.get("departure_date"), parsed.get("return_date")
        if not dep or not ret:
            await message.reply_text("⚠️ I couldn't parse those dates. Try format like Jun 14-17.")
            return
        trip_map[key]["departure_date"] = dep
        trip_map[key]["return_date"] = ret
        nights = (date.fromisoformat(ret) - date.fromisoformat(dep)).days
        trip_map[key]["nights"] = nights
        trip_map[key]["duration_label"] = "Overnight" if nights == 1 else ("2-3 Days" if nights <= 3 else "4-5 Days")
        await message.reply_text(f"✈️ {trip_map[key]['destination']} — {trips_mod.format_trip_dates(dep, ret)} ({nights} night(s), {trip_map[key]['purpose']})\n\nWhat field work are you doing?\n(Tap all that apply, then tap ✅ Done)", reply_markup=field_work_keyboard(key))
        return

    if awaiting_packing_feedback and not command_head.startswith('/'):
        awaiting_packing_feedback = False
        try:
            notion.pages.create(parent={"database_id": NOTION_PACKING_ITEMS_DB}, properties={"Item": {"title": [{"text": {"content": text[:100]}}]}, "Always": {"checkbox": True}})
            await message.reply_text("✅ Added to packing items.")
        except Exception:
            await message.reply_text("⚠️ Couldn't save packing feedback.")
        return

    if context.user_data.get("awaiting_mute_days"):
        try:
            days = int(text)
            if days <= 0:
                raise ValueError("days must be positive")
            global mute_until
            mute_until = datetime.now(TZ) + timedelta(days=days)
            save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await message.reply_text(
                f"🔕 Digests paused for {days} day(s), until {mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
            )
        except Exception:
            await message.reply_text("Please send a valid positive number of days (example: 3).")
        return

    if context.user_data.get("awaiting_location"):
        if wx.set_location_smart(text, claude):
            context.user_data["awaiting_location"] = False
            await message.reply_text(f"📍 Location updated to {wx.current_location}.")
            wx.save_location_state(wx.current_location)
            try:
                await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
            except Exception as e:
                log.error("Weather quick-action failed: %s", e)
                await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
        else:
            await message.reply_text(
                "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
            )
        return

    if command_head.startswith("/location"):
        requested_location = command_arg_text.strip()
        if requested_location:
            if wx.set_location_smart(requested_location, claude):
                context.user_data["awaiting_location"] = False
                await message.reply_text(f"📍 Location updated to {wx.current_location}.")
                wx.save_location_state(wx.current_location)
                try:
                    await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
                except Exception as e:
                    log.error("Weather quick-action failed: %s", e)
                    await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
            else:
                await message.reply_text(
                    "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
                )
            return
        context.user_data["awaiting_location"] = True
        await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
        return

    if lower.startswith("weather:"):
        requested_location = ""
        if lower.startswith("weather:"):
            requested_location = text.split(":", 1)[1].strip()
            if requested_location:
                if not wx.set_location_smart(requested_location, claude):
                    await message.reply_text(
                        "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
                    )
                    return
                wx.save_location_state(wx.current_location)
        if not wx.current_location:
            context.user_data["awaiting_location"] = True
            await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
            return
        try:
            await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
        except Exception as e:
            log.error("Weather quick-action failed: %s", e)
            await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
        return

    pending_sport_competition = ent_log.pending_sport_competition_map.get(update.effective_chat.id)
    if pending_sport_competition:
        competition = text.strip()
        if competition:
            schema = ent_log.entertainment_schemas.get("sports") or {}
            page_id = pending_sport_competition.get("page_id")
            props = ent_log._build_sport_competition_props(schema, competition)
            if page_id and props:
                try:
                    notion_call(notion.pages.update, page_id=page_id, properties=props)
                    ent_log.pending_sport_competition_map.pop(update.effective_chat.id, None)
                    await message.reply_text(
                        f"🏆 Competition set: *{competition}*\n_Saved to Notion_",
                        parse_mode="Markdown",
                    )
                    return
                except Exception as e:
                    log.error("Sports competition update error: %s", e)
                    await message.reply_text("⚠️ I couldn't update that competition in Notion.")
                    return
            if page_id and not props:
                ent_log.pending_sport_competition_map.pop(update.effective_chat.id, None)
                await message.reply_text(
                    "⚠️ I couldn't find a Competition property in your Sports Log schema to update."
                )
                return

    # ── CrossFit programme upload — must be first before any classifier ──
    upload_programme_aliases = {
        "📤 upload programme",
        "📤 upload program",
        "upload programme",
        "upload program",
        "📤 upload programme...",
        "📤 upload program...",
    }
    if lower in upload_programme_aliases:
        context.user_data["awaiting_programme_upload"] = True
        await message.reply_text(
            "📋 *Upload Weekly Programme*\n\nPaste the full programme text now.\n"
                    "_Paste the whole thing — I'll extract Performance, Fitness and Hyrox._",
            parse_mode="Markdown",
        )
        return

    if context.user_data.get("awaiting_programme_upload") or cf_pending.pop("__awaiting_upload__", False):
        buffered = (context.user_data.get("programme_upload_buffer") or "").strip()
        merged_text = (buffered + "\n" + text).strip() if buffered else text
        ok = await handle_cf_upload_programme(message, merged_text, claude, notion, {
            "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB,
            "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB,
            "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB,
            "CLAUDE_PARSE_MAX_TOKENS": CLAUDE_PARSE_MAX_TOKENS,
            "NOTION_PROGRESSIONS_DB": NOTION_PROGRESSIONS_DB,
            "CLAUDE_MODEL": CLAUDE_MODEL,
        })
        if ok:
            context.user_data["awaiting_programme_upload"] = False
            context.user_data["programme_upload_buffer"] = ""
        else:
            context.user_data["awaiting_programme_upload"] = True
            context.user_data["programme_upload_buffer"] = merged_text[-12000:]
            await message.reply_text("📎 If Telegram split your programme into multiple messages, paste the next part now and I'll merge them.")
        return

    # Fallback: parse obvious weekly programmes even if transient upload state was lost
    # (e.g., after a worker restart between callback and pasted message).
    if looks_like_crossfit_programme(text):
        await handle_cf_upload_programme(message, text, claude, notion, {
            "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB,
            "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB,
            "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB,
            "CLAUDE_PARSE_MAX_TOKENS": CLAUDE_PARSE_MAX_TOKENS,
            "NOTION_PROGRESSIONS_DB": NOTION_PROGRESSIONS_DB,
            "CLAUDE_MODEL": CLAUDE_MODEL,
        })
        return

    pending_custom_topic = context.user_data.get("awaiting_note_custom_topic")
    if pending_custom_topic:
        key = pending_custom_topic.get("key")
        entry = pending_note_map.pop(key, None)
        context.user_data["awaiting_note_custom_topic"] = None
        if not entry:
            await message.reply_text("⚠️ This note prompt expired — please re-send the note.")
            return
        custom_topic = text.strip()[:60]
        if not custom_topic:
            await message.reply_text("⚠️ Topic can't be empty — please re-send the note.")
            return
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, entry["content"], custom_topic)
            topic_recency_map[custom_topic] = datetime.now(timezone.utc)
            await message.reply_text(
                f"✅ Note captured!\n🏷️ {custom_topic}\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Notion note custom-topic error: {e}")
            await message.reply_text("⚠️ Couldn't save note to Notion.")
        return

    awaiting_note_capture = context.user_data.get("awaiting_note_capture")
    if awaiting_note_capture:
        if not NOTION_NOTES_DB:
            context.user_data["awaiting_note_capture"] = None
            await message.reply_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return
        if awaiting_note_capture == "link" and not re.search(r"https?://\S+", text):
            await message.reply_text("Please send a valid URL starting with http:// or https://.")
            return
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, text)
            kind_label_map = {
                "quick": "note",
                "idea": "idea",
                "code": "code snippet",
                "link": "link",
            }
            kind_label = kind_label_map.get(awaiting_note_capture, "note")
            await message.reply_text(
                f"✅ {kind_label.capitalize()} saved to Notes.",
                reply_markup=kb.quick_actions_keyboard(BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS, BTN_CROSSFIT, BTN_NOTES, BTN_WEATHER),
            )
        except Exception as e:
            log.error("fn=handle_message_text event=note_quick_save_failed err=%s", e)
            await reply_notion_error(message, "save note")
        finally:
            context.user_data["awaiting_note_capture"] = None
        return

    # note: <text or url> — explicit inline command
    match_note = re.match(r"note:\s*(.+)$", text, re.IGNORECASE)
    if match_note:
        notes_pending.discard(update.effective_chat.id)
        await handle_note_input(message, match_note.group(1).strip())
        return

    # User is in note-capture mode — next message is the note content
    if update.effective_chat.id in notes_pending:
        await handle_note_input(message, text)
        return

    if lower == "done" and message.reply_to_message:
        replied_id = message.reply_to_message.message_id
        if replied_id in capture_map:
            captured = capture_map[replied_id]
            await complete_task_by_page_id(message, captured["page_id"], captured["name"])
            return
        if replied_id in digest_map:
            await message.reply_text("Reply with `done 1` or `done 1,3`, or use `done: task name`.", parse_mode="Markdown")
            return

    command_handler = COMMAND_DISPATCH.get(lower)
    if command_handler:
        await command_handler(message, context)
        return

    explicit_entertainment = ent_log.parse_explicit_entertainment_log(text)
    if explicit_entertainment:
        try:
            prompted = await ent_log._maybe_prompt_explicit_venue(notion, message, explicit_entertainment, text)
            if prompted:
                return
            await ent_log.handle_entertainment_log(notion, message, explicit_entertainment)
        except Exception as e:
            log.error("Explicit entertainment text save error: %s", e)
            await message.reply_text(_entertainment_save_error_text(e, explicit_entertainment))
        return

    numbers = parse_done_numbers_command(text)
    if numbers:
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        done_names: list[str] = []

        if source_id and source_id in digest_map:
            items = digest_map[source_id]
            for n in numbers:
                if 1 <= n <= len(items):
                    pid  = items[n - 1]["page_id"]
                    name = items[n - 1]["name"]
                    notion_tasks.mark_done(notion, pid)
                    suffix = " ↻ next queued" if notion_tasks.handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")
        elif message.reply_to_message:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            recovered = notion_tasks.recover_digest_items_from_text(notion, NOTION_DB_ID, replied_text)
            for n in numbers:
                task = recovered.get(n)
                if task:
                    pid = task["page_id"]
                    name = task["name"]
                    notion_tasks.mark_done(notion, pid)
                    suffix = " ↻ next queued" if notion_tasks.handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")

        if done_names:
            msg = "Marked done:\n" + "\n".join(f"✅ {n}" for n in done_names)
            await message.reply_text(msg)
        else:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    review_numbers = parse_review_numbers_command(text)
    if review_numbers:
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        queued = 0

        if source_id and source_id in digest_map:
            items = digest_map[source_id]
            for n in review_numbers:
                if 1 <= n <= len(items):
                    task = items[n - 1]
                    await message.reply_text(
                        f"{fmt.num_emoji(n)} {task['name']}\nChoose a new horizon:",
                        reply_markup=kb.review_keyboard(task["page_id"]),
                    )
                    queued += 1
        elif message.reply_to_message:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            recovered = notion_tasks.recover_digest_items_from_text(notion, NOTION_DB_ID, replied_text)
            for n in review_numbers:
                task = recovered.get(n)
                if task:
                    await message.reply_text(
                        f"{fmt.num_emoji(n)} {task['name']}\nChoose a new horizon:",
                        reply_markup=kb.review_keyboard(task["page_id"]),
                    )
                    queued += 1

        if queued == 0:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    match_name = re.match(r"done:\s*(.+)$", text, re.IGNORECASE)
    if match_name:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_name.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_name.group(1).strip()}\".")
        return

    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_mark_done.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_mark_done.group(1).strip()}\".")
        return

    match_focus = re.match(r"focus:\s*(.+)$", text, re.IGNORECASE)
    if match_focus:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_focus.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            notion_tasks.set_focus(notion, matched["page_id"], True)
            await message.reply_text(f"🎯 Focused: {matched['name']} → *Doing*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_focus.group(1).strip()}\".")
        return

    match_unfocus = re.match(r"unfocus:\s*(.+)$", text, re.IGNORECASE)
    if match_unfocus:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_unfocus.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            notion_tasks.set_focus(notion, matched["page_id"], False)
            await message.reply_text(f"⬜ Unfocused: {matched['name']} → *To Do*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_unfocus.group(1).strip()}\".")
        return

    cf_flow_key = context.user_data.get("cf_flow_key")
    if cf_flow_key and cf_flow_key in cf_pending:
        await handle_cf_text_reply(message, text, cf_flow_key, claude, notion, {"NOTION_WORKOUT_LOG_DB": NOTION_WORKOUT_LOG_DB, "NOTION_WOD_LOG_DB": NOTION_WOD_LOG_DB, "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB, "NOTION_PRS_DB": NOTION_PRS_DB, "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB, "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB, "NOTION_CYCLES_DB": NOTION_CYCLES_DB, "NOTION_PROGRESSIONS_DB": NOTION_PROGRESSIONS_DB}, cf_pending)
        return

    match_signoff = re.match(r"signoff:\s*(.+)$", text, re.IGNORECASE)
    if match_signoff:
        note = match_signoff.group(1).strip()
        store_signoff_note(note)
        await message.reply_text(
            f"📓 Signoff noted — I'll include this in tonight's log.\n\n_{note}_",
            parse_mode="Markdown",
        )
        return

    match_force = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if match_force:
        await create_or_prompt_task(message, match_force.group(1).strip(), force_create=True); return

    await route_classified_message_v10(message, text)




async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q     = update.callback_query
    await q.answer()
    # Callback prefix registry
    # hc:{page_id}           — habit check-in (log habit); hl redirects here
    # nt:{key}:{code}        — new task horizon picker
    # ntctx:{key}:{ctx}      — new task context picker
    # d:{page_id}            — mark task done
    # h:{page_id}:{code}     — reassign horizon
    # td:{key}:{idx}         — to-do picker mark done
    # dp:{key}:{idx}         — done picker select
    # dpp:{key}:{page}       — done picker paginate
    # dpc:{key}              — done picker cancel
    # el:{key}:{action}      — entertainment log confirm
    # qp:{action}            — command palette
    # qv:{view}              — quick horizon view
    # mq:{action}            — mute options
    # nq:{mode}              — notes quick capture
    # note_topic:{key}:{ref} — note topic picker
    # cf:{action}            — crossfit flow
    # tw:{key}:{slug}        — trip field work picker
    # twd/tms/tcl:{key}      — trip flow steps
    # wl_save/wl_cancel      — wantslist confirm
    # tmdb_pick/skip/cancel  — watchlist TMDB picker
    parts = q.data.split(":")
    if parts[0] == "hl":
        parts[0] = "hc"
    if await handle_v10_callback(q, parts):
        return
    if parts[0] == "tw" and len(parts) == 3:
        _, key, slug = parts
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        slug_to_label = {"sw": "Site Walk", "st": "Site Testing", "it": "Isolation Testing", "hm": "24hr Monitoring", "nn": "None"}
        label = slug_to_label.get(slug)
        current = trip_map[key].get("field_work_types", [])
        if label == "None":
            trip_map[key]["field_work_types"] = ["None"]
        elif label in current:
            current.remove(label); trip_map[key]["field_work_types"] = current
        elif label:
            current = [x for x in current if x != "None"]; current.append(label); trip_map[key]["field_work_types"] = current
        await q.edit_message_reply_markup(reply_markup=field_work_keyboard(key))
        return

    if parts[0] == "twd" and len(parts) == 2:
        key = parts[1]
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        if not trip_map[key].get("field_work_types"):
            trip_map[key]["field_work_types"] = ["None"]
        await q.message.reply_text("Multiple sites on this trip?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Yes", callback_data=f"tms:{key}:y"), InlineKeyboardButton("No", callback_data=f"tms:{key}:n")]]))
        return

    if parts[0] == "tms" and len(parts) == 3:
        _, key, ans = parts
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        trip_map[key]["multiple_sites"] = (ans == "y")
        await q.message.reply_text("Checking a bag?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Yes", callback_data=f"tcl:{key}:y"), InlineKeyboardButton("No", callback_data=f"tcl:{key}:n")]]))
        return

    if parts[0] == "tcl" and len(parts) == 3:
        _, key, ans = parts
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        trip_map[key]["checked_luggage"] = (ans == "y")
        await q.message.reply_text("🧠 Building your packing list...")
        await trips_mod.execute_trip(key, q, notion=notion, claude=claude, trip_map=trip_map, set_awaiting_packing_feedback=lambda value: globals().__setitem__("awaiting_packing_feedback", value), fetch_weather=wx.fetch_weather)
        return

    if parts[0] == "cf":
        if len(parts) > 1 and parts[1] == "upload_programme":
            context.user_data["awaiting_programme_upload"] = True
            cf_pending["__awaiting_upload__"] = True
            await q.message.reply_text(
                "📋 *Upload Weekly Programme*\n\nPaste the full programme text now.\n_Paste the whole thing — I'll extract Performance, Fitness and Hyrox._",
                parse_mode="Markdown",
            )
            return
        else:
            context.user_data["cf_flow_key"] = str(q.message.chat_id)
        await handle_cf_callback(q, parts, claude, notion, {"NOTION_WORKOUT_LOG_DB": NOTION_WORKOUT_LOG_DB, "NOTION_WOD_LOG_DB": NOTION_WOD_LOG_DB, "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB, "NOTION_PRS_DB": NOTION_PRS_DB, "NOTION_SUBS_DB": NOTION_SUBS_DB, "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB, "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB, "NOTION_CYCLES_DB": NOTION_CYCLES_DB, "CLAUDE_PARSE_MAX_TOKENS": CLAUDE_PARSE_MAX_TOKENS, "NOTION_PROGRESSIONS_DB": NOTION_PROGRESSIONS_DB}, cf_pending)
        return

    if parts[0] == "kind_task" and len(parts) == 2:
        key = parts[1]
        text = pending_message_map.pop(key, None)
        if not text:
            await q.edit_message_text("⚠️ This prompt expired — please send it again.")
            return
        await q.edit_message_text("📌 Routed to task flow.")
        if looks_like_task_batch(text):
            await create_or_prompt_task(q.message, text)
        else:
            await route_classified_message_v10(q.message, text)
        return

    if parts[0] == "kind_refresh" and len(parts) == 2:
        key = parts[1]
        pending_message_map.pop(key, None)
        await q.edit_message_text("🔄 Refreshed.")
        await send_quick_reminder(q.message, mode="priority")
        return

    if parts[0] == "mq" and len(parts) == 2:
        action = parts[1]
        if action == "cancel":
            await q.edit_message_text("❌ Mute action canceled.")
            return
        if action == "status":
            await q.edit_message_text(fmt.mute_status_text())
            return
        if action == "unmute":
            global mute_until
            mute_until = None
            save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await q.edit_message_text("🔔 Digests resumed.")
            return
        if action in {"1", "3", "7"}:
            days = int(action)
            mute_until = datetime.now(TZ) + timedelta(days=days)
            save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await q.edit_message_text(
                f"🔕 Digests paused for {days} day(s), until {mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
            )
            return

    if parts[0] == "nq" and len(parts) == 2:
        mode = parts[1]
        if mode == "cancel":
            await q.edit_message_text("❌ Notes action canceled.")
            return
        if not NOTION_NOTES_DB:
            await q.edit_message_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return
        capture_mode = mode if mode in {"quick", "idea", "code", "link"} else "quick"
        context.user_data["awaiting_note_capture"] = capture_mode
        prompt_map = {
            "quick": "📝 Send the note text you want to save.",
            "idea": "💡 Send the idea you want to save.",
            "code": "💻 Send the code snippet you want to save.",
            "link": "🔗 Send the link you want to save.",
        }
        prompt = prompt_map[capture_mode]
        await q.edit_message_text(prompt)
        return

    if parts[0] == "kind_note" and len(parts) == 2:
        key = parts[1]
        text = pending_message_map.pop(key, None)
        if not text:
            await q.edit_message_text("⚠️ This prompt expired — please send it again.")
            return
        await q.edit_message_text("📝 Routed to note flow.")
        await start_note_capture_flow(q.message, text)
        return

    if parts[0] == "note_topic" and len(parts) == 3:
        key = parts[1]
        topic_ref = parts[2]
        entry = pending_note_map.get(key)
        if not entry:
            await q.edit_message_text("⚠️ This note prompt expired — please re-send the note.")
            return
        if topic_ref == "add":
            context.user_data["awaiting_note_custom_topic"] = {"key": key}
            await q.edit_message_text("🏷️ Send the new topic name for this note.")
            return
        pending_note_map.pop(key, None)
        if topic_ref == "none":
            selected_topic = None
        else:
            try:
                selected_topic = entry["topic_order"][int(topic_ref)]
            except Exception:
                selected_topic = None
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, entry["content"], selected_topic)
            if selected_topic:
                topic_recency_map[selected_topic] = datetime.now(timezone.utc)
            topic_line = f"\n🏷️ {selected_topic}" if selected_topic else ""
            await q.edit_message_text(
                f"✅ Note captured!\n{topic_line}\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Notion note error: {e}")
            await q.edit_message_text("⚠️ Couldn't save note to Notion.")
        return

    if parts[0] == "notes_start":
        notes_pending.add(q.message.chat_id)
        await q.edit_message_text(
            "📒 *Notes* — send me a link or type a note:",
            parse_mode="Markdown",
        )
        return

    if q.data == "h:check:cancel":
        await q.edit_message_text("✅ Habit check closed.")
        return

    if q.data.startswith("h:log:"):
        pid_raw = q.data.removeprefix("h:log:").strip()
        if not pid_raw:
            await q.edit_message_text("⚠️ Habit button expired. Please open 🎯 Habits again.")
            return
        habit_page_id = _restore_pid(pid_raw)
        habit_name = next((n for n, h in habit_cache.items() if h["page_id"] == habit_page_id), "Unknown")

        if already_logged_today(habit_page_id):
            try:
                await q.edit_message_text(f"✅ Already logged {habit_name} today!")
            except Exception as ui_error:
                log.warning("Habit dedupe UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text(f"Already logged {habit_name} today! ✅")
            return

        try:
            log_habit(habit_page_id, habit_name)
        except Exception as notion_error:
            log.error("Habit log Notion error for %s: %s", habit_name, notion_error)
            try:
                await q.edit_message_text("⚠️ Couldn't log to Notion.")
            except Exception as ui_error:
                log.warning("Habit log error UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text("⚠️ Couldn't log to Notion.")
            return

        try:
            await q.edit_message_text(f"✅ {habit_name} logged!")
        except Exception as ui_error:
            log.warning("Habit success UI update failed for %s: %s", habit_name, ui_error)
            await q.message.reply_text(f"✅ {habit_name} logged!")

        asyncio.create_task(check_and_notify_weekly_goals(q.bot, MY_CHAT_ID))
        return

    if parts[0] == "h" and len(parts) >= 2:
        if parts[1] == "check" and len(parts) == 3 and parts[2] == "cancel":
            await q.edit_message_text("✅ Habit check closed.")
            return

        if parts[1] != "log" or len(parts) != 3:
            return

        habit_page_id = _restore_pid(parts[2])
        habit_name = next((n for n, h in habit_cache.items() if h["page_id"] == habit_page_id), "Unknown")

        if already_logged_today(habit_page_id):
            try:
                await q.edit_message_text(f"✅ Already logged {habit_name} today!")
            except Exception as ui_error:
                log.warning("Habit dedupe UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text(f"Already logged {habit_name} today! ✅")
            return

        try:
            log_habit(habit_page_id, habit_name)
        except Exception as notion_error:
            log.error("Habit log Notion error for %s: %s", habit_name, notion_error)
            try:
                await q.edit_message_text("⚠️ Couldn't log to Notion.")
            except Exception as ui_error:
                log.warning("Habit log error UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text("⚠️ Couldn't log to Notion.")
            return

        try:
            await q.edit_message_text(f"✅ {habit_name} logged!")
        except Exception as ui_error:
            log.warning("Habit success UI update failed for %s: %s", habit_name, ui_error)
            await q.message.reply_text(f"✅ {habit_name} logged!")

        try:
            if q.message:
                await open_habit_picker(q.message)
            else:
                await q.bot.send_message(chat_id=update.effective_chat.id, text="🏃 Which habit did you complete?", reply_markup=kb.habit_buttons([
                    {"page_id": h["page_id"], "name": h["name"]}
                    for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
                    if not already_logged_today(h["page_id"])
                ], "manual"))
        except Exception as follow_up_error:
            log.error("Habit follow-up picker failed after logging %s: %s", habit_name, follow_up_error)
            if q.message:
                await q.message.reply_text("✅ Logged. Send /done to continue logging more habits.")
            else:
                await q.bot.send_message(chat_id=update.effective_chat.id, text="✅ Logged. Send /done to continue logging more habits.")

        asyncio.create_task(
            check_and_notify_weekly_goals(
                q.bot,
                MY_CHAT_ID,
                notion,
                NOTION_LOG_DB,
                NOTION_HABIT_DB,
                habit_cache,
                notified_goals_this_week,
                get_week_completion_count,
                get_habit_frequency,
            )
        )
        return

    if parts[0] == "hpag" and len(parts) == 3:
        _, prefix, page_str = parts
        all_habits = notion_habits.get_active_habits_for_trigger(notion_query_all=notion_query_all, notion_habit_db=NOTION_HABIT_DB, parse_time_to_minutes=_parse_time_to_minutes, count_habit_completions_this_week=_count_habit_completions_this_week)
        try:
            await q.edit_message_reply_markup(
                reply_markup=kb.habit_buttons(all_habits, prefix, page=int(page_str))
            )
        except Exception as e:
            log.error(f"Habit pagination error: {e}")
            await q.edit_message_text("⚠️ Couldn't update habits view.")
        return

    if parts[0] == "el" and len(parts) == 3:
        _, key, action = parts
        entry = pending_map.pop(key, None)
        if not entry or entry.get("type") != "entertainment_log":
            await q.edit_message_text("⚠️ This entertainment prompt expired — please send it again.")
            return
        payload = dict(entry.get("payload") or {})
        if action == "no":
            payload = dict(entry.get("original_payload") or payload)
        elif action in ("cancel", "save"):
            # Backward compatibility with older inline keyboards.
            if action == "cancel":
                await q.edit_message_text("❌ Not saved.")
                return
        elif action != "yes":
            await q.edit_message_text("⚠️ Invalid choice — please send the log again.")
            return
        raw_text = entry.get("raw_text", "")
        if not (payload.get("title") or "").strip():
            payload["title"] = raw_text
        payload.setdefault("date", date.today().isoformat())
        try:
            entry_id, fav_saved = ent_log.create_entertainment_log_entry(notion, payload)
            label = ENTERTAINMENT_LOG_LABELS.get(payload.get("log_type"), "Entertainment")
            suffix = "\n🎞️ Added to Favourite Films" if fav_saved and payload.get("log_type") == "cinema" else ""
            await q.edit_message_text(
                f"✅ Logged to {label}\n\n🎫 {payload.get('title','Untitled')}\n📅 {payload.get('date')}{suffix}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            if payload.get("log_type") == "sport":
                _remember_pending_sport_competition(q.message, entry_id)
                await q.message.reply_text("🏆 Logged to Sports Log. Which competition should I set for this one?")
            log.info("Entertainment confirmed and saved page_id=%s", entry_id)
        except Exception as e:
            log.error("Entertainment callback save error: %s", e)
            await q.edit_message_text(_entertainment_save_error_text(e, payload))
        return



    if parts[0] == "d" and len(parts) == 2:
        page_id = _restore_pid(parts[1])
        try:
            notion_tasks.mark_done(notion, page_id)
            suffix = "\n↻ Next instance created" if notion_tasks.handle_done_recurring(page_id) else ""
            await q.edit_message_text(f"✅ Marked as done!{suffix}")
        except Exception as e:
            log.error(f"Notion done error: {e}"); await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "h" and len(parts) == 3:
        _, pid_clean, code = parts
        page_id       = _restore_pid(pid_clean)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        try:
            notion_tasks.set_deadline_from_horizon_code(notion, page_id, code)
            await q.edit_message_text(f"Updated → {horizon_label} ✓")
        except Exception as e:
            log.error(f"Notion horizon error: {e}"); await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "td" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in todo_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `✅ To Do` again.", parse_mode="Markdown")
            return
        tasks = todo_picker_map[key]
        try:
            idx = int(idx_str)
            task = tasks[idx]
        except Exception:
            await q.answer("That task is no longer available.", show_alert=False)
            return
        if task.get("_done"):
            await q.answer("Already marked done.", show_alert=False)
            return
        try:
            notion_tasks.mark_done(notion, task["page_id"])
            notion_tasks.handle_done_recurring(task["page_id"])
            task["_done"] = True
        except Exception as e:
            log.error(f"To do picker error: {e}")
            await q.edit_message_text("⚠️ Couldn't mark that task done.")
            return

        done_count = sum(1 for t in tasks if t.get("_done"))
        remaining = len(tasks) - done_count
        if remaining == 0:
            todo_picker_map.pop(key, None)
            await q.edit_message_text("🎉 All done!")
            return
        await q.edit_message_text(
            f"✅ {done_count} done · {remaining} remaining",
            reply_markup=todo_picker_keyboard(key),
        )
        return

    if parts[0] == "dp" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown"); return
        try:
            task = done_picker_map[key][int(idx_str)]
            notion_tasks.mark_done(notion, task["page_id"])
            suffix = "\n↻ Next instance created" if notion_tasks.handle_done_recurring(task["page_id"]) else ""
            await q.edit_message_text(f"✅ Done: {task['name']}{suffix}")
        except Exception as e:
            log.error(f"Done picker error: {e}"); await q.edit_message_text("⚠️ Couldn't mark that task done.")
        return

    if parts[0] == "dpp" and len(parts) == 3:
        _, key, page_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown"); return
        await q.edit_message_reply_markup(reply_markup=done_picker_keyboard(key, page=int(page_str)))
        return

    if parts[0] == "noop":
        await q.answer()
        return

    if parts[0] == "dpc" and len(parts) == 2:
        done_picker_map.pop(parts[1], None)
        await q.edit_message_text("Done picker closed.")
        return

    if parts[0] == "qp" and len(parts) >= 2:
        action = parts[1]

        if action == "digest":
            try:
                message, keyboard = format_digest_view()
                await q.edit_message_text(message, reply_markup=keyboard)
            except Exception as e:
                log.error("Palette digest callback error: %s", e)
                await q.edit_message_text("⚠️ Couldn't load digest view right now.")
            return

        if action == "todo":
            context.user_data["palette_done_indices"] = set()
            message, keyboard = format_todo_view()
            await q.edit_message_text(message, reply_markup=keyboard)
            return

        if action == "done" and len(parts) == 3:
            try:
                idx = int(parts[2])
            except ValueError:
                await q.answer("Invalid task selection.", show_alert=False)
                return

            tasks = _get_today_tasks_for_palette()
            if idx < 0 or idx >= len(tasks):
                await q.answer("That task is no longer available.", show_alert=False)
                message, keyboard = format_todo_view(context.user_data.get("palette_done_indices", set()))
                await q.edit_message_text(message, reply_markup=keyboard)
                return

            done_indices = set(context.user_data.get("palette_done_indices", set()))
            if idx in done_indices:
                await q.answer("Already marked done.", show_alert=False)
            else:
                task = tasks[idx]
                try:
                    notion_tasks.mark_done(notion, task["page_id"])
                    notion_tasks.handle_done_recurring(task["page_id"])
                    done_indices.add(idx)
                    context.user_data["palette_done_indices"] = done_indices
                except Exception as e:
                    log.error("Palette done callback error: %s", e)
                    await q.edit_message_text("⚠️ Couldn't mark that task done.")
                    return

            message, keyboard = format_todo_view(done_indices)
            await q.edit_message_text(message, reply_markup=keyboard)
            return

        if action == "back":
            context.user_data.pop("palette_done_indices", None)
            await q.edit_message_text(
                "🎯 *Quick Access*",
                parse_mode="Markdown",
                reply_markup=kb.format_command_palette(),
            )
            return

        if action == "habits":
            await q.edit_message_text("🎯 Loading habits…")
            await send_daily_habits_list(q.bot)
            return

        if action == "notes":
            if NOTION_NOTES_DB:
                await q.edit_message_text("📝 Notes connected. Choose an option:", reply_markup=kb.notes_options_keyboard())
            else:
                await q.edit_message_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return

        if action == "weather":
            await q.edit_message_text(fmt.format_weather_snapshot(), parse_mode="Markdown")
            return

        if action == "mute":
            await q.edit_message_text(
                "🔕 Choose a mute option:",
                reply_markup=kb.mute_options_keyboard(),
            )
            return

    if parts[0] == "qv" and len(parts) == 2 and parts[1] in {"week", "backlog"}:
        try:
            message, ordered = fmt.format_week_view(parts[1])
            await q.edit_message_text(
                text=message,
                parse_mode="Markdown",
                reply_markup=kb.horizon_view_back_keyboard(),
            )
            if ordered and q.message:
                digest_map[q.message.message_id] = ordered
        except Exception as e:
            log.error("Quick-view callback error (%s): %s", q.data, e)
            await q.edit_message_text("⚠️ Couldn't load that view right now.")
        return

    if q.data == "digest:today":
        try:
            tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
            message, ordered = fmt.format_hybrid_digest(tasks)
            await q.edit_message_text(text=message, parse_mode="Markdown")
            if ordered and q.message:
                digest_map[q.message.message_id] = ordered
        except Exception as e:
            log.error("Digest today callback error: %s", e)
            await q.edit_message_text("⚠️ Couldn't refresh today's digest right now.")
        return

    if q.data == "digest:sunday":
        await send_sunday_review(q.bot)
        return


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ══════════════════════════════════════════════════════════════════════════════

async def run_recurring_check(bot) -> None:
    notion_habits.load_habit_cache(notion=notion, notion_habit_db=NOTION_HABIT_DB); _refresh_habit_cache_refs()
    if datetime.now(TZ).weekday() == 0:
        notified_goals_this_week.clear()
        await record_weekly_streaks(
            bot,
            notion,
            NOTION_LOG_DB,
            NOTION_HABIT_DB,
            NOTION_STREAK_DB,
            habit_cache,
            get_current_monday,
            get_habit_frequency,
        )
    if is_muted():
        log.info("Recurring check skipped (muted)")
        return
    spawned = notion_tasks.process_recurring_tasks(notion, NOTION_DB_ID)
    log.info(f"Recurring check: {spawned} task(s) spawned")


async def get_digest_config(slot_time: str, weekday: bool) -> dict:
    try:
        slots = load_digest_slots()
    except Exception as e:
        log.error("Failed to read digest config for %s (%s): %s", slot_time, "weekday" if weekday else "weekend", e)
        return {"contexts": None, "max_items": None, "include_habits": False}
    for slot in slots:
        if slot.get("time") == slot_time and bool(slot.get("is_weekday")) == bool(weekday):
            return {
                "contexts": slot.get("contexts"),
                "max_items": slot.get("max_items"),
                "include_habits": bool(slot.get("include_habits")),
            }
    return {"contexts": None, "max_items": None, "include_habits": False}


def _filter_digest_tasks(tasks: list[dict], config: dict | None = None) -> list[dict]:
    if not config:
        return tasks
    filtered = tasks
    contexts = config.get("contexts")

    def normalize_context_label(value: str | None) -> str:
        v = (value or "").strip().lower()
        if "personal" in v or "🏠" in v:
            return "personal"
        if "work" in v or "💼" in v:
            return "work"
        if "health" in v or "🏃" in v:
            return "health"
        if "hk" in v or "collab" in v or "🤝" in v:
            return "hk"
        return v

    if contexts is not None and isinstance(contexts, list):
        allowed = {normalize_context_label(c) for c in contexts}
        filtered = [t for t in filtered if normalize_context_label(t.get("context")) in allowed]
    return filtered


async def send_digest_for_slot(bot, slot: dict) -> None:
    now = datetime.now(TZ)
    day_key = now.date().isoformat()
    for key in list(_digest_slot_sent_today):
        if not key.startswith(day_key):
            _digest_slot_sent_today.discard(key)
    weekday = now.weekday() < 5
    slot_key = f"{day_key}|{'wd' if weekday else 'we'}|{slot.get('time')}"
    if slot_key in _digest_slot_sent_today:
        log.info("Skipping duplicate digest send for slot %s (%s)", slot.get("time"), "weekday" if weekday else "weekend")
        return
    config = await get_digest_config(slot["time"], slot["is_weekday"])
    log.info(
        "Digest slot trigger fired at %s (%s) — include_habits=%s contexts=%s max_items=%s",
        slot.get("time"),
        "weekday" if slot.get("is_weekday") else "weekend",
        bool(slot.get("include_habits")),
        config.get("contexts"),
        config.get("max_items"),
    )
    is_signoff = slot.get("is_signoff", False)

    if not config.get("contexts") and not config.get("include_habits") and not is_signoff:
        log.info(
            "Skipping slot %s — nothing selected (no contexts, habits, or signoff)",
            slot.get("time"),
        )
        return

    if is_signoff:
        await generate_daily_log(bot)
        return
    await send_daily_digest(bot, include_habits=slot["include_habits"], config=config)
    _digest_slot_sent_today.add(slot_key)


def _queue_missed_slots_for_today(scheduler, bot, slots: list[dict]) -> None:
    """
    Queue immediate one-off sends for slots that were added/updated shortly after
    their scheduled minute on the current day.
    """
    now = datetime.now(TZ)
    weekday = now.weekday() < 5
    grace_minutes = 20

    # Keep memory bounded; keys include yyyy-mm-dd and expire naturally.
    today_prefix = now.date().isoformat()
    for key in list(_digest_catchup_sent):
        if not key.startswith(today_prefix):
            _digest_catchup_sent.discard(key)

    for slot in slots:
        if bool(slot.get("is_weekday")) != weekday:
            continue
        try:
            slot_hour, slot_minute = map(int, str(slot["time"]).split(":"))
        except Exception:
            continue

        slot_dt = now.replace(hour=slot_hour, minute=slot_minute, second=0, microsecond=0)
        age_minutes = (now - slot_dt).total_seconds() / 60.0
        if age_minutes < 0 or age_minutes > grace_minutes:
            continue

        catchup_key = f"{today_prefix}|{'wd' if weekday else 'we'}|{slot['time']}"
        if catchup_key in _digest_catchup_sent:
            continue

        try:
            job = scheduler.add_job(
                send_digest_for_slot,
                "date",
                run_date=now + timedelta(seconds=2),
                args=[bot, slot],
                id=f"digest_catchup_{today_prefix}_{'wd' if weekday else 'we'}_{slot_hour:02d}{slot_minute:02d}",
                replace_existing=True,
            )
            _digest_jobs.append(job)
            _digest_catchup_sent.add(catchup_key)
            log.info("Queued digest catch-up for slot %s (%s)", slot["time"], "weekday" if weekday else "weekend")
        except Exception as e:
            log.warning("Failed to queue digest catch-up for slot %s: %s", slot.get("time"), e)


def build_digest_schedule(scheduler, bot, queue_catchup: bool = False) -> int:
    global _digest_slots_last_load_succeeded
    for job in _digest_jobs:
        try:
            job.remove()
        except Exception:
            pass
    _digest_jobs.clear()

    try:
        slots = load_digest_slots()
    except Exception as e:
        _digest_slots_last_load_succeeded = False
        log.error("Failed to load digest slots: %s", e)
        return 0

    dedupe_keys: set[tuple[str, bool]] = set()
    for slot in slots:
        slot_key = (slot.get("time", ""), bool(slot.get("is_weekday")))
        if slot_key in dedupe_keys:
            log.warning("Skipping duplicate digest slot %s (%s)", slot.get("time"), "weekday" if slot.get("is_weekday") else "weekend")
            continue
        dedupe_keys.add(slot_key)
        try:
            hour_str, minute_str = slot["time"].split(":")
            hour, minute = int(hour_str), int(minute_str)
        except Exception:
            log.warning("Skipping invalid digest slot time: %r", slot.get("time"))
            continue
        day_of_week = "mon-fri" if slot.get("is_weekday") else "sat,sun"
        job = scheduler.add_job(
            send_digest_for_slot,
            "cron",
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            args=[bot, slot],
        )
        _digest_jobs.append(job)

    if queue_catchup:
        _queue_missed_slots_for_today(scheduler, bot, slots)
    _digest_slots_last_load_succeeded = True
    log.info("Digest schedule built: %d slots registered", len(_digest_jobs))
    return len(_digest_jobs)


async def rebuild_digest_schedule_job(bot, scheduler) -> None:
    was_last_success = _digest_slots_last_load_succeeded
    result = build_digest_schedule(scheduler, bot)
    if result == 0 and was_last_success:
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text="⚠️ Digest schedule rebuild returned 0 slots. Check Digest Selector.",
        )


async def refresh_digest_schedule_job(bot, scheduler) -> None:
    """Periodic silent rebuild so new/edited Digest Selector rows take effect quickly."""
    build_digest_schedule(scheduler, bot)


async def generate_daily_log(bot) -> None:
    """
    Generates end-of-day narrative log and writes it to 📓 Daily Log Notion DB.
    Triggered by a Digest Selector slot with Signoff=True (typically 23:59).
    Runs silently — no Telegram message at generation time.
    Link is sent next morning via send_daily_digest().
    """
    global _last_daily_log_url
    _last_daily_log_url = notion_daily_log.generate_daily_log(
        notion=notion,
        notion_daily_log_db=NOTION_DAILY_LOG_DB,
        notion_db_id=NOTION_DB_ID,
        notion_log_db=NOTION_LOG_DB,
        notion_notes_db=NOTION_NOTES_DB,
        claude=claude,
        claude_model=CLAUDE_MODEL,
        tz=TZ,
        signoff_note=get_and_clear_signoff_note(),
    )


async def send_daily_digest(bot, include_habits: bool = True, config: dict | None = None) -> None:
    global last_digest_msg_id
    if is_muted():
        log.info("Daily digest skipped (muted)")
        return
    tasks = _filter_digest_tasks(get_today_and_overdue_tasks(limit=None), config=config)
    today = local_today()
    overdue = [t for t in tasks if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
    today_tasks = [t for t in tasks if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
    this_week_tasks = [t for t in tasks if t not in overdue and t not in today_tasks]
    ordered = overdue + today_tasks + this_week_tasks
    max_items = config.get("max_items") if config else None
    if isinstance(max_items, int):
        ordered = ordered[:max_items]
        overdue = [t for t in ordered if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
        today_tasks = [t for t in ordered if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
        this_week_tasks = [t for t in ordered if t not in overdue and t not in today_tasks]

    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    lines = [f"☀️ *{date_str}*", ""]
    weather_block = fmt.format_weather_block(wx.fetch_weather("today"), label="🌤️")
    uvi_data = wx.fetch_uvi_data()
    if weather_block and uvi_data:
        max_uvi = uvi_data["max"]
        weather_block += f" · ☀️ {max_uvi:.1f} {fmt.uvi_emoji(max_uvi)}"
    location_label = fmt.digest_location_label()
    if weather_block:
        lines.append(fmt.append_location_to_weather_block(weather_block, location_label))
    else:
        lines.append(fmt.weather_unavailable_digest_line())
    lines.append("")
    n = 1

    habits: list[dict] = []
    habits_enabled = include_habits
    if config and config.get("include_habits") is not None:
        habits_enabled = bool(config.get("include_habits"))
    if habits_enabled:
        morning_habits_all = notion_habits.get_habits_by_time(time_filter="🌅 Morning", notion_query_all=notion_query_all, notion_habit_db=NOTION_HABIT_DB, parse_time_to_minutes=_parse_time_to_minutes, count_habit_completions_this_week=_count_habit_completions_this_week, habit_capped_this_week=habit_capped_this_week) + notion_habits.get_habits_by_time(time_filter="🕐 Anytime", notion_query_all=notion_query_all, notion_habit_db=NOTION_HABIT_DB, parse_time_to_minutes=_parse_time_to_minutes, count_habit_completions_this_week=_count_habit_completions_this_week, habit_capped_this_week=habit_capped_this_week)
        morning_habits_all = [h for h in morning_habits_all if not already_logged_today(h["page_id"]) and not is_on_pace(h)]
        uv_max = uvi_data["max"] if uvi_data else None
        if uv_max is not None:
            habits = [
                h for h in morning_habits_all
                if not h.get("weather_gated") or uv_max >= UV_THRESHOLD
            ]
            log.info(f"UV max today: {uv_max} — threshold: {UV_THRESHOLD}")
        else:
            # UV fetch failed — fail open so weather-gated habits are not silently dropped
            habits = morning_habits_all
            log.warning("UV fetch failed — showing all habits including weather-gated ones")

    if overdue:
        lines.append("🚨 *Overdue*")
        for task in overdue:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if today_tasks:
        lines.append("📌 *Today*")
        for task in today_tasks:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if this_week_tasks:
        lines.append("📅 *This Week*")
        for task in this_week_tasks:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if habits:
        lines.append("*Habits* - tap to log:")
        lines.append("")

    message = "\n".join(lines).strip()
    sent_digest = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(habits, "morning") if habits else None,
    )

    if ordered:
        digest_map[sent_digest.message_id] = ordered
    last_digest_msg_id = sent_digest.message_id
    log.info("Consolidated daily digest sent — %d tasks, %d habits", len(ordered), len(habits))

    global _last_daily_log_url
    if _last_daily_log_url:
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text=f"📓 [Yesterday's log]({_last_daily_log_url})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        _last_daily_log_url = ""


async def send_evening_checkin(bot) -> None:
    """Evening habit check-in with time display and frequency status."""
    evening_habits = pending_habits_for_digest()
    if not evening_habits:
        return

    habit_text = "🌙 *Evening check-in* — did you do these today?\n\n"
    for h in evening_habits[:5]:
        freq_tag = f" _{h['completion_count']}/{h['frequency']}_" if h.get("frequency") else ""
        habit_text += f"⏰ {h['time_str']} — {h['name']}{freq_tag}\n"
    if len(evening_habits) > 5:
        habit_text += f"\n_+{len(evening_habits) - 5} more_"

    await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=habit_text.rstrip(),
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(evening_habits, "evening"),
    )
    log.info("Evening check-in sent — %d habits", len(evening_habits))


async def send_sunday_review(bot) -> None:
    if is_muted():
        log.info("Sunday review skipped (muted)")
        return
    week_tasks = notion_habits.query_tasks_by_auto_horizon(notion=notion, notion_db_id=NOTION_DB_ID, horizons=["🟠 This Week"])
    month_tasks = notion_habits.query_tasks_by_auto_horizon(notion=notion, notion_db_id=NOTION_DB_ID, horizons=["🟡 This Month"])
    header, ordered = fmt.format_sunday_intro(week_tasks, month_tasks)
    sent_review = await bot.send_message(chat_id=MY_CHAT_ID, text=header, parse_mode="Markdown")
    if ordered:
        digest_map[sent_review.message_id] = ordered
        limit = max(1, SUNDAY_REVIEW_CARD_LIMIT)
        for task in ordered[:limit]:
            context_label = task.get("context") or "No context"
            horizon_label = task.get("auto_horizon") or "No horizon"
            await bot.send_message(
                chat_id=MY_CHAT_ID,
                text=f"• *{task.get('name', 'Untitled')}*\n{context_label} · {horizon_label}",
                parse_mode="Markdown",
            )
        overflow = len(ordered) - limit
        if overflow > 0:
            await bot.send_message(
                chat_id=MY_CHAT_ID,
                text=f"…and {overflow} more items not shown to avoid flooding chat.",
            )
    log.info(f"Sunday review sent — {len(ordered)} items")


async def send_daily_habits_list(bot) -> None:
    """Fetch all active habits for today and send as clickable buttons."""
    habits = pending_habits_for_digest()
    if not habits:
        await bot.send_message(chat_id=MY_CHAT_ID, text="🎯 No habits for today.")
        return

    await bot.send_message(
        chat_id=MY_CHAT_ID,
        text="🎯 *Daily habits* — tap to log:",
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(habits, "morning"),
    )
    log.info("Habits list sent — %s available habits", len(habits))


async def run_asana_sync(bot) -> None:
    """
    Bi-directional Asana ↔ Notion reconcile.
    Offloads blocking I/O to thread pool so Telegram event loop stays responsive.
    Self-contained: does not touch Telegram, does not read habit_cache.
    """
    if not ASANA_PAT:
        return  # Sync disabled — bot still works without Asana

    loop = asyncio.get_running_loop()
    sync_status["asana"]["last_run"] = utc_now_iso()
    started = time.monotonic()
    try:
        stats = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: reconcile(
                    notion=notion,
                    notion_db_id=NOTION_DB_ID,
                    asana_token=ASANA_PAT,
                    asana_project_gid=ASANA_PROJECT_GID,
                    asana_workspace_gid=ASANA_WORKSPACE_GID,   # v9.2: required for my_tasks mode
                    source_mode=ASANA_SYNC_SOURCE,
                    archive_orphans=ASANA_ARCHIVE_ORPHANS,
                ),
            ),
            timeout=ASANA_SYNC_TIMEOUT_SECONDS,
        )
        # Only log when something happened — keeps logs readable at 15s polling
        elapsed = round(time.monotonic() - started, 3)
        if any(v for k, v in stats.items() if k != "skipped"):
            log.info(f"Asana sync: {stats} (elapsed={elapsed}s)")
        sync_status["asana"]["ok"] = True
        sync_status["asana"]["error"] = None
        sync_status["asana"]["stats"] = stats
        elapsed = round(time.monotonic() - started, 3)
        metrics = sync_status["asana"]["metrics"]
        durations = metrics.setdefault("durations_seconds", [])
        durations.append(elapsed)
        if len(durations) > 100:
            del durations[:-100]
        metrics["last_duration_seconds"] = elapsed
        metrics["notion_failed_queue_size"] = stats.get("failed_queue_size", 0)
        metrics["deferred_pages"] = stats.get("deferred_pages", 0)
    except TimeoutError:
        log.error("Asana sync timed out after %ss", ASANA_SYNC_TIMEOUT_SECONDS)
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = f"timeout_after_{ASANA_SYNC_TIMEOUT_SECONDS}s"
    except AsanaSyncError as e:
        log.error(f"Asana sync config error: {e}")
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)
    except Exception as e:
        elapsed = round(time.monotonic() - started, 3)
        log.exception(f"Asana sync failed after {elapsed}s: {e}")
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)


async def run_cinema_sync(bot, *, force: bool = False) -> dict[str, int]:
    """Background sync for Cinema Log → Favourite Shows."""
    if not CINEMA_DB_ID:
        return {"scanned": 0, "updated": 0, "skipped": 0, "failed": 0, "tmdb_found": 0, "tmdb_missing": 0, "added_to_fave": 0}

    sync_status["cinema"]["last_run"] = utc_now_iso()
    try:
        stats = await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id=CINEMA_DB_ID,
            fave_db_id=FAVE_DB_ID,
            tmdb_api_key=TMDB_API_KEY,
            force=force,
        )
        log.info(
            "Cinema sync: scanned=%s, updated=%s, skipped=%s, failed=%s, tmdb_found=%s, tmdb_missing=%s, added_to_fave=%s",
            stats["scanned"],
            stats["updated"],
            stats["skipped"],
            stats["failed"],
            stats["tmdb_found"],
            stats["tmdb_missing"],
            stats["added_to_fave"],
        )
        sync_status["cinema"]["ok"] = True
        sync_status["cinema"]["error"] = None
        sync_status["cinema"]["stats"] = stats
    except Exception as e:
        log.exception("Cinema sync failed: %s", e)
        await _try_send_telegram(
            bot,
            "🚨 Cinema sync crashed.\n"
            f"Error: {e}",
        )
        sync_status["cinema"]["ok"] = False
        sync_status["cinema"]["error"] = str(e)
        return {"scanned": 0, "updated": 0, "skipped": 0, "failed": 1, "tmdb_found": 0, "tmdb_missing": 0, "added_to_fave": 0}
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# /habits-data JSON ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

async def habits_data_handler(request: web.Request) -> web.Response:
    try:
        habits_sorted = sorted(habit_cache.values(), key=lambda h: h["sort"])
        today    = datetime.now(TZ).date()
        num_days = WEEKS_HISTORY * 7
        start_dt = today - timedelta(days=num_days - 1)

        results = notion_query_all(
            database_id=NOTION_LOG_DB,
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
            date_str = extract_date_only(d.get("start") if d else None)
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
            streak_results = notion_query_all(
                NOTION_STREAK_DB,
                filter={"property": "Habit", "relation": {"contains": habit["page_id"]}},
            )
            streak_weeks_by_date: dict[date, bool] = {}
            for streak_row in streak_results:
                props = streak_row.get("properties", {})
                week_date_raw = extract_date_only(
                    props.get("Week Of", {}).get("date", {}).get("start"),
                )
                if not week_date_raw:
                    continue
                try:
                    week_date = datetime.fromisoformat(week_date_raw).date()
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
                    day_date = datetime.fromisoformat(date_str).date()
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
            "generated":    datetime.now(TZ).isoformat(),
            "habits":       habits_out,
            "dates":        all_dates,
            "todayDate":    today.isoformat(),
            "weeksHistory": WEEKS_HISTORY,
        }
        return web.Response(
            text=json.dumps(payload),
            content_type="application/json",
            headers=cors_headers(),
        )
    except Exception as e:
        log.error(f"/habits-data error: {e}")
        return web.Response(status=500, text=str(e), headers=cors_headers())



async def log_habit_http_handler(request: web.Request) -> web.Response:
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

        if already_logged_today(matched["page_id"]):
            return web.Response(
                text=json.dumps({"ok": True, "alreadyLogged": True, "habitName": matched["name"]}),
                content_type="application/json",
                headers=cors_headers(),
            )

        log_habit(matched["page_id"], matched["name"], source="🌐 HabitKit")
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


async def start_http_server() -> None:
    app    = web.Application()
    app.router.add_get("/habits-data", habits_data_handler)
    app.router.add_post("/log-habit", log_habit_http_handler)
    app.router.add_options("/log-habit", log_habit_http_handler)
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    register_health_routes(
        app,
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        tz=TZ,
        bot_getter=lambda: _app_bot,
        chat_id=MY_CHAT_ID,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    log.info(f"HTTP server started on port {HTTP_PORT}")


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP HELPERS — schema validation + alert
# ══════════════════════════════════════════════════════════════════════════════

def _format_schema_alert(problems: list[str]) -> str:
    """Telegram-friendly alert message for schema validation problems."""
    bullets = "\n".join(f"• {p}" for p in problems)
    return (
        "🚨 *Asana sync DISABLED — Notion schema check failed*\n\n"
        f"{bullets}\n\n"
        "_Fix the To-Do DB and redeploy. The bot is otherwise running normally "
        "(habits, tasks, digests all work)._"
    )


async def _try_send_telegram(bot, text: str) -> None:
    """Best-effort Telegram alert. Never raises."""
    try:
        kwargs = {
            "chat_id": ALERT_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }
        if ALERT_THREAD_ID is not None:
            kwargs["message_thread_id"] = ALERT_THREAD_ID
        await bot.send_message(**kwargs)
    except Exception as e:
        log.error(f"Could not send operational alert via Telegram: {e}")


def _git_sha() -> str:
    """Best-effort short commit SHA for deploy receipts."""
    # Prefer CI/deploy-provided commit SHAs because production images often
    # don't include a full .git directory (e.g., Render/Heroku containers).
    for env_key in (
        "RAILWAY_GIT_COMMIT_SHA",
        "GIT_SHA",
        "RENDER_GIT_COMMIT",
        "COMMIT_SHA",
        "SOURCE_VERSION",
    ):
        val = os.environ.get(env_key, "").strip()
        if val:
            return val[:12]
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _asana_boot_mode_label() -> str:
    if ASANA_SYNC_SOURCE == "project":
        return f"project:{ASANA_PROJECT_GID or 'missing_gid'}"
    return f"my_tasks:{ASANA_WORKSPACE_GID or 'missing_gid'}"


def v10_feature_flags() -> str:
    flags = [
        f"watchlist={'ON' if NOTION_WATCHLIST_DB else 'OFF'}",
        f"wantslist={'ON' if NOTION_WANTSLIST_V2_DB else 'OFF'}",
        f"photo={'ON' if NOTION_PHOTO_DB else 'OFF'}",
        f"tmdb={'ON' if TMDB_API_KEY else 'OFF (title-only)'}",
        f"notes={'ON' if NOTION_NOTES_DB else 'OFF'}",
        f"weather={'ON' if OPENWEATHER_KEY else 'OFF'}",
        f"mute={'ON' if is_muted() else 'OFF'}",
    ]
    return "  ".join(flags)


def startup_notion_health_check() -> None:
    """Fail fast for core Notion DBs, but don't block startup for optional features."""
    dbs = {
        "NOTION_DB_ID": (NOTION_DB_ID, True),
        "NOTION_HABIT_DB": (NOTION_HABIT_DB, True),
        "NOTION_LOG_DB": (NOTION_LOG_DB, True),
        "NOTION_CINEMA_LOG_DB": (NOTION_CINEMA_LOG_DB, False),
        "NOTION_PERFORMANCE_LOG_DB": (NOTION_PERFORMANCE_LOG_DB, False),
        "NOTION_SPORTS_LOG_DB": (NOTION_SPORTS_LOG_DB, False),
        "NOTION_FAVE_DB": (NOTION_FAVE_DB, False),
        "NOTION_NOTES_DB": (NOTION_NOTES_DB, True),
        "NOTION_DIGEST_SELECTOR_DB": (NOTION_DIGEST_SELECTOR_DB, True),
        "NOTION_WATCHLIST_DB": (NOTION_WATCHLIST_DB, False),
    }
    for label, (db_id, required) in dbs.items():
        if not db_id:
            log.warning("startup_health_check fn=startup_notion_health_check db=%s status=skipped_empty", label)
            continue
        try:
            notion_call(notion.databases.retrieve, database_id=db_id)
            log.info("startup_health_check fn=startup_notion_health_check db=%s status=ok", label)
        except Exception as exc:  # noqa: BLE001
            if required:
                raise RuntimeError(f"Startup health check failed for {label} ({db_id}): {exc}") from exc
            log.warning(
                "startup_health_check fn=startup_notion_health_check db=%s status=failed_optional err=%s",
                label,
                exc,
            )


async def process_pending_programmes(bot) -> None:
    """Poll Weekly Programs DB for unprocessed rows and parse/save asynchronously."""
    if not NOTION_WORKOUT_PROGRAM_DB:
        return

    try:
        results = notion_call(
            notion.databases.query,
            database_id=NOTION_WORKOUT_PROGRAM_DB,
            filter={
                "and": [
                    {"property": "Processed", "checkbox": {"equals": False}},
                    {"property": "Full Program", "rich_text": {"is_not_empty": True}},
                ]
            },
        )
        rows = results.get("results", [])
    except Exception as e:
        log.error("process_pending_programmes: query failed: %s", e)
        return

    if not rows:
        return

    log.info("process_pending_programmes: found %d unprocessed row(s)", len(rows))

    for row in rows:
        page_id = row["id"]
        props = row.get("properties", {})
        title_parts = props.get("Name", {}).get("title", [])
        week_name = title_parts[0].get("plain_text", "") if title_parts else "Unknown week"

        rt = props.get("Full Program", {}).get("rich_text", [])
        full_text = "".join(chunk.get("plain_text", "") for chunk in rt).strip()
        if not full_text:
            continue

        log.info("process_pending_programmes: processing '%s' (%d chars)", week_name, len(full_text))

        try:
            parsed = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: parse_programme(full_text, claude, CLAUDE_MODEL, CLAUDE_PARSE_MAX_TOKENS),
            )

            days_created = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: save_programme_from_notion_row(
                    notion,
                    page_id,
                    NOTION_WORKOUT_DAYS_DB,
                    NOTION_MOVEMENTS_DB,
                    parsed,
                ),
            )

            notion_call(
                notion.pages.update,
                page_id=page_id,
                properties={"Processed": {"checkbox": True}, "Parse Error": {"rich_text": []}},
            )

            tracks = parsed.get("tracks", []) if isinstance(parsed, dict) else []
            track_names = ", ".join(t.get("track", "") for t in tracks if t.get("track"))
            await bot.send_message(
                chat_id=MY_CHAT_ID,
                text=(
                    f"📋 *{week_name}* parsed ✅\n\n"
                    f"Tracks: {track_names or 'N/A'}\n"
                    f"Day rows created: {days_created}\n"
                    f"_Saved to Workout Days_"
                ),
                parse_mode="Markdown",
            )
            log.info("process_pending_programmes: completed '%s'", week_name)
        except Exception as e:
            log.error("process_pending_programmes: failed '%s': %s", week_name, e)
            try:
                notion_call(
                    notion.pages.update,
                    page_id=page_id,
                    properties={"Parse Error": {"rich_text": [{"text": {"content": str(e)[:1900]}}]}},
                )
            except Exception as inner:
                log.error("process_pending_programmes: could not write error to Notion: %s", inner)
            try:
                await bot.send_message(
                    chat_id=MY_CHAT_ID,
                    text=f"⚠️ Couldn't parse *{week_name}*\n\n`{str(e)[:300]}`",
                    parse_mode="Markdown",
                )
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    global _scheduler
    try:
        startup_notion_health_check()
    except RuntimeError as e:
        log.warning("Startup health check failed (bot will still start): %s", e)
        await _try_send_telegram(
            app.bot,
            f"⚠️ Startup health check failed — bot started anyway.\n`{e}`",
        )
    try:
        ent_log.load_entertainment_schemas(notion)
    except Exception as e:
        log.warning("Entertainment schema load failed at startup: %s", e)
    load_mute_state()
    wx.load_location_state()  # load from local JSON cache first (fast)
    if not wx.load_notion_env_location():  # try Notion (authoritative)
        # Notion had no location — geocode from env var or history
        if OPENWEATHER_KEY and (wx.current_lat is None or wx.current_lon is None):
            if not wx.set_location_smart(wx.current_location, claude):
                wx.recover_location_from_history(claude)
    else:
        # Notion loaded successfully — sync back to local JSON cache
        wx.save_location_state(wx.current_location)
    notion_habits.load_habit_cache(notion=notion, notion_habit_db=NOTION_HABIT_DB); _refresh_habit_cache_refs()
    global _app_bot
    _app_bot = app.bot
    await start_http_server()
    scheduler = AsyncIOScheduler(timezone=TZ)
    if FEATURES.get("FEATURE_RECURRING", True):
        scheduler.add_job(run_recurring_check, "cron", hour=_rc_h, minute=_rc_m, args=[app.bot])
    if FEATURES.get("FEATURE_SUNDAY_REVIEW", True):
        scheduler.add_job(send_sunday_review, "cron", day_of_week="sun", hour=_sr_h, minute=_sr_m, args=[app.bot])
    build_digest_schedule(scheduler, app.bot, queue_catchup=True)
    scheduler.add_job(
        rebuild_digest_schedule_job,
        "cron",
        hour=0,
        minute=0,
        args=[app.bot, scheduler],
        id="digest_schedule_rebuild",
    )
    scheduler.add_job(
        refresh_digest_schedule_job,
        "interval",
        minutes=10,
        args=[app.bot, scheduler],
        id="digest_schedule_refresh",
    )
    scheduler.add_job(
        wx.fetch_weather_cache,
        "interval",
        hours=1,
        args=[app.bot],
        id="weather_refresh_hourly",
    )

    scheduler.add_job(
        process_pending_programmes,
        "interval",
        minutes=15,
        args=[app.bot],
        id="process_pending_programmes",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(TZ) + timedelta(minutes=1),
    )

    # ── Asana reconciler — gated by schema validation ──
    asana_status = "OFF"
    smoke_status = "SKIPPED"
    if ASANA_PAT:
        problems = validate_notion_schema(notion, NOTION_DB_ID)
        # v9.2: also catch missing workspace GID for my_tasks mode early
        if ASANA_SYNC_SOURCE == "my_tasks" and not ASANA_WORKSPACE_GID:
            problems.append(
                "ASANA_WORKSPACE_GID env var is required when ASANA_SYNC_SOURCE=my_tasks"
            )
        if problems:
            log.error("Asana sync DISABLED — startup checks failed:")
            for p in problems:
                log.error(f"  - {p}")
            await _try_send_telegram(app.bot, _format_schema_alert(problems))
            asana_status = "DISABLED (schema)"
        else:
            if ASANA_STARTUP_SMOKE:
                try:
                    loop = asyncio.get_running_loop()
                    smoke = await loop.run_in_executor(
                        None,
                        lambda: startup_smoke_test(
                            notion=notion,
                            notion_db_id=NOTION_DB_ID,
                            asana_token=ASANA_PAT,
                            asana_project_gid=ASANA_PROJECT_GID,
                            asana_workspace_gid=ASANA_WORKSPACE_GID,
                            source_mode=ASANA_SYNC_SOURCE,
                        ),
                    )
                    smoke_status = f"PASS (sample={smoke.get('sample_task_gid')})"
                    log.info("Asana startup smoke test passed ✓ %s", smoke)
                except AsanaSyncError as e:
                    smoke_status = f"FAIL ({e})"
                    asana_status = "DISABLED (smoke)"
                    log.error("Asana sync DISABLED — startup smoke failed: %s", e)
                    await _try_send_telegram(
                        app.bot,
                        "🚨 *Asana sync DISABLED — startup smoke test failed*\n\n"
                        f"• {e}\n\n"
                        "_Fix config/integration and redeploy. Scheduler was not started for Asana sync._",
                    )
                except Exception as e:
                    smoke_status = f"FAIL ({e})"
                    asana_status = "DISABLED (smoke)"
                    log.exception("Asana sync DISABLED — unexpected smoke test error: %s", e)
                    await _try_send_telegram(
                        app.bot,
                        "🚨 *Asana sync DISABLED — startup smoke test crashed*\n\n"
                        f"• {e}\n\n"
                        "_Fix and redeploy._",
                    )
            else:
                smoke_status = "SKIPPED (disabled by ASANA_STARTUP_SMOKE)"

        if asana_status not in {"DISABLED (schema)", "DISABLED (smoke)"}:
            scheduler.add_job(
                run_asana_sync,
                "interval",
                seconds=ASANA_SYNC_INTERVAL,
                args=[app.bot],
                id="asana_sync",
                max_instances=ASANA_SYNC_MAX_INSTANCES,
                coalesce=True,                     # Don't backfill missed runs
                misfire_grace_time=ASANA_SYNC_MISFIRE_GRACE_SECONDS,
                next_run_time=datetime.now(TZ),    # Fire once immediately on startup
            )
            asana_status = f"ON ({ASANA_SYNC_INTERVAL}s, mode={ASANA_SYNC_SOURCE})"
            log.info("Notion schema validation passed ✓")

    # ── Cinema sync — config validation + hourly background schedule ──
    cinema_ok, cinema_problems = validate_cinema_config()
    if not cinema_ok:
        log.warning("Cinema sync disabled due to config issues:")
        for p in cinema_problems:
            log.warning(f"  - {p}")
    elif CINEMA_DB_ID:
        register_cinema_jobs(
            scheduler=scheduler,
            bot=app.bot,
            run_cinema_sync=run_cinema_sync,
            sync_interval_minutes=60,
            tz=TZ,
            now_fn=datetime.now,
        )
        log.info(
            "Cinema sync jobs registered (every 60 minutes)",
        )

    async def _run_steps_final_stamp(bot) -> None:
        await handle_steps_final_stamp(
            notion=notion,
            habit_db_id=NOTION_HABIT_DB,
            log_db_id=NOTION_LOG_DB,
            habit_name=STEPS_HABIT_NAME,
            threshold=STEPS_THRESHOLD,
            source_label=STEPS_SOURCE_LABEL,
            tz=TZ,
            bot=bot,
            chat_id=MY_CHAT_ID,
        )

    scheduler.add_job(
        _run_steps_final_stamp,
        "cron",
        hour=STEPS_FINAL_HOUR,
        minute=STEPS_FINAL_MIN,
        args=[app.bot],
        id="steps_final_stamp",
    )

    scheduler.start()
    _scheduler = scheduler
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"sunday_review={_sr_h:02d}:{_sr_m:02d}  "
        f"weather=hourly  "
        f"recurring={_rc_h:02d}:{_rc_m:02d}  "
        f"asana_sync={asana_status}  smoke={smoke_status}  "
        f"archive_orphans={ASANA_ARCHIVE_ORPHANS}  "
        f"v10_flags=[{v10_feature_flags()}]"
    )
    await _try_send_telegram(
        app.bot,
        f"🚀 {APP_VERSION} booted\n"
        f"sha={_git_sha()}\n"
        f"asana={asana_status}\n"
        f"source={_asana_boot_mode_label()}\n"
        f"archive_orphans={ASANA_ARCHIVE_ORPHANS}\n"
        f"smoke={smoke_status}\n"
        f"features={v10_feature_flags()}",
    )
    await app.bot.set_my_commands(
        [
            BotCommand("done", "Mark task/habit done"),
            BotCommand("remind", "Show quick reminder"),
            BotCommand("r", "Alias for /remind"),
            BotCommand("notes", "Open notes capture"),
            BotCommand("weather", "Show weather snapshot"),
            BotCommand("habits", "Show habits list"),
            BotCommand("log", "Log cinema/performance/sport"),
            BotCommand("sync", "Run manual sync"),
            BotCommand("syncstatus", "Show sync status"),
            BotCommand("mute", "Pause scheduled digests"),
            BotCommand("unmute", "Resume scheduled digests"),
            BotCommand("location", "Set weather location"),
        ]
    )


# ══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS — defined before main() so Python can resolve names
# ══════════════════════════════════════════════════════════════════════════════

async def handle_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/done — combined habit + task picker."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    pending_habits = [
        h for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
        if not already_logged_today(h["page_id"])
    ]
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    if not pending_habits and not tasks:
        await update.message.reply_text("✅ Everything done for today — nothing left to log!")
        return
    if pending_habits:
        await update.message.reply_text(
            "🏃 *Which habit did you complete?*",
            parse_mode="Markdown",
            reply_markup=kb.habit_buttons(pending_habits, "manual"),
        )
    if tasks:
        global _done_picker_counter
        key = str(_done_picker_counter); _done_picker_counter += 1
        done_picker_map[key] = tasks
        await update.message.reply_text(
            "✅ *Which task did you finish?*",
            parse_mode="Markdown",
            reply_markup=done_picker_keyboard(key, page=0),
        )


async def handle_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start log_<habit> — optional Telegram deep-link fallback."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    args = context.args
    if not args or not args[0].startswith("log_"):
        await update.message.reply_text(
            "👋 *Second Brain Bot*\n\nSend me any task or habit to capture it.\nUse /done to mark completions.\nUse /r or /remind for your quick snapshot.\nUse /notes for Notes capture and /weather for forecast.",
            parse_mode="Markdown",
        )
        await refresh_quick_actions_keyboard(update.message)
        return
    raw     = args[0][4:].replace("_", " ").strip()
    matched = next((h for h in habit_cache.values() if raw.lower() in h["name"].lower()), None)
    if not matched:
        await update.message.reply_text(f"Couldn't find a habit matching *{raw}*.", parse_mode="Markdown")
        return
    pid  = matched["page_id"]
    name = matched["name"]
    if already_logged_today(pid):
        await update.message.reply_text(f"Already logged *{name}* today! ✅", parse_mode="Markdown")
        return
    log_habit(pid, name)
    await update.message.reply_text(
        f"✅ Logged!\n\n{name}\n📅 {datetime.now(TZ).strftime('%B %-d')}",
        parse_mode="Markdown",
    )
    await refresh_quick_actions_keyboard(update.message)


async def handle_remind_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/r and /remind — quick to-do reminder snapshot."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await send_quick_reminder(update.message, mode="priority")


async def handle_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/sync — manual catch-up trigger for core sync pipelines."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    args = [a.strip().lower() for a in (context.args or []) if a.strip()]
    cinema_only = args[:1] == ["cinema"]
    status = await update.message.reply_text(
        "🔄 Running cinema sync…" if cinema_only else "🔄 Running full sync (Asana + Cinema + Habit cache)…"
    )
    try:
        notion_habits.load_habit_cache(notion=notion, notion_habit_db=NOTION_HABIT_DB); _refresh_habit_cache_refs()
        if not cinema_only:
            await run_asana_sync(context.bot)
        cinema_stats = await run_cinema_sync(context.bot)
        await status.edit_text(
            "✅ Sync finished.\n"
            f"Cinema: scanned={cinema_stats['scanned']} updated={cinema_stats['updated']} "
            f"missing={cinema_stats['tmdb_missing']} skipped={cinema_stats['skipped']} failed={cinema_stats['failed']}"
        )
    except Exception as e:
        log.exception("Manual /sync failed: %s", e)
        await status.edit_text(f"⚠️ /sync failed: {e}")


async def handle_sync_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/syncstatus — show latest sync telemetry for Asana + Cinema."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await update.message.reply_text(format_sync_status_message(sync_status), parse_mode="Markdown")


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt for mute duration in days."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    context.user_data["awaiting_mute_days"] = True
    await update.message.reply_text("🔕 How many days should I pause scheduled digests?")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear mute state immediately."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    global mute_until
    mute_until = None
    save_mute_state()
    context.user_data["awaiting_mute_days"] = False
    await update.message.reply_text("🔔 Digests resumed.")


async def cmd_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt for a new weather location or parse inline /location arguments."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    location_text = " ".join(context.args or []).strip()
    if location_text:
        if wx.set_location_smart(location_text, claude):
            context.user_data["awaiting_location"] = False
            await update.message.reply_text(f"📍 Location updated to {wx.current_location}.")
            wx.save_location_state(wx.current_location)
            await update.message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
            return
        await update.message.reply_text(
            "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
        )
        return
    context.user_data["awaiting_location"] = True
    await update.message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/weather — show current + upcoming forecast snapshot."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    try:
        await update.message.reply_text(fmt.format_weather_snapshot(), parse_mode="Markdown")
    except Exception as e:
        log.error("/weather failed: %s", e)
        await update.message.reply_text("⚠️ Weather is temporarily unavailable. Try again in a moment or send /location.")


async def cmd_notes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/notes — open note capture shortcuts and show connection status."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    if NOTION_NOTES_DB:
        await update.message.reply_text("📝 Notes connected. Choose an option:", reply_markup=kb.notes_options_keyboard())
    else:
        await update.message.reply_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")


async def cmd_habits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/habits — show incomplete habits as one-tap check-ins."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await send_daily_habits_list(context.bot)


async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/log <cinema|movie|performance|sport> <title> at <venue> — explicit entertainment logging."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    raw = " ".join(context.args or []).strip()
    parsed = ent_log.parse_explicit_entertainment_log(f"/log {raw}")
    if not parsed:
        await update.message.reply_text(
            "Usage:\n"
            "/log cinema Dune at AMC\n"
            "/log movie Dune at AMC\n"
            "/log performance ABBA Voyage at ABBA Arena\n"
            "/log sport Cubs vs Sox at Wrigley"
        )
        return
    try:
        prompted = await ent_log._maybe_prompt_explicit_venue(notion, update.message, parsed, f"/log {raw}")
        if prompted:
            return
        await ent_log.handle_entertainment_log(notion, update.message, parsed)
    except Exception as e:
        log.error("Explicit /log save error: %s", e)
        await update.message.reply_text(_entertainment_save_error_text(e, parsed))



# ══════════════════════════════════════════════════════════════════════════════
# MAIN — after all handlers are defined
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", handle_start_command))
    app.add_handler(CommandHandler("r", handle_remind_command))
    app.add_handler(CommandHandler("remind", handle_remind_command))
    app.add_handler(CommandHandler("sync", handle_sync_command))
    app.add_handler(CommandHandler("syncstatus", handle_sync_status_command))
    app.add_handler(CommandHandler("done",  handle_done_command))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("weather", cmd_weather))
    app.add_handler(CommandHandler("notes", cmd_notes))
    app.add_handler(CommandHandler("location", cmd_location))
    app.add_handler(CommandHandler("habits", cmd_habits))
    app.add_handler(CommandHandler("log", cmd_log))
    app.add_handler(CommandHandler("trip", handle_trip_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_text))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info(f"🤖 Second Brain bot starting ({APP_VERSION})...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
