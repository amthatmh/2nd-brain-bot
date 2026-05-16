#!/usr/bin/env python3
"""Second Brain — Telegram bot entry point and handler wiring."""

import asyncio
import os
import json
import re
import importlib
import logging
import calendar
import subprocess
import time
import urllib.parse
import uuid
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
from typing import Callable

from zoneinfo import ZoneInfo
from aiohttp import web
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
)
from telegram.helpers import escape_markdown as _escape_markdown_v2
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from notion_client import Client as NotionClient

from second_brain.asana.sync import (
    reconcile,
    AsanaSyncError,
    validate_notion_schema,
    startup_smoke_test,
)
from second_brain.cinema.sync import sync_cinema_log_to_notion
from second_brain.cinema.config import (
    CINEMA_DB_ID,
    FAVE_DB_ID,
    TMDB_API_KEY,
    validate_config as validate_cinema_config,
)
from second_brain.sync_telemetry import init_sync_status, utc_now_iso, format_sync_status_message
from second_brain.notion import notes as notion_notes
from second_brain.digest import manual_digest_config_now as _manual_digest_config_now_fn
from second_brain.notion import daily_log as notion_daily_log
from second_brain.notes.flow import (
    ordered_topics,
    note_topics_keyboard,
)
from second_brain.ai import classify as ai_classify
from second_brain.ai.client import get_claude_client
from second_brain.healthtrack.routes import register_health_routes
from second_brain.healthtrack import config as health_config
from second_brain.healthtrack.steps import (
    _find_steps_habit_page_id,
    backfill_steps_state_from_notion,
    handle_steps_final_stamp,
    migrate_steps_entry_titles,
)
from second_brain.healthtrack.scheduler import check_and_create_steps_entry
import second_brain.config as _config_module
_config_module = importlib.reload(_config_module)
import second_brain.config as config
from second_brain.config import (
    TELEGRAM_TOKEN,
    MY_CHAT_ID,
    ALERT_CHAT_ID,
    ALERT_THREAD_ID,
    ANTHROPIC_KEY,
    NOTION_TOKEN,
    NOTION_DB_ID,
    NOTION_HABIT_DB,
    NOTION_LOG_DB,
    NOTION_HEALTH_METRICS_DB,
    NOTION_STREAK_DB,
    NOTION_CINEMA_LOG_DB,
    NOTION_PERFORMANCE_LOG_DB,
    NOTION_SPORTS_LOG_DB,
    NOTION_FAVE_DB,
    NOTION_NOTES_DB,
    NOTION_DIGEST_SELECTOR_DB,
    NOTION_UTILITY_SCHEDULER_DB,
    NOTION_DAILY_LOG_DB,
    NOTION_PACKING_ITEMS_DB,
    NOTION_TRIPS_DB,
    OPENWEATHER_KEY,
    WEATHER_LOCATION,
    TZ,
    CLAUDE_MODEL,
    CLAUDE_MAX_TOK,
    CLAUDE_PARSE_MAX_TOKENS,
    NOTION_MOVEMENTS_DB,
    NOTION_WORKOUT_PROGRAM_DB,
    NOTION_WORKOUT_DAYS_DB,
    NOTION_WORKOUT_LOG_DB,
    NOTION_WOD_LOG_DB,
    NOTION_PROGRESSIONS_DB,
    NOTION_DAILY_READINESS_DB,
    NOTION_WATCHLIST_DB,
    NOTION_WANTSLIST_V2_DB,
    NOTION_PHOTO_DB,
    NOTION_ENV_DB,
    NOTION_BOOT_LOG_DB,
    ASANA_SYNC_INTERVAL,
    HTTP_PORT,
    WEEKS_HISTORY,
    APP_VERSION,
    UV_THRESHOLD,
    TMDB_BASE,
    FEATURES,
    UTILITY_SCHEDULER_RELOAD_MINUTES,
    ASANA_PAT,
    ASANA_PROJECT_GID,
    ASANA_WORKSPACE_GID,
    ASANA_SYNC_SOURCE,
    ASANA_ARCHIVE_ORPHANS,
)
from second_brain.notion import notion_call
from second_brain.notion.properties import (
    query_all,
    rich_text_prop,
    title_prop,
)
from second_brain.notion import habits as notion_habits
from second_brain.notion.habits import (
    log_habit as _habit_log_habit,
    already_logged_today as _habit_already_logged,
    get_week_completion_count as _habit_week_count,
    get_habit_frequency as _habit_frequency,
    habit_capped_this_week as _habit_capped,
    _count_habit_completions_this_week as _habit_count_this_week,
    logs_this_week as _habit_logs_this_week,
    is_on_pace as _habit_is_on_pace,
    record_weekly_streaks,
)
from second_brain.notion.tasks import next_repeat_day_date
from second_brain.notion import tasks as notion_tasks
from second_brain import keyboards as kb
from second_brain import formatters as fmt
from second_brain import digest as digest_helpers
from second_brain import mute as mute_helpers
from second_brain.utils import _safe_user_error, get_current_monday, parse_time_to_minutes
from second_brain.digest import (
    get_digest_config,
    _filter_digest_tasks,
    send_digest_for_slot,
    _queue_missed_slots_for_today,
    build_digest_schedule,
    rebuild_digest_schedule_job,
    refresh_digest_schedule_job,
    generate_daily_log,
    send_daily_digest,
)
from second_brain import palette as palette_helpers
from second_brain import weather as wx
from second_brain import watchlist as wl
from second_brain import trips as trips_mod
from second_brain.trips import (
    handle_trip_command,
    fetch_weather,
    weather_triggered_items,
    _scheduler_run_datetime,
    schedule_weather_refresh,
    run_weather_refresh,
    cmd_refreshweather,
    get_upcoming_trips_needing_reminder,
    mark_trip_reminder_sent,
    format_trip_reminder_block,
    append_trip_reminders_to_text,
    update_trip_weather_job,
    refresh_trip_weather_job,
    _run_trip_weather_refresh,
    handle_trip_weather_refresh,
)
from second_brain.handler_registry import register_core_handlers
from second_brain.scheduler_manager import UtilitySchedulerManager
from second_brain.rules.engine import RuleEngine
from second_brain.state import STATE
from second_brain.utils import ExpiringDict, local_today
from second_brain.http_utils import cors_headers
from second_brain.healthtrack.dashboard import create_health_dashboard_handler, load_steps_threshold_from_env_db as load_dashboard_steps_threshold
from second_brain.services import task_parsing as task_parsing_service
from second_brain.services import note_utils as note_utils_service
from second_brain.handlers.commands import CommandHandlers
from second_brain.handlers.admin_commands import test_alert_command, test_channel_send
from second_brain.monitoring import track_job_execution
from second_brain.boot import git_sha as _git_sha

from second_brain.monitoring.metrics import generate_weekly_summary
from utils.date_parser import parse_date
from utils.alert_handlers import (
    alert_digest_sent,
    alert_scheduler_event,
    alert_startup,
)

from second_brain.crossfit.classify import classify_workout_message
from second_brain.crossfit.handlers import (
    MOVEMENTS_CACHE,
    handle_cf_callback,
    handle_cf_strength_flow,
    handle_cf_text_reply,
    handle_cf_upload_programme,
    handle_cf_wod_flow,
    reload_movement_library,
)
from second_brain.crossfit.keyboards import crossfit_submenu_keyboard
from second_brain.crossfit.readiness import check_readiness_logged_today
from second_brain.crossfit.notion import parse_weekly_program_text, save_programme_from_notion_row, this_monday
from second_brain.entertainment import log as ent_log
from second_brain.entertainment.handlers import (
    _entertainment_rule_entry_data,
    handle_entertainment_log as _ent_handle_log,
    _maybe_prompt_explicit_venue,
    load_entertainment_schemas,
)
from second_brain.routers import (
    handle_message_text,
    handle_callback,
    route_classified_message_v10,
)

def _apply_shared_date_parse(payload: dict) -> object:
    raw_date = payload.get("date")
    if isinstance(raw_date, str) and "T" in raw_date:
        return None
    result = parse_date(raw_date, today=local_today())
    if result.ambiguous:
        payload["raw_date_a"] = result.option_a
        payload["raw_date_b"] = result.option_b
    else:
        payload["date"] = result.resolved
    return result

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)
logger = log

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
_rc_h, _rc_m = config.parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")

def format_reminder_snapshot(mode: str = "priority", limit: int = 8) -> str:
    return fmt.format_reminder_snapshot(notion, NOTION_DB_ID, TZ, mode=mode, limit=limit)

def load_notion_env_config() -> dict[str, str]:
    """
    Read scalar config rows from Notion ENV DB.

    Returns dict of {Name: Value} for all rows that have a Value.
    Falls back gracefully — never raises.
    """
    if not NOTION_ENV_DB:
        return {}

    try:
        config: dict[str, str] = {}
        for row in query_all(notion, NOTION_ENV_DB, page_size=None):
            props = row.get("properties", {})
            name_parts = props.get("Name", {}).get("title", [])
            name = "".join(p.get("plain_text", "") for p in name_parts).strip()
            value_parts = props.get("Value", {}).get("rich_text", [])
            value = value_parts[0].get("text", {}).get("content", "").strip() if value_parts else ""
            if name and value:
                config[name] = value

        return config
    except Exception as e:
        log.warning("load_notion_env_config failed: %s", e)
        return {}

async def write_boot_log(*args, **kwargs):
    from second_brain.boot import write_boot_log as _impl
    kwargs.setdefault("notion", notion)
    kwargs.setdefault("boot_log_db", NOTION_BOOT_LOG_DB)
    kwargs.setdefault("tz", TZ)
    return await _impl(*args, **kwargs)

# ── Clients ──────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = get_claude_client()
wx.notion = notion
wx.NOTION_ENV_DB = NOTION_ENV_DB
wx._loc.location = WEATHER_LOCATION

# ── Cache TTLs ───────────────────────────────────────────────────────────────
_PREVIEW_CACHE_TTL = 900    # 15 min — task preview confirmations
_CF_PENDING_TTL = 3600      # 1 hr  — crossfit in-progress flow state
_HABITS_DATA_TTL = 300      # 5 min — HTTP /habits-data endpoint cache

# ── In-memory state ──────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = STATE.digest_map
pending_map: dict[str, dict] = STATE.pending_map
capture_map: dict[int, dict] = STATE.capture_map
pending_batches: dict[str, dict] = {}
preview_map: dict[int, dict] = ExpiringDict(ttl_seconds=_PREVIEW_CACHE_TTL)
done_picker_map: dict[str, list[dict]] = STATE.done_picker_map
todo_picker_map: dict[str, list[dict]] = {}
pending_message_map: dict[str, str] = {}
pending_note_map: dict[str, dict] = {}
cf_pending: dict[str, dict] = ExpiringDict(ttl_seconds=_CF_PENDING_TTL)
topic_recency_map: dict[str, datetime] = {}
_cf_counter = 0
habit_cache: dict[str, dict] = STATE.habit_cache
# Preserve prior module-level semantics when this entrypoint is reloaded in tests or workers.
STATE.done_picker_counter = 0
STATE.todo_picker_counter = 0
STATE.v10_counter = 0
STATE.habits_data_cache = ExpiringDict(ttl_seconds=_HABITS_DATA_TTL)
STATE.mute_until = None
STATE.signoff_notes_today = {"second_brain": "", "brian_ii": ""}
STATE.claude_activity_today = []

_habit_selections: dict[int, dict[str, object]] = {}

def _store_habit_selection_session(message_id: int, habits: list[dict], selected: set[str] | None = None) -> None:
    """Cache habit button state for a rendered Telegram message."""
    _habit_selections[message_id] = {"selected": selected or set(), "habits": habits}

def _habit_selection_session(message_id: int) -> dict[str, object]:
    """Return a habit selection session, creating one if absent."""
    session = _habit_selections.get(message_id)
    if isinstance(session, dict):
        session.setdefault("selected", set())
        session.setdefault("habits", [])
        return session
    session = {"selected": set(), "habits": []}
    _habit_selections[message_id] = session
    return session

def _habit_selection_selected(message_id: int) -> set[str]:
    """Return selected habit IDs for a message."""
    selected = _habit_selection_session(message_id).get("selected", set())
    return selected if isinstance(selected, set) else set()

def _habit_selection_habits(message_id: int) -> list[dict]:
    """Return cached habits for a message."""
    habits = _habit_selection_session(message_id).get("habits", [])
    return habits if isinstance(habits, list) else []

def cleanup_old_habit_selections() -> None:
    """Clear in-memory habit button selections to prevent stale message state."""
    _habit_selections.clear()

def cleanup_pending_task_interactions() -> None:
    """Trigger TTL purges for preview interactions."""
    preview_map.get("__ttl_purge__")

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

notified_goals_this_week: set[str] = set()
_scheduler: AsyncIOScheduler | None = None
_app_bot = None  # set during post_init for health route bot access
rule_engine: RuleEngine | None = None
_steps_title_migration_ran = False
STATE_DIR = _resolve_state_dir()
mute_state_file = STATE_DIR / "mute_state.json"

# ── Constants ────────────────────────────────────────────────────────────────
_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)
BTN_REFRESH = "📜Digest"
BTN_ALL_OPEN = "✅ To Do"
BTN_HABITS = "🏃 Habits"
BTN_CROSSFIT = "💪 CrossFit"
BTN_NOTES = "📝 Notes"
BTN_WEATHER = "🌤️ Weather"
BTN_MUTE = "🔕 Mute"
TOPIC_OPTIONS = [
    "🎵 Acoustics", "💼 Work", "🏠 Personal",
    "💪 Health", "🏢 LEED", "✅ WELL", "💡 Ideas", "📚 Research",
]
_URL_RE = re.compile(r"https?://[^\s\)\]>\"']+", re.IGNORECASE)

def _has_explicit_personal_or_work_context(text: str) -> bool:
    lower = (text or "").lower()
    return bool(re.search(r"\b(personal|work)\b|🏠|💼", lower))

def _load_mute_state() -> None:
    STATE.mute_until = mute_helpers.load_mute_state(mute_state_file, TZ, log)

def _save_mute_state() -> None:
    mute_helpers.save_mute_state(STATE.mute_until, mute_state_file, log)

def _is_muted() -> bool:
    if not mute_helpers.is_muted(STATE.mute_until, TZ):
        if STATE.mute_until is None:
            return False
        STATE.mute_until = None
        _save_mute_state()
        return False
    return True

# ══════════════════════════════════════════════════════════════════════════════
# HABIT CACHE
# ══════════════════════════════════════════════════════════════════════════════

def notion_query_all(database_id: str, **kwargs) -> list[dict]:
    """Return all rows from a Notion database query (handles pagination)."""
    return query_all(
        notion,
        database_id,
        filter=kwargs.pop("filter", None),
        sorts=kwargs.pop("sorts", None),
        page_size=kwargs.pop("page_size", None),
    )

def extract_date_only(date_str: str | None) -> str | None:
    """Normalize Notion date strings to YYYY-MM-DD for calendar matching."""
    return note_utils_service.extract_date_only(date_str)

# ══════════════════════════════════════════════════════════════════════════════
# MULTI-TASK PARSING
# ══════════════════════════════════════════════════════════════════════════════

def split_tasks(text: str) -> list[str]:
    return task_parsing_service.split_tasks(text, _BULLET_RE)

def looks_like_crossfit_programme(text: str) -> bool:
    return task_parsing_service.looks_like_crossfit_programme(text)

def looks_like_task_batch(text: str) -> bool:
    return task_parsing_service.looks_like_task_batch(text, _BULLET_RE)

def infer_batch_overrides(text: str) -> dict:
    return task_parsing_service.infer_batch_overrides(text)

def infer_deadline_override(text: str) -> int | None:
    return task_parsing_service.infer_deadline_override(text)

# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

async def start_note_capture_flow(message, text: str) -> None:
    if not NOTION_NOTES_DB:
        await create_or_prompt_task(message, text)
        return

    note_key = str(STATE.v10_counter)
    STATE.v10_counter += 1
    try:
        topics = notion_notes.fetch_note_topics_from_notion(notion, NOTION_NOTES_DB)
    except Exception as e:
        log.error("Failed to read note topics from Notion schema: %s", e)
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
        log.error("Notion note error: %s", e)
        await message.reply_text("⚠️ Couldn't save note to Notion.")

def extract_url(text: str) -> str | None:
    """Return first URL found in text, or None."""
    return note_utils_service.extract_url(text, _URL_RE)

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
        log.warning("fetch_url_metadata failed for %s: %s", url, e)
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
        log.error("save_note error: %s", e)
        await thinking.edit_text("⚠️ Couldn't save note to Notion.", parse_mode="Markdown")

def deadline_days_to_label(days: int | None) -> str:
    return note_utils_service.deadline_days_to_label(days)

# ══════════════════════════════════════════════════════════════════════════════
# V10 REFERENCE DATABASE FLOWS
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# NOTION — HABIT LOG
# ══════════════════════════════════════════════════════════════════════════════

def log_habit(habit_page_id: str, habit_name: str, source: str = "📱 Telegram") -> None:
    return _habit_log_habit(notion, NOTION_LOG_DB, habit_page_id, habit_name, source)

def already_logged_today(habit_page_id: str) -> bool:
    return _habit_already_logged(notion, NOTION_LOG_DB, habit_page_id, TZ)

def get_week_completion_count(habit_page_id: str) -> int:
    return _habit_week_count(notion, NOTION_LOG_DB, habit_page_id, TZ)

def get_habit_frequency(habit_page_id: str) -> int:
    return _habit_frequency(notion, habit_page_id)

def habit_capped_this_week(habit_page_id: str) -> bool:
    return _habit_capped(notion, NOTION_LOG_DB, habit_page_id, TZ)

def _count_habit_completions_this_week(habit_page_id: str) -> int:
    return _habit_count_this_week(notion, NOTION_LOG_DB, habit_page_id, TZ)

def logs_this_week(habit_page_id: str) -> int:
    return _habit_logs_this_week(notion, NOTION_LOG_DB, habit_page_id, TZ)

def is_on_pace(habit: dict) -> bool:
    return _habit_is_on_pace(notion, NOTION_LOG_DB, habit, TZ)

# ══════════════════════════════════════════════════════════════════════════════
# NOTION — TO-DO
# ══════════════════════════════════════════════════════════════════════════════

def store_signoff_note(project: str, text: str) -> None:
    if project not in STATE.signoff_notes_today:
        log.warning("Unknown signoff project: %s", project)
        return
    STATE.signoff_notes_today[project] = text.strip()
    log.info("Signoff note stored for %s: %s", project, text[:80])

def is_muted() -> bool:
    return _is_muted()

def get_and_clear_project_signoff_notes() -> dict[str, str]:
    notes = STATE.signoff_notes_today.copy()
    STATE.signoff_notes_today = {"second_brain": "", "brian_ii": ""}
    return notes

async def trigger_signoff_now(message, note: str | None = None, project: str = "second_brain") -> None:
    if note:
        store_signoff_note(project, note)
    await generate_daily_log(message.get_bot())
    note_msg = f"\n\n📝 {_escape_markdown_v2(note[:180])}" if note else ""
    await message.reply_text(
        "📓 Daily log note captured — daily log generated now." + note_msg,
        parse_mode="MarkdownV2" if note else None,
    )

def _get_today_tasks_for_palette() -> list[dict]:
    return palette_helpers.get_today_tasks_for_palette(
        notion_tasks=notion_tasks, notion=notion, notion_db_id=NOTION_DB_ID, local_today_fn=local_today
    )

def track_claude_activity(text: str) -> None:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    if not cleaned:
        return
    timestamp = datetime.now(TZ).strftime("%H:%M")
    STATE.claude_activity_today.append(f"{timestamp} — {cleaned[:200]}")
    if len(STATE.claude_activity_today) > 60:
        STATE.claude_activity_today = STATE.claude_activity_today[-60:]

def get_and_clear_claude_activity() -> list[str]:
    items = STATE.claude_activity_today
    STATE.claude_activity_today = []
    return items

# ══════════════════════════════════════════════════════════════════════════════
# BATCH CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

def _run_capture(raw_text: str, force_create: bool = False,
                 context_override: str | None = None,
                 deadline_override: int | None = None) -> dict:
    try:
        habit_names = list(habit_cache.keys())
        today = local_today()
        result        = ai_classify.classify_message(claude, CLAUDE_MODEL, raw_text, habit_names, bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), today)
        task_name     = result.get("task_name") or raw_text
        deadline_days = result.get("deadline_days")
        ctx           = context_override or result.get("context", "🏠 Personal")
        recurring, repeat_day = _recurring_fields_from_classification(result)
        target_date = next_repeat_day_date(recurring, repeat_day)
        if target_date is not None:
            computed_days = (target_date - local_today()).days
            if deadline_days is None or (deadline_days <= 0 and computed_days > 0):
                deadline_days = computed_days
        if deadline_override is not None:
            deadline_days = deadline_override
        explicit_deadline = infer_deadline_override(raw_text)
        if explicit_deadline is not None:
            deadline_days = explicit_deadline
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error("Claude error for '%s': %s", raw_text, e)
        return {"status": "error", "name": raw_text, "error": str(e)}

    if not force_create:
        dup = notion_tasks.find_duplicate_active_task(notion, NOTION_DB_ID, task_name)
        if dup:
            return {"status": "duplicate", "name": task_name, "duplicate": dup}

    try:
        if recurring != "None":
            page_id, first_deadline = _create_recurring_task_template_and_first_instance(
                task_name, ctx, recurring, repeat_day
            )
            return {
                "status": "captured", "name": task_name,
                "horizon_label": first_deadline.strftime("%b %d"), "context": ctx,
                "recurring": recurring, "page_id": page_id,
            }

        page_id = notion_tasks.create_task(notion, NOTION_DB_ID, task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
        return {
            "status": "captured", "name": task_name,
            "horizon_label": horizon_label, "context": ctx,
            "recurring": recurring, "page_id": page_id,
        }
    except Exception as e:
        log.error("Notion error for '%s': %s", task_name, e)
        return {"status": "error", "name": task_name, "error": str(e)}

async def refresh_quick_actions_keyboard(message) -> None:
    """Force-refresh the reply keyboard to replace legacy layouts (e.g. old Mute button)."""
    await message.reply_text("🔄 Refreshing quick actions…", reply_markup=ReplyKeyboardRemove())
    await message.reply_text(
        "✅ Quick actions updated.",
        reply_markup=kb.quick_actions_keyboard(BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS, BTN_CROSSFIT, BTN_NOTES, BTN_WEATHER),
    )

async def send_quick_reminder(message, mode: str = "priority") -> None:
    await message.reply_text(
        fmt.format_reminder_snapshot(notion, NOTION_DB_ID, TZ, mode=mode),
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
#   tdc:{key}              - Close/cancel to-do picker
#
# HABITS (h) — Morning/evening habit check-in
#   h:toggle:{pid}         - Toggle a habit selection (morning/evening/manual)
#   h:done                 - Log selected habits to Notion
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
    suffix = "\n↻ Next instance will be generated by the recurring batch job" if notion_tasks.handle_done_recurring(notion, NOTION_DB_ID, page_id) else ""
    await message.reply_text(f"✅ Done: {name}{suffix}")

async def _classify_task_texts(task_texts: list[str]) -> list[dict]:
    """Classify multiple task texts concurrently."""
    loop = asyncio.get_running_loop()
    habit_names = list(habit_cache.keys())
    today = local_today()
    return await asyncio.gather(*[
        loop.run_in_executor(
            None,
            ai_classify.classify_message,
            claude,
            CLAUDE_MODEL,
            task_text,
            habit_names,
            bool(NOTION_WATCHLIST_DB),
            bool(NOTION_WANTSLIST_V2_DB),
            bool(NOTION_PHOTO_DB),
            bool(NOTION_NOTES_DB),
            today,
        )
        for task_text in task_texts
    ])

def _recurring_fields_from_classification(classification: dict) -> tuple[str, str | None]:
    recurring_type = classification.get("recurring_type")
    recurring_map = {
        "daily": "🔁 Daily",
        "weekly": "📅 Weekly",
        "monthly": "🗓️ Monthly",
    }
    recurring = recurring_map.get(recurring_type, classification.get("recurring", "None") or "None")
    repeat_day = classification.get("repeat_day")
    return recurring, repeat_day


def _create_recurring_task_template_and_first_instance(
    task_name: str,
    ctx: str,
    recurring: str,
    repeat_day: str | None,
) -> tuple[str, date]:
    props = {
        "Name": title_prop(task_name),
        "Context": {"select": {"name": ctx}},
        "Recurring": {"select": {"name": recurring}},
        "Is Template": {"checkbox": True},
        "Source": {"select": {"name": "📱 Telegram"}},
    }
    if repeat_day:
        props["Repeat Day"] = {"select": {"name": repeat_day}}

    template_id = notion.pages.create(
        parent={"database_id": NOTION_DB_ID},
        properties=props,
    )["id"]

    template_dict = {
        "page_id": template_id,
        "name": task_name,
        "context": ctx,
        "recurring": recurring,
        "repeat_day": repeat_day,
        "deadline": None,
        "recurrence_pattern": None,
    }
    first_deadline = notion_tasks.calculate_next_deadline(
        template_dict,
        from_date=notion_tasks.local_today(),
    )
    notion_tasks.spawn_recurring_instance(
        notion,
        NOTION_DB_ID,
        template_dict,
        next_deadline=first_deadline,
        source="📱 Telegram",
    )
    return template_id, first_deadline


async def _create_task_from_classification(
    raw_text: str,
    classification: dict,
    context_override: str | None,
    deadline_override: int | None,
    force_create: bool,
) -> dict:
    """Create one Notion task from a precomputed classification."""
    try:
        task_name = classification.get("task_name") or raw_text
        deadline_days = classification.get("deadline_days")
        ctx = context_override or classification.get("context", "🏠 Personal")
        recurring, repeat_day = _recurring_fields_from_classification(classification)
        target_date = next_repeat_day_date(recurring, repeat_day)
        if target_date is not None:
            computed_days = (target_date - local_today()).days
            if deadline_days is None or (deadline_days <= 0 and computed_days > 0):
                deadline_days = computed_days
        if deadline_override is not None:
            deadline_days = deadline_override
        explicit_deadline = infer_deadline_override(raw_text)
        if explicit_deadline is not None:
            deadline_days = explicit_deadline
        horizon_label = deadline_days_to_label(deadline_days)

        if not force_create:
            dup = notion_tasks.find_duplicate_active_task(notion, NOTION_DB_ID, task_name)
            if dup:
                return {"status": "duplicate", "name": task_name, "duplicate": dup}

        if recurring != "None":
            page_id, first_deadline = _create_recurring_task_template_and_first_instance(
                task_name,
                ctx,
                recurring,
                repeat_day,
            )
            capture_map[page_id] = {"page_id": page_id, "name": task_name}
            return {
                "status": "captured",
                "name": task_name,
                "horizon_label": first_deadline.strftime("%b %d"),
                "context": ctx,
                "recurring": recurring,
                "page_id": page_id,
            }

        page_id = notion_tasks.create_task(
            notion,
            NOTION_DB_ID,
            task_name,
            deadline_days,
            ctx,
            recurring=recurring,
            repeat_day=repeat_day,
        )
        capture_map[page_id] = {"page_id": page_id, "name": task_name}
        return {
            "status": "captured",
            "name": task_name,
            "horizon_label": horizon_label,
            "context": ctx,
            "recurring": recurring,
            "page_id": page_id,
        }
    except Exception as e:
        log.error("Task creation error for '%s': %s", raw_text, e)
        return {"status": "error", "name": raw_text, "error": str(e)}

async def _confirm_multi_task_batch(
    message,
    thinking_msg,
    task_texts: list[str],
    classifications: list[dict],
    context_override: str | None,
    deadline_override: int | None,
    force_create: bool,
) -> None:
    """Show a confirmation UI for a mixed-confidence task batch."""
    confirmation_lines = []
    for i, (text, classification) in enumerate(zip(task_texts, classifications), start=1):
        confidence = classification.get("confidence", "low")
        icon = "✅" if confidence == "high" else "⚠️"
        task_name = classification.get("task_name") or text[:40]
        confirmation_lines.append(f"{icon} {i}. {task_name}")

    batch_id = str(uuid.uuid4())[:8]
    pending_batches[batch_id] = {
        "task_texts": task_texts,
        "classifications": classifications,
        "context_override": context_override,
        "deadline_override": deadline_override,
        "force_create": force_create,
        "message_id": message.message_id,
        "chat_id": message.chat_id,
        "confirmation_msg_id": thinking_msg.message_id,
        "timeout": asyncio.get_running_loop().time() + 300,
        "created_at": time.time(),
    }

    await thinking_msg.edit_text(
        "I found mixed-confidence tasks. Confirm?\n\n" + "\n".join(confirmation_lines),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm All", callback_data=f"confirm_batch:{batch_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_batch:{batch_id}"),
        ]]),
    )

async def create_task_batch(message, raw_text: str, task_texts: list[str], force_create: bool = False) -> None:
    """Classify and create a task batch without a separate confirmation prompt."""
    thinking = await message.reply_text(f"🧠 Classifying {len(task_texts)} tasks...")
    try:
        overrides = infer_batch_overrides(raw_text)
        classifications = await _classify_task_texts(task_texts)
        results = await asyncio.gather(*[
            _create_task_from_classification(
                task_text,
                classifications[i],
                overrides.get("context"),
                overrides.get("deadline_days"),
                force_create,
            )
            for i, task_text in enumerate(task_texts)
        ])
    except Exception as e:
        log.error("Batch task creation error: %s", e)
        await thinking.edit_text("⚠️ Couldn't classify. Try rephrasing?")
        return
    await thinking.edit_text(fmt.format_batch_summary(list(results)), parse_mode="Markdown")

async def create_or_prompt_task(message, raw_text: str, force_create: bool = False) -> None:
    """
    Handle task capture with smart confirmation.

    Single tasks keep the existing preview behavior. Explicit multi-task batches
    are classified up front: all high-confidence batches are created immediately,
    while mixed-confidence batches ask for confirmation before writing.
    """
    task_texts = split_tasks(raw_text)
    is_multi = len(task_texts) > 1
    thinking = await message.reply_text(
        f"🧠 Classifying {len(task_texts)} tasks..." if is_multi else "🧠 Classifying..."
    )

    if is_multi:
        try:
            overrides = infer_batch_overrides(raw_text)
            context_override = overrides.get("context")
            deadline_override = overrides.get("deadline_days")
            classifications = await _classify_task_texts(task_texts)
        except Exception as e:
            log.error("Claude classification error: %s", e)
            await thinking.edit_text("⚠️ Couldn't classify. Try rephrasing?")
            return

        confidences = [classification.get("confidence", "low") for classification in classifications]
        if all(confidence == "high" for confidence in confidences):
            results = await asyncio.gather(*[
                _create_task_from_classification(
                    task_text,
                    classifications[i],
                    context_override,
                    deadline_override,
                    force_create,
                )
                for i, task_text in enumerate(task_texts)
            ])
            await thinking.edit_text(fmt.format_batch_summary(list(results)), parse_mode="Markdown")
        else:
            await _confirm_multi_task_batch(
                message,
                thinking,
                task_texts,
                classifications,
                context_override,
                deadline_override,
                force_create,
            )
        return

    try:
        habit_names = list(habit_cache.keys())
        today = local_today()
        result        = ai_classify.classify_message(claude, CLAUDE_MODEL, raw_text, habit_names, bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), today)
        task_name     = result.get("task_name") or raw_text
        deadline_days = result.get("deadline_days")
        ctx           = result.get("context", "🏠 Personal")
        confidence    = result.get("confidence", "low")
        recurring, repeat_day = _recurring_fields_from_classification(result)
        target_date = next_repeat_day_date(recurring, repeat_day)
        if target_date is not None:
            computed_days = (target_date - local_today()).days
            if deadline_days is None or (deadline_days <= 0 and computed_days > 0):
                deadline_days = computed_days
        explicit_deadline = infer_deadline_override(raw_text)
        if explicit_deadline is not None:
            deadline_days = explicit_deadline
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error("Claude error: %s", e)
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

    if confidence != "high":
        preview_map[thinking.message_id] = {
            "task_name": task_name,
            "deadline_days": deadline_days,
            "context": ctx,
            "recurring": recurring,
            "repeat_day": repeat_day,
        }
        await thinking.edit_text(
            "📋 *Preview* (confirm to save)\n\n"
            f"*Task:* {task_name}\n"
            f"*Deadline:* {horizon_label}\n"
            f"*Context:* {ctx}\n"
            f"*Recurring:* {recurring}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💾 Save", callback_data=f"save_task:{thinking.message_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_task:{thinking.message_id}"),
            ]]),
        )
        return

    try:
        if recurring != "None":
            page_id, first_deadline = _create_recurring_task_template_and_first_instance(
                task_name,
                ctx,
                recurring,
                repeat_day,
            )
            await thinking.edit_text(
                f"✅ Recurring task created: {task_name}\n"
                f"📅 Pattern: {recurring}\n"
                f"🗓️ First due: {first_deadline.strftime('%b %d')}",
                parse_mode="Markdown",
            )
        else:
            page_id = notion_tasks.create_task(notion, NOTION_DB_ID, task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
            await thinking.edit_text(
                f"✅ Captured!\n\n📝 {task_name}\n🕐 {horizon_label}  {ctx}{recur_tag}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        capture_map[thinking.message_id] = {"page_id": page_id, "name": task_name}
    except Exception as e:
        log.error("Notion error: %s", e)
        await thinking.edit_text("⚠️ Classified but couldn't write to Notion.")

async def open_done_picker(message) -> None:
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    if not tasks:
        await message.reply_text("✅ Nothing open in Today or overdue right now.")
        return
    key = str(STATE.done_picker_counter); STATE.done_picker_counter += 1
    done_picker_map[key] = tasks
    await message.reply_text("Which task should be marked done?", reply_markup=kb.done_picker_keyboard(key, done_picker_map, page=0))

async def open_habit_picker(message) -> None:
    pending_habits = [
        h for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
        if not already_logged_today(h["page_id"])
    ]
    if not pending_habits:
        await message.reply_text("✅ No habits left to log today.")
        return
    sent = await message.reply_text(
        "🏃 *Which habit did you complete?*",
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(pending_habits, "manual", selected=set()),
    )
    _store_habit_selection_session(sent.message_id, pending_habits)

async def cmd_refresh(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    config = None
    try:
        slots = digest_helpers.load_digest_slots(rows=notion_query_all(NOTION_DIGEST_SELECTOR_DB), logger=log)
        now_dt = datetime.now(TZ)
        config = _manual_digest_config_now_fn(slots, now_dt=now_dt, is_weekday=now_dt.weekday() < 5)
    except Exception as e:
        log.warning("Manual digest fallback config failed: %s", e)

    include_habits = True if config is None else bool(config.get("include_habits", True))
    await send_daily_digest(message.get_bot(), include_habits=include_habits, config=config)

def _manual_digest_config_now(slots: list[dict], now_dt: datetime | None = None) -> dict | None:
    now_dt = now_dt or datetime.now(TZ)
    return _manual_digest_config_now_fn(slots, now_dt=now_dt, is_weekday=now_dt.weekday() < 5)

async def cmd_todo(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
    if not tasks:
        await message.reply_text("✅ Nothing open in Today or overdue right now.")
        return
    key = str(STATE.todo_picker_counter)
    STATE.todo_picker_counter += 1
    todo_picker_map[key] = tasks
    await message.reply_text(
        "✅ *What did you get done?*",
        parse_mode="Markdown",
        reply_markup=kb.todo_picker_keyboard(key, todo_picker_map, fmt.context_emoji),
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
    readiness_logged = await check_readiness_logged_today(notion, NOTION_DAILY_READINESS_DB)
    await message.reply_text(
        "💪 *CrossFit*\n\nWhat would you like to do?",
        parse_mode="Markdown",
        reply_markup=crossfit_submenu_keyboard(readiness_logged),
    )

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
    if not wx._loc.location:
        await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
        return
    try:
        await message.reply_text(await handle_weather(wx._loc.location), parse_mode="Markdown")
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

async def handle_v10_callback(q, parts):
    from second_brain.routers import handle_v10_callback as _impl
    return await _impl(q, parts)

def _crossfit_config(**extra) -> dict:
    cfg = {
        "NOTION_WORKOUT_LOG_DB": NOTION_WORKOUT_LOG_DB,
        "NOTION_WOD_LOG_DB": NOTION_WOD_LOG_DB,
        "NOTION_MOVEMENTS_DB": NOTION_MOVEMENTS_DB,
        "NOTION_WORKOUT_PROGRAM_DB": NOTION_WORKOUT_PROGRAM_DB,
        "NOTION_WORKOUT_DAYS_DB": NOTION_WORKOUT_DAYS_DB,
        "NOTION_CYCLES_DB": os.getenv("NOTION_CYCLES_DB", ""),
        "NOTION_PROGRESSIONS_DB": NOTION_PROGRESSIONS_DB,
        "NOTION_DAILY_READINESS_DB": NOTION_DAILY_READINESS_DB,
        "CLAUDE_PARSE_MAX_TOKENS": CLAUDE_PARSE_MAX_TOKENS,
    }
    cfg.update(extra)
    return cfg

async def cmd_cf_reload_movements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    del context
    if not NOTION_MOVEMENTS_DB:
        await update.effective_message.reply_text("⚠️ NOTION_MOVEMENTS_DB is not configured")
        return
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: reload_movement_library(notion, NOTION_MOVEMENTS_DB)
    )
    await update.effective_message.reply_text(f"✅ Movement library reloaded ({len(MOVEMENTS_CACHE)} entries)")

COMMAND_DISPATCH: dict[str, Callable] = {
    "digest": cmd_refresh,
    "📜digest": cmd_refresh,
    "📜 digest": cmd_refresh,
    "refresh": cmd_refresh,
    "🔄 refresh": cmd_refresh,
    "✅ to do": cmd_todo,
    "✅to do": cmd_todo,
    "✅ todo": cmd_todo,
    "✅todo": cmd_todo,
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

# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ══════════════════════════════════════════════════════════════════════════════

async def run_recurring_check(bot) -> dict:
    """
    Daily morning job — two responsibilities:
    1. Spawn recurring task instances from To-Do DB templates
    2. Record weekly habit streaks (Mondays only)
    Habit cache refresh is handled separately by digest_schedule_refresh.
    """
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
    if _is_muted():
        log.info("Recurring check skipped (muted)")
        return {"action": "skipped", "reason": "muted"}
    spawned = notion_tasks.process_recurring_tasks(notion, NOTION_DB_ID)
    log.info("Recurring check: %d task(s) spawned", spawned)
    return {"action": "spawned", "tasks_spawned": spawned}

async def generate_next_recurring_instances(bot) -> None:
    """Scan completed recurring instances and generate their next occurrences."""
    try:
        results = notion.databases.query(
            database_id=NOTION_DB_ID,
            filter={
                "and": [
                    {"property": "Done", "checkbox": {"equals": True}},
                    {"property": "Recurring Parent ID", "rich_text": {"is_not_empty": True}},
                    {"property": "Last Generated", "date": {"is_empty": True}},
                ]
            },
            page_size=10,
        )

        spawned = 0
        for page in results.get("results", []):
            try:
                parent_id_prop = page["properties"].get("Recurring Parent ID", {}).get("rich_text", [])
                if not parent_id_prop:
                    continue
                parent_id = parent_id_prop[0]["text"]["content"]

                parent = notion.pages.retrieve(page_id=parent_id)
                p = parent["properties"]
                template = {
                    "page_id": parent_id,
                    "name": notion_tasks._get_prop(p, "Name", "title") or "Untitled",
                    "context": notion_tasks._get_prop(p, "Context", "select") or "🏠 Personal",
                    "recurring": notion_tasks._get_prop(p, "Recurring", "select") or "None",
                    "repeat_day": notion_tasks._get_prop(p, "Repeat Day", "select"),
                    "deadline": notion_tasks._get_prop(p, "Deadline", "date"),
                    "recurrence_pattern": notion_tasks._get_prop(p, "Recurrence Pattern", "rich_text"),
                }

                template_source = notion_tasks._get_prop(p, "Source", "select") or "✏️ Manual"
                completed_deadline = notion_tasks._get_prop(page["properties"], "Deadline", "date")
                ref_date = notion_tasks._parse_deadline(completed_deadline) or notion_tasks.local_today()
                next_deadline = notion_tasks.calculate_next_deadline(template, from_date=ref_date)

                notion_tasks.spawn_recurring_instance(
                    notion,
                    NOTION_DB_ID,
                    template,
                    next_deadline=next_deadline,
                    source=template_source,
                )
                notion_tasks.set_last_generated(notion, page["id"], notion_tasks.local_today())

                spawned += 1
                log.info("Generated next recurring instance for parent %s, due %s", parent_id, next_deadline)
            except Exception as e:
                page_id = page.get("id")
                log.error("Failed to generate next instance for page %s: %s", page_id, e)
                continue

        if spawned > 0:
            log.info("Recurring batch generation: %s instance(s) spawned", spawned)
    except Exception as e:
        log.error("Recurring batch generation job failed: %s", e)


async def send_evening_checkin(bot) -> None:
    """Evening habit check-in with time display and frequency status."""
    now_str = datetime.now(TZ).strftime("%H:%M")
    evening_habits = digest_helpers.pending_habits_for_digest(habit_cache=habit_cache, time_str=now_str, already_logged_today=already_logged_today, is_on_pace=is_on_pace)
    if not evening_habits:
        return

    habit_text = "🌙 *Evening check-in* — did you do these today?\n\n"
    for h in evening_habits[:5]:
        frequency = h.get("frequency") or h.get("freq_per_week")
        completion_count = h.get("completion_count")
        freq_tag = f" _{completion_count}/{frequency}_" if frequency and completion_count is not None else ""
        time_label = h.get("time_str") or h.get("show_after") or "—"
        habit_text += f"⏰ {time_label} — {h['name']}{freq_tag}\n"
    if len(evening_habits) > 5:
        habit_text += f"\n_+{len(evening_habits) - 5} more_"

    sent = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=habit_text.rstrip(),
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(evening_habits, "evening", selected=set()),
    )
    _store_habit_selection_session(sent.message_id, evening_habits)
    log.info("Evening check-in sent — %d habits", len(evening_habits))

async def send_daily_habits_list(bot) -> None:
    """Fetch all active habits for today and send as clickable buttons."""
    habits = digest_helpers.pending_habits_for_digest(habit_cache=habit_cache, time_str=None, already_logged_today=already_logged_today, is_on_pace=is_on_pace)
    if not habits:
        await bot.send_message(chat_id=MY_CHAT_ID, text="🎯 No habits for today.")
        return

    sent = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text="🎯 *Daily habits* — tap habits to select, then tap Done:",
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(habits, "morning", selected=set()),
    )
    _store_habit_selection_session(sent.message_id, habits)
    log.info("Habits list sent — %s available habits", len(habits))

async def run_asana_sync(bot) -> dict:
    """
    Bi-directional Asana <-> Notion reconcile.
    Offloads blocking I/O to thread pool so Telegram event loop stays responsive.
    """
    if not ASANA_PAT:
        return {"ok": True, "action": "disabled"}  # Sync disabled — bot still works without Asana
    loop = asyncio.get_event_loop()
    sync_status["asana"]["last_run"] = utc_now_iso()
    try:
        stats = await loop.run_in_executor(
            None,
            lambda: reconcile(
                notion=notion,
                notion_db_id=NOTION_DB_ID,
                asana_token=ASANA_PAT,
                asana_project_gid=ASANA_PROJECT_GID,
                asana_workspace_gid=ASANA_WORKSPACE_GID,
                source_mode=ASANA_SYNC_SOURCE,
                archive_orphans=ASANA_ARCHIVE_ORPHANS,
            ),
        )
        if any(v for k, v in stats.items() if k != "skipped"):
            log.info("Asana sync: %s", stats)
        sync_status["asana"]["ok"] = True
        sync_status["asana"]["error"] = None
        sync_status["asana"]["stats"] = stats
        return {**stats, "action": "synced"}
    except AsanaSyncError as e:
        log.error("Asana sync config error: %s", e)
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)
        return {"ok": False, "action": "error", "reason": str(e)}
    except Exception as e:
        log.exception("Asana sync failed: %s", e)
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)
        return {"ok": False, "action": "error", "reason": str(e)}

async def run_cinema_sync(bot, *, force: bool = False) -> dict[str, int | str]:
    from second_brain.cinema.sync import run_cinema_sync as _impl
    result = await _impl(notion, bot, cinema_log_db=NOTION_CINEMA_LOG_DB, chat_id=MY_CHAT_ID, force=force)
    if result.get("action") == "disabled":
        return result
    sync_status["cinema"]["last_run"] = utc_now_iso()
    sync_status["cinema"]["ok"] = result.get("action") != "error"
    sync_status["cinema"]["error"] = result.get("reason") if result.get("action") == "error" else None
    sync_status["cinema"]["stats"] = result
    return result

# ══════════════════════════════════════════════════════════════════════════════
# /habits-data JSON ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

async def habits_data_handler(request: web.Request) -> web.Response:
    from second_brain.healthtrack.routes import habits_data_handler as _impl
    return await _impl(
        request,
        notion=notion,
        habit_cache=habit_cache,
        log_db=NOTION_LOG_DB,
        habit_db=NOTION_HABIT_DB,
        streak_db=NOTION_STREAK_DB,
        tz=TZ,
        weeks_history=WEEKS_HISTORY,
        query_all_fn=notion_query_all,
        extract_date_fn=extract_date_only,
        datetime_cls=datetime,
    )

async def log_habit_http_handler(request: web.Request) -> web.Response:
    from second_brain.healthtrack.routes import log_habit_http_handler as _impl
    return await _impl(
        request,
        notion=notion,
        habit_cache=habit_cache,
        log_db=NOTION_LOG_DB,
        habit_db=NOTION_HABIT_DB,
        streak_db=NOTION_STREAK_DB,
        tz=TZ,
        weeks_history=WEEKS_HISTORY,
    )

async def _persist_steps_sync_to_env_db(notion_client, env_db_id: str) -> None:
    """Update HEALTH_STEPS_THRESHOLD row in Notion ENV DB with current sync date."""
    if not env_db_id:
        log.warning("steps: ENV DB ID is not configured; skipping sync timestamp update")
        return

    try:
        try:
            results = notion_client.databases.query(
                database_id=env_db_id,
                filter={
                    "property": "Name",
                    "rich_text": {"equals": "HEALTH_STEPS_THRESHOLD"},
                },
            )
        except Exception:
            log.debug("steps: rich_text filter unsupported, retrying with title filter", exc_info=True)
            results = notion_client.databases.query(
                database_id=env_db_id,
                filter={
                    "property": "Name",
                    "title": {"equals": "HEALTH_STEPS_THRESHOLD"},
                },
            )

        if not results.get("results"):
            log.warning("steps: HEALTH_STEPS_THRESHOLD not found in ENV DB")
            return

        page_id = results["results"][0]["id"]
        today = datetime.now(TZ).strftime("%Y-%m-%d")

        notion_client.pages.update(
            page_id=page_id,
            properties={
                "Last Sync Time": {
                    "date": {"start": today},
                },
            },
        )
        log.info("steps: updated HEALTH_STEPS_THRESHOLD Last Sync Time to %s", today)

    except Exception as e:
        log.error("steps: error updating ENV DB sync timestamp: %s", e)

async def _record_steps_sync_result(result: dict) -> None:
    """Handle steps sync completion: update in-memory status and ENV DB."""
    if not result:
        return

    sync_status["steps"].update(
        {
            "last_run": result.get("timestamp") or utc_now_iso(),
            "ok": result.get("action") != "error",
            "error": result.get("reason") if result.get("action") == "error" else None,
            "stats": result,
        }
    )

    if result.get("action") != "error":
        asyncio.create_task(_persist_steps_sync_to_env_db(notion, NOTION_ENV_DB))

async def start_http_server() -> None:
    app    = web.Application()
    app.router.add_get(
        "/api/health-dashboard",
        create_health_dashboard_handler(
            notion=notion,
            health_metrics_db_id=NOTION_HEALTH_METRICS_DB,
            habit_log_db_id=NOTION_LOG_DB,
            tz=TZ,
        ),
    )
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    register_health_routes(
        app,
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        env_db_id=NOTION_ENV_DB,
        tz=TZ,
        bot_getter=lambda: _app_bot,
        chat_id=MY_CHAT_ID,
        on_sync_result=_record_steps_sync_result,
        health_metrics_db_id=NOTION_HEALTH_METRICS_DB,
        habit_cache=habit_cache,
        streak_db_id=NOTION_STREAK_DB,
        weeks_history=WEEKS_HISTORY,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    log.info("HTTP server started on port %s", HTTP_PORT)

# ══════════════════════════════════════════════════════════════════════════════
# STARTUP HELPERS — schema validation + alert
# ══════════════════════════════════════════════════════════════════════════════

async def _try_send_telegram(bot, text: str) -> None:
    """Best-effort Telegram alert. Never raises."""
    try:
        kwargs = {
            "chat_id": ALERT_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }
        if ALERT_CHAT_ID is None:
            log.error("Could not send operational alert via Telegram: ALERT_CHANNEL_ID is not configured")
            return
        if ALERT_THREAD_ID is not None:
            kwargs["message_thread_id"] = ALERT_THREAD_ID
        await bot.send_message(**kwargs)
    except Exception as e:
        log.error("Could not send operational alert via Telegram: %s", e)

def v10_feature_flags() -> str:
    flags = [
        f"watchlist={'ON' if NOTION_WATCHLIST_DB else 'OFF'}",
        f"wantslist={'ON' if NOTION_WANTSLIST_V2_DB else 'OFF'}",
        f"photo={'ON' if NOTION_PHOTO_DB else 'OFF'}",
        f"tmdb={'ON' if TMDB_API_KEY else 'OFF (title-only)'}",
        f"notes={'ON' if NOTION_NOTES_DB else 'OFF'}",
        f"weather={'ON' if OPENWEATHER_KEY else 'OFF'}",
        f"mute={'ON' if _is_muted() else 'OFF'}",
    ]
    return "  ".join(flags)

def startup_notion_health_check() -> None:
    """Fail fast for core Notion DBs, but don't block startup for optional features."""
    dbs = {
        "NOTION_DB_ID": (NOTION_DB_ID, True),
        "NOTION_HABIT_DB": (NOTION_HABIT_DB, True),
        "NOTION_LOG_DB": (NOTION_LOG_DB, True),
        "NOTION_HEALTH_METRICS_DB": (NOTION_HEALTH_METRICS_DB, True),
        "NOTION_CINEMA_LOG_DB": (NOTION_CINEMA_LOG_DB, False),
        "NOTION_PERFORMANCE_LOG_DB": (NOTION_PERFORMANCE_LOG_DB, False),
        "NOTION_SPORTS_LOG_DB": (NOTION_SPORTS_LOG_DB, False),
        "NOTION_FAVE_DB": (NOTION_FAVE_DB, False),
        "NOTION_NOTES_DB": (NOTION_NOTES_DB, True),
        "NOTION_DIGEST_SELECTOR_DB": (NOTION_DIGEST_SELECTOR_DB, True),
        "NOTION_UTILITY_SCHEDULER_DB": (NOTION_UTILITY_SCHEDULER_DB, False),
        "NOTION_WATCHLIST_DB": (NOTION_WATCHLIST_DB, False),
    }
    for label, (db_id, required) in dbs.items():
        if not db_id:
            if required:
                raise RuntimeError(f"Startup health check failed: required Notion DB env {label} is missing")
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
    from second_brain.crossfit.weekly_program import process_pending_programmes as _impl
    await _impl(notion, bot, workout_program_db=NOTION_WORKOUT_PROGRAM_DB, chat_id=MY_CHAT_ID)

async def _run_steps_sync_check_dispatch(bot) -> dict:
    result = await check_and_create_steps_entry(
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        habit_name=health_config.STEPS_HABIT_NAME,
        tz=TZ,
        bot=bot,
        chat_id=MY_CHAT_ID,
    )
    sync_status["steps"]["last_run"] = utc_now_iso()
    sync_status["steps"]["ok"] = bool(result.get("ok"))
    sync_status["steps"]["error"] = None if result.get("ok") else result.get("reason")
    sync_status["steps"]["stats"] = result
    return result

async def _run_steps_final_stamp_dispatch(bot) -> dict:
    result = await handle_steps_final_stamp(
        notion=notion,
        habit_db_id=NOTION_HABIT_DB,
        log_db_id=NOTION_LOG_DB,
        env_db_id=NOTION_ENV_DB,
        habit_name=health_config.STEPS_HABIT_NAME,
        threshold=health_config.STEPS_THRESHOLD,
        source_label=health_config.STEPS_SOURCE_LABEL,
        tz=TZ,
        bot=bot,
        chat_id=MY_CHAT_ID,
    )
    sync_status["steps"]["last_run"] = utc_now_iso()
    sync_status["steps"]["ok"] = True
    sync_status["steps"]["error"] = None
    sync_status["steps"]["stats"] = result
    return result

UTILITY_JOB_DISPATCH: dict[str, Callable] = {}

def _utility_async_handler(job_key: str, coro_factory: Callable):
    @track_job_execution(job_key)
    async def _utility_dispatch_handler() -> object:
        return await coro_factory()

    return _utility_dispatch_handler

def _tracked_utility_manager_handler(job_key: str, coro_factory: Callable):
    @track_job_execution(job_key)
    async def _utility_manager_dispatch_handler(bot) -> object:
        return await coro_factory(bot)

    return _utility_manager_dispatch_handler

def _build_utility_job_dispatch(bot) -> dict[str, Callable]:
    """
    Maps Utility Scheduler job keys to their async handler functions.
    Add new job keys here as new features are added.
    Each value must be an async callable that accepts no arguments (bot is
    captured via closure).
    """
    dispatch = {
        "digest_schedule_rebuild": _utility_async_handler("digest_schedule_rebuild", lambda: rebuild_digest_schedule_job(bot, digest_helpers._scheduler)),
        "digest_schedule_refresh": _utility_async_handler("digest_schedule_refresh", lambda: refresh_digest_schedule_job(bot, digest_helpers._scheduler)),
        "weather_cache_refresh": _utility_async_handler("weather_cache_refresh", lambda: wx.fetch_weather_cache(bot)),
        "trip_weather_refresh": _utility_async_handler("trip_weather_refresh", lambda: handle_trip_weather_refresh(bot)),
        "process_pending_programmes": _utility_async_handler("process_pending_programmes", lambda: process_pending_programmes(bot)),
        "cinema_sync": _utility_async_handler("cinema_sync", lambda: run_cinema_sync(bot)),
        "asana_sync": _utility_async_handler("asana_sync", lambda: run_asana_sync(bot)),
        "steps_final_stamp": _utility_async_handler("steps_final_stamp", lambda: _run_steps_final_stamp_dispatch(bot)),
        "steps_sync_check": _utility_async_handler("steps_sync_check", lambda: _run_steps_sync_check_dispatch(bot)),
        "daily_log_generate": _utility_async_handler("daily_log_generate", lambda: generate_daily_log(bot)),
        "run_recurring_check": _utility_async_handler("run_recurring_check", lambda: run_recurring_check(bot)),
    }
    UTILITY_JOB_DISPATCH.clear()
    UTILITY_JOB_DISPATCH.update(dispatch)
    return dispatch

def load_and_register_utility_jobs(scheduler, bot) -> int:
    """
    Reads NOTION_UTILITY_SCHEDULER_DB and registers all active jobs with
    APScheduler. Returns count of jobs registered.
    Unknown job keys are logged as warnings (not errors) to avoid blocking startup.
    Writes Last Status = 'ok' / 'unknown_job' and Last Loaded At back to Notion.
    """
    if not NOTION_UTILITY_SCHEDULER_DB:
        log.warning("NOTION_UTILITY_SCHEDULER_DB not set — utility scheduler disabled")
        return 0

    dispatch = _build_utility_job_dispatch(bot)
    rows = notion_query_all(NOTION_UTILITY_SCHEDULER_DB)
    registered = 0

    for row in rows:
        props = row.get("properties", {})
        page_id = row["id"]

        active = bool(props.get("Active", {}).get("checkbox", False))
        if not active:
            continue

        job_key_parts = props.get("Job Key", {}).get("title", [])
        job_key = "".join(p.get("plain_text", "") for p in job_key_parts).strip()
        if not job_key:
            continue

        if job_key not in dispatch:
            log.warning("Utility Scheduler: unknown job key '%s' — skipping", job_key)
            _update_utility_job_status(notion, page_id, "unknown_job", None)
            continue

        handler = dispatch[job_key]
        trigger_type = (props.get("Trigger Type", {}).get("select") or {}).get("name", "").lower()

        try:
            if trigger_type == "interval":
                interval_seconds = props.get("Interval Seconds", {}).get("number")
                interval_minutes = props.get("Interval Minutes", {}).get("number")
                interval_hours = props.get("Interval Hours", {}).get("number")
                max_instances = int((props.get("Max Instances", {}).get("number") or 1))
                misfire_grace = int((props.get("Misfire Grace Seconds", {}).get("number") or 300))
                coalesce = bool(props.get("Coalesce", {}).get("checkbox", True))
                kwargs = dict(max_instances=max_instances, misfire_grace_time=misfire_grace, coalesce=coalesce)
                if interval_seconds:
                    scheduler.add_job(handler, "interval", seconds=int(interval_seconds), id=job_key, replace_existing=True, **kwargs)
                elif interval_minutes:
                    scheduler.add_job(handler, "interval", minutes=int(interval_minutes), id=job_key, replace_existing=True, **kwargs)
                elif interval_hours:
                    scheduler.add_job(handler, "interval", hours=int(interval_hours), id=job_key, replace_existing=True, **kwargs)
                else:
                    log.warning("Utility Scheduler: interval job '%s' has no interval value", job_key)
                    continue

            elif trigger_type == "cron":
                cron_day = (props.get("Cron Day Of Week", {}).get("select") or {}).get("name") or None
                cron_hour = props.get("Cron Hour", {}).get("number")
                cron_minute = props.get("Cron Minute", {}).get("number")
                max_instances = int((props.get("Max Instances", {}).get("number") or 1))
                misfire_grace = int((props.get("Misfire Grace Seconds", {}).get("number") or 300))
                coalesce = bool(props.get("Coalesce", {}).get("checkbox", True))
                kwargs = dict(max_instances=max_instances, misfire_grace_time=misfire_grace, coalesce=coalesce)
                cron_kwargs = {}
                if cron_day:
                    cron_kwargs["day_of_week"] = cron_day
                if cron_hour is not None:
                    cron_kwargs["hour"] = int(cron_hour)
                if cron_minute is not None:
                    cron_kwargs["minute"] = int(cron_minute)
                scheduler.add_job(handler, "cron", id=job_key, replace_existing=True, **cron_kwargs, **kwargs)

            else:
                log.warning("Utility Scheduler: unknown trigger type '%s' for job '%s'", trigger_type, job_key)
                continue

            _update_utility_job_status(notion, page_id, "ok", datetime.now(TZ).isoformat())
            registered += 1
            log.info("Utility Scheduler: registered job '%s' (%s)", job_key, trigger_type)

        except Exception as e:
            log.error("Utility Scheduler: failed to register job '%s': %s", job_key, e)
            _update_utility_job_status(notion, page_id, f"error: {str(e)[:80]}", None)

    log.info("Utility Scheduler: %d jobs registered", registered)
    return registered

def _update_utility_job_status(notion, page_id: str, status: str, loaded_at: str | None) -> None:
    try:
        props = {"Last Status": {"select": {"name": status}}}
        if loaded_at:
            props["Last Loaded At"] = {"date": {"start": loaded_at}}
        notion.pages.update(page_id=page_id, properties=props)
    except Exception as e:
        log.warning("Could not update utility job status for %s: %s", page_id, e)

# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

def _scheduler_event_listener(event) -> None:
    if getattr(event, "exception", None):
        alert_scheduler_event(getattr(event, "job_id", "unknown"), "error", str(event.exception))
    else:
        alert_scheduler_event(getattr(event, "job_id", "unknown"), "missed")

async def post_init(app: Application) -> None:
    global _scheduler, _steps_title_migration_ran, UV_THRESHOLD, WEEKS_HISTORY, TZ, rule_engine
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

    rule_engine = RuleEngine(notion)
    if not rule_engine.startup():
        log.error("Rule engine startup failed. Bot may not execute cross-DB rules.")
    else:
        log.info("Rule engine initialized successfully")

    try:
        if NOTION_MOVEMENTS_DB:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: reload_movement_library(notion, NOTION_MOVEMENTS_DB)
            )
            log.info("Movement library loaded: %d entries", len(MOVEMENTS_CACHE))
    except Exception as e:
        log.warning("CrossFit movement cache load failed at startup: %s", e)

    env_config = load_notion_env_config()

    if "UV_THRESHOLD" in env_config:
        try:
            UV_THRESHOLD = float(env_config["UV_THRESHOLD"])
            log.info("UV_THRESHOLD loaded from Notion ENV: %s", UV_THRESHOLD)
        except ValueError:
            log.warning("Invalid UV_THRESHOLD in Notion ENV: %s", env_config["UV_THRESHOLD"])

    if "WEEKS_HISTORY" in env_config:
        try:
            WEEKS_HISTORY = int(env_config["WEEKS_HISTORY"])
            log.info("WEEKS_HISTORY loaded from Notion ENV: %s", WEEKS_HISTORY)
        except ValueError:
            log.warning("Invalid WEEKS_HISTORY in Notion ENV: %s", env_config["WEEKS_HISTORY"])

    if "TIMEZONE" in env_config:
        try:
            TZ = ZoneInfo(env_config["TIMEZONE"])
            log.info("TIMEZONE loaded from Notion ENV: %s", TZ)
        except Exception:
            log.warning("Invalid TIMEZONE in Notion ENV: %s", env_config["TIMEZONE"])

    _load_mute_state()
    wx.load_location_state()  # load from local JSON cache first (fast)
    if not wx.load_notion_env_location():  # try Notion (authoritative)
        # Notion had no location — geocode from env var or history
        if OPENWEATHER_KEY and (wx._loc.lat is None or wx._loc.lon is None):
            if not wx.set_location_smart(wx._loc.location, claude):
                wx.recover_location_from_history(claude)
    else:
        # Notion loaded successfully — sync back to local JSON cache
        wx.save_location_state(wx._loc.location)
    cleanup_old_habit_selections()
    notion_habits.load_habit_cache(notion=notion, notion_habit_db=NOTION_HABIT_DB); _refresh_habit_cache_refs()
    # Load steps config from Notion ENV DB
    health_config.load_steps_threshold_from_notion_env(notion=notion, notion_env_db=NOTION_ENV_DB)
    load_dashboard_steps_threshold(notion=notion, env_db_id=NOTION_ENV_DB)
    health_config.load_steps_config_from_notion_env(notion=notion, notion_env_db=NOTION_ENV_DB)
    global _app_bot
    _app_bot = app.bot
    await start_http_server()
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_listener(_scheduler_event_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
    _scheduler = scheduler
    digest_helpers._scheduler = scheduler
    digest_helpers._notion = notion
    digest_helpers._on_rebuild_fn = cleanup_old_habit_selections
    digest_helpers._store_habit_session_fn = _store_habit_selection_session
    digest_helpers._refresh_cache_fn = _refresh_habit_cache_refs
    digest_helpers._signoff_notes_fn = get_and_clear_project_signoff_notes
    digest_helpers._claude_activity_fn = get_and_clear_claude_activity
    scheduler.add_job(
        cleanup_pending_task_interactions,
        "interval",
        minutes=5,
        id="cleanup_pending_task_interactions",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        cleanup_expired_batches,
        "interval",
        seconds=60,
        id="cleanup_batch_confirmations",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    log.info("Batch cleanup scheduler started (every 60 seconds)")
    try:
        await backfill_steps_state_from_notion(
            notion=notion,
            habit_db_id=NOTION_HABIT_DB,
            log_db_id=NOTION_LOG_DB,
            env_db_id=NOTION_ENV_DB,
            habit_name=health_config.STEPS_HABIT_NAME,
            tz=TZ,
        )
        log.info("Steps state backfill complete")
    except Exception as e:
        log.warning("Steps state backfill failed (non-fatal): %s", e)
    if FEATURES.get("FEATURE_RECURRING", True):
        log.info("Recurring check is managed by Utility Scheduler when configured")
        scheduler.add_job(
            generate_next_recurring_instances,
            "interval",
            minutes=5,
            args=[app.bot],
            id="recurring_batch_generator",
            max_instances=1,
            coalesce=True,
        )
    # Register digest cron jobs only. Do not queue missed digest slots on startup;
    # restarting the bot should not send an immediate digest.
    build_digest_schedule(scheduler, app.bot)
    # ── Cinema sync — validate config before Utility Scheduler can enable it ──
    cinema_ok, cinema_problems = validate_cinema_config()
    if not cinema_ok:
        log.warning("Cinema sync disabled due to config issues:")
        for p in cinema_problems:
            log.warning("  - %s", p)
    elif CINEMA_DB_ID:
        log.info("Cinema sync config validated ✓")

    scheduler.start()

    if NOTION_UTILITY_SCHEDULER_DB:
        utility_manager = UtilitySchedulerManager(
            notion=notion,
            db_id=NOTION_UTILITY_SCHEDULER_DB,
            scheduler=scheduler,
            bot=app.bot,
            chat_id=MY_CHAT_ID,
            tz=TZ,
            reload_minutes=UTILITY_SCHEDULER_RELOAD_MINUTES,
            env_fallbacks={"asana_sync": ASANA_SYNC_INTERVAL},
        )

        from second_brain.healthtrack.scheduler import register_handlers as healthtrack_register
        from second_brain.feature_schedulers import (
            register_cinema_handlers as cinema_register,
            register_daily_log_handlers as daily_log_register,
            register_tasks_handlers as tasks_register,
            register_trips_handlers as trips_register,
            register_weather_handlers as weather_register,
        )

        healthtrack_register(utility_manager)
        cinema_register(utility_manager)
        weather_register(utility_manager)
        tasks_register(utility_manager)
        trips_register(utility_manager)
        daily_log_register(utility_manager)

        utility_manager.register_handler(
            "digest_schedule_rebuild",
            _tracked_utility_manager_handler(
                "digest_schedule_rebuild",
                lambda bot: rebuild_digest_schedule_job(bot, scheduler),
            ),
        )
        utility_manager.register_handler(
            "digest_schedule_refresh",
            _tracked_utility_manager_handler(
                "digest_schedule_refresh",
                lambda bot: refresh_digest_schedule_job(bot, scheduler),
            ),
        )
        utility_manager.register_handler(
            "run_recurring_check",
            _tracked_utility_manager_handler(
                "run_recurring_check",
                lambda bot: run_recurring_check(bot),
            ),
        )

        await utility_manager.initialize()
        log.info("Utility Scheduler Manager initialized ✓")
    else:
        log.warning("NOTION_UTILITY_SCHEDULER_DB not set — Utility Scheduler disabled")

    if NOTION_LOG_DB and NOTION_HABIT_DB and not _steps_title_migration_ran:
        _steps_title_migration_ran = True
        try:
            habit_page_id = _find_steps_habit_page_id(
                notion,
                NOTION_HABIT_DB,
                health_config.STEPS_HABIT_NAME,
            )
            if habit_page_id:
                result = await asyncio.to_thread(
                    migrate_steps_entry_titles,
                    notion,
                    NOTION_LOG_DB,
                    habit_page_id,
                )
                log.info("steps: title migration result: %s", result)
            else:
                log.warning("steps: title migration skipped — Steps habit page not found")
        except Exception as e:
            log.warning("steps: title migration error (non-blocking): %s", e)

    scheduler.add_job(
        track_job_execution("digest_schedule_refresh")(refresh_digest_schedule_job),
        "interval",
        minutes=UTILITY_SCHEDULER_RELOAD_MINUTES,
        args=[app.bot, scheduler],
        id="digest_schedule_refresh",
        replace_existing=True,
        max_instances=1,
    )

    utility_scheduler_status = "enabled" if NOTION_UTILITY_SCHEDULER_DB else "disabled"
    recurring_time = "%02d:%02d" % (_rc_h, _rc_m)
    feature_flags = v10_feature_flags()
    log.info(
        "Scheduler started ✓ TZ=%s utility_scheduler=%s recurring=%s "
        "digest_refresh=%smin v10_flags=[%s]",
        TZ,
        utility_scheduler_status,
        recurring_time,
        UTILITY_SCHEDULER_RELOAD_MINUTES,
        feature_flags,
    )
    asana_status = (
        f"ENABLED source={ASANA_SYNC_SOURCE} archive_orphans={ASANA_ARCHIVE_ORPHANS}"
        if ASANA_PAT
        else "DISABLED"
    )
    smoke_status = "SKIP"

    # Determine boot status — warn if any subsystem degraded
    boot_notes_parts = []
    boot_status = "ok"
    if "DISABLED" in asana_status:
        boot_status = "warn"
        boot_notes_parts.append(f"Asana: {asana_status}")
    if smoke_status.startswith("FAIL"):
        boot_status = "warn"
        boot_notes_parts.append(f"Smoke: {smoke_status}")

    boot_sha = _git_sha()
    await write_boot_log(
        bot=app.bot,
        version=APP_VERSION,
        sha=boot_sha,
        asana_status=f"{asana_status} smoke={smoke_status}",
        features=v10_feature_flags(),
        status=boot_status,
        notes="; ".join(boot_notes_parts),
    )
    commands = [
        BotCommand("done", "Mark task/habit done"),
        BotCommand("remind", "Show quick reminder"),
        BotCommand("r", "Alias for /remind"),
        BotCommand("notes", "Open notes capture"),
        BotCommand("weather", "Show weather snapshot"),
        BotCommand("habits", "Show habits list"),
        BotCommand("log", "Log cinema/performance/sport"),
        BotCommand("trip", "Log a work trip"),
        BotCommand("sync", "Run manual sync"),
        BotCommand("syncstatus", "Show sync status"),
        BotCommand("mute", "Pause scheduled digests"),
        BotCommand("unmute", "Resume scheduled digests"),
        BotCommand("location", "Set weather location"),
    ]
    log.info("[MAIN] Calling alert_startup with version=%s, commit=%s", APP_VERSION, boot_sha)
    alert_startup(APP_VERSION, boot_sha)
    log.info("[MAIN] alert_startup() completed")
    await app.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    await app.bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=MY_CHAT_ID))

def _next_done_picker_key() -> int:
    key = STATE.done_picker_counter
    STATE.done_picker_counter += 1
    return key

def _command_handlers() -> CommandHandlers:
    return CommandHandlers({
        "MY_CHAT_ID": MY_CHAT_ID,
        "habit_cache": habit_cache,
        "already_logged_today": already_logged_today,
        "notion_tasks": notion_tasks,
        "notion": notion,
        "NOTION_DB_ID": NOTION_DB_ID,
        "kb": kb,
        "done_picker_map": done_picker_map,
        "done_picker_keyboard": lambda key, page=0: kb.done_picker_keyboard(key, done_picker_map, page=page),
        "next_done_picker_key": _next_done_picker_key,
        "send_quick_reminder": send_quick_reminder,
    })

# ══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS — defined before main() so Python can resolve names
# ══════════════════════════════════════════════════════════════════════════════

async def handle_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _command_handlers().handle_done_command(update, context)

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
    await _command_handlers().handle_remind_command(update, context)

async def handle_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/sync — manual catch-up trigger for core sync pipelines."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    status = await update.message.reply_text("🔄 Running cinema sync…")
    try:
        cinema_stats = await run_cinema_sync(context.bot)
        await status.edit_text(
            "✅ Sync finished.\n"
            f"Cinema: scanned={cinema_stats['scanned']} updated={cinema_stats['updated']} "
            f"missing={cinema_stats['tmdb_missing']} skipped={cinema_stats['skipped']} failed={cinema_stats['failed']}"
        )
    except Exception as e:
        log.exception("Manual /sync failed: %s", e)
        await status.edit_text(_safe_user_error(e))

async def handle_sync_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/syncstatus — show latest sync telemetry for Cinema + Steps."""
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
    STATE.mute_until = None
    _save_mute_state()
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
            await update.message.reply_text(f"📍 Location updated to {wx._loc.location}.")
            wx.save_location_state(wx._loc.location)
            await update.message.reply_text(await handle_weather(wx._loc.location), parse_mode="Markdown")
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
        weather_text = append_trip_reminders_to_text(fmt.format_weather_snapshot(), within_days=2, notion=notion, notion_trips_db=NOTION_TRIPS_DB)
        await update.message.reply_text(weather_text, parse_mode="Markdown")
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

async def cmd_signoff(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return
    note = " ".join(context.args or []).strip()
    await trigger_signoff_now(update.message, note=note or None)

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
    date_result = _apply_shared_date_parse(parsed)
    if date_result and getattr(date_result, "ambiguous", False):
        key = str(STATE.entertainment_counter)
        STATE.entertainment_counter += 1
        pending_map[key] = {"type": "entertainment_log", "payload": parsed, "raw_text": f"/log {raw}"}
        await update.message.reply_text("📅 Which date did you mean?", reply_markup=kb.date_pick_keyboard("ent", key, date_result))
        return
    try:
        prompted = await ent_log._maybe_prompt_explicit_venue(notion, update.message, parsed, f"/log {raw}")
        if prompted:
            return
        await _ent_handle_log(notion, update.message, parsed, rule_engine=rule_engine)
    except Exception as e:
        log.error("Explicit /log save error: %s", e)
        await update.message.reply_text(ent_log._entertainment_save_error_text(e, parsed))

async def on_confirm_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create a pending mixed-confidence task batch after user confirmation."""
    del context
    q = update.callback_query
    if not q:
        return
    batch_id = (q.data or "").split(":", 1)[1] if ":" in (q.data or "") else None
    if not batch_id or batch_id not in pending_batches:
        await q.edit_message_text("❌ Confirmation expired. Resend your tasks.")
        return

    batch = pending_batches[batch_id]
    task_texts = batch.get("task_texts", [])
    classifications = batch.get("classifications", [])
    await q.edit_message_text(f"⏳ Creating {len(task_texts)} tasks...")

    try:
        results = await asyncio.gather(*[
            _create_task_from_classification(
                task_texts[i],
                classifications[i],
                batch.get("context_override"),
                batch.get("deadline_override"),
                bool(batch.get("force_create")),
            )
            for i in range(len(task_texts))
        ])
        await q.edit_message_text(fmt.format_batch_summary(list(results)), parse_mode="Markdown")
        log.info("Batch confirmed: %s, %d tasks processed", batch_id, len(task_texts))
    except Exception as e:
        log.error("Batch creation error: %s", e)
        await q.edit_message_text("⚠️ Couldn't create tasks — please try again.")
    finally:
        pending_batches.pop(batch_id, None)

async def on_cancel_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel a pending mixed-confidence task batch."""
    del context
    q = update.callback_query
    if not q:
        return
    batch_id = (q.data or "").split(":", 1)[1] if ":" in (q.data or "") else None
    if batch_id:
        pending_batches.pop(batch_id, None)
        log.info("Batch cancelled: %s", batch_id)
    await q.edit_message_text("❌ Cancelled. Resend your tasks if you'd like to try again.")

async def cleanup_expired_batches() -> None:
    """Remove stale pending batch confirmations and notify Telegram when possible."""
    now = asyncio.get_running_loop().time()
    expired_ids = [
        batch_id
        for batch_id, data in pending_batches.items()
        if data.get("timeout", 0) < now
    ]

    for batch_id in expired_ids:
        batch = pending_batches.get(batch_id, {})
        try:
            bot = _app_bot
            if bot:
                await bot.edit_message_text(
                    chat_id=batch.get("chat_id"),
                    message_id=batch.get("confirmation_msg_id"),
                    text="⏰ Confirmation timeout (5 min). Resend your tasks.",
                )
            log.info("Batch timeout: %s", batch_id)
        except Exception as e:
            log.warning("Failed to notify timeout for batch %s: %s", batch_id, e)
        finally:
            pending_batches.pop(batch_id, None)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN — after all handlers are defined
# ══════════════════════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled Telegram exception", exc_info=context.error)
    if not isinstance(update, Update):
        return
    chat = update.effective_chat
    if not chat:
        return
    try:
        await context.bot.send_message(
            chat_id=chat.id,
            text="❌ Something went wrong. I've logged it for review.",
        )
    except Exception:
        log.debug("error_handler: could not send user-facing error message", exc_info=True)


def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_error_handler(error_handler)
    register_core_handlers(
        app,
        handle_start_command=handle_start_command,
        handle_remind_command=handle_remind_command,
        handle_sync_command=handle_sync_command,
        handle_sync_status_command=handle_sync_status_command,
        handle_done_command=handle_done_command,
        cmd_mute=cmd_mute,
        cmd_unmute=cmd_unmute,
        cmd_weather=cmd_weather,
        cmd_notes=cmd_notes,
        cmd_location=cmd_location,
        cmd_habits=cmd_habits,
        cmd_log=cmd_log,
        handle_trip_command=handle_trip_command,
        cmd_signoff=cmd_signoff,
        handle_message_text=handle_message_text,
        handle_callback=handle_callback,
        test_alert_command=test_alert_command,
        test_channel_send=test_channel_send,
    )
    app.add_handler(CommandHandler("refreshweather", cmd_refreshweather))
    app.add_handler(CommandHandler("cf_reload_movements", cmd_cf_reload_movements))
    log.info("🤖 Second Brain bot starting (%s)...", APP_VERSION)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
