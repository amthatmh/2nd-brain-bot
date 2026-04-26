#!/usr/bin/env python3
"""
Second Brain — Telegram Bot (v9.3)
────────────────────────────────────────────────────────────────
All v9.2 features preserved plus:

v9.3 changes:
- Startup smoke test for Asana↔Notion integration boundary:
  fetch sample Asana task → create Notion row → archive Notion row.
- Deploy receipt Telegram message on boot with version, git SHA, and Asana mode.
"""

import asyncio
import os
import json
import re
import logging
import calendar
import subprocess
import urllib.parse
from datetime import date, datetime, timedelta
from collections import defaultdict
from pathlib import Path
from typing import Callable

import pytz
from aiohttp import web
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
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

from asana_sync import (
    reconcile,
    AsanaSyncError,
    validate_notion_schema,
    startup_smoke_test,
)
from cinema.sync import sync_cinema_log_to_notion
from cinema.config import (
    CINEMA_DB_ID,
    FAVE_DB_ID,
    TMDB_API_KEY,
    CINEMA_SYNC_HOUR,
    CINEMA_SYNC_MINUTE,
    validate_config as validate_cinema_config,
)
from sync_telemetry import init_sync_status, utc_now_iso, format_sync_status_message
from scheduler_setup import register_cinema_jobs
from notes_flow import (
    split_kind_keyboard,
    ordered_topics,
    note_topics_keyboard,
    create_note_payload,
)
from second_brain.ai.classify import claude_classify
from second_brain.config import FEATURES
from second_brain.notion import notion_call, notion_call_async
from second_brain.state import STATE
from second_brain.utils import ExpiringDict, reply_notion_error

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
    3) Current working directory.
    """
    override = os.environ.get("BOT_STATE_DIR", "").strip()
    if override:
        state_dir = Path(override).expanduser()
    elif Path("/data").exists():
        state_dir = Path("/data")
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
NOTION_NOTES_DB = os.environ["NOTION_NOTES_DB"]    # 📒 Notes
NOTION_DIGEST_SELECTOR_DB = os.environ["NOTION_DIGEST_SELECTOR_DB"]

TZ           = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
_wk_h, _wk_m = _parse_hhmm_env("DIGEST_TIME_WEEKDAY", "8:15")
_we_h, _we_m = _parse_hhmm_env("DIGEST_TIME_WEEKEND", "12:00")
_rc_h, _rc_m = _parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")

CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))
HTTP_PORT      = int(os.environ.get("PORT", "8080"))
WEEKS_HISTORY  = int(os.environ.get("WEEKS_HISTORY", "52"))
APP_VERSION    = os.environ.get("APP_VERSION", "v10.1.0")
SYNC_BUFFER_MINUTES = max(1, int(os.environ.get("SYNC_BUFFER_MINUTES", "5")))
OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "").strip()
WEATHER_LOCATION = os.environ.get("WEATHER_LOCATION", "Chicago,IL").strip()

# ── Asana sync config ────────────────────────────────────────────────────────
ASANA_PAT           = os.environ.get("ASANA_PAT", "")
ASANA_PROJECT_GID   = os.environ.get("ASANA_PROJECT_GID", "")
ASANA_WORKSPACE_GID = os.environ.get("ASANA_WORKSPACE_GID", "")  # v9.2: required for my_tasks mode
ASANA_SYNC_SOURCE   = os.environ.get("ASANA_SYNC_SOURCE", "project").strip().lower()
ASANA_SYNC_INTERVAL = max(1, int(os.environ.get("ASANA_SYNC_EVERY_SECONDS", "15")))
ASANA_STARTUP_SMOKE = os.environ.get("ASANA_STARTUP_SMOKE", "1").strip().lower() not in {"0", "false", "no", "off"}
ASANA_ARCHIVE_ORPHANS = os.environ.get("ASANA_ARCHIVE_ORPHANS", "0").strip().lower() in {"1", "true", "yes", "on"}
NOTION_WATCHLIST_DB    = os.environ.get("NOTION_WATCHLIST_DB", "")
NOTION_WANTSLIST_V2_DB = os.environ.get("NOTION_WANTSLIST_V2_DB", "")
NOTION_PHOTO_DB        = os.environ.get("NOTION_PHOTO_DB", "")
TMDB_BASE              = "https://api.themoviedb.org/3"

# ── Clients ──────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ── In-memory state ──────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = {}
last_digest_msg_id: int | None = None
pending_map: dict[str, dict] = ExpiringDict(ttl_seconds=3600)
capture_map: dict[int, dict] = {}
done_picker_map: dict[str, list[dict]] = ExpiringDict(ttl_seconds=3600)
todo_picker_map: dict[str, list[dict]] = {}
pending_wantslist_map: dict[str, dict] = {}
pending_photo_map: dict[str, dict] = {}
pending_tmdb_map: dict[str, list[dict]] = {}
pending_message_map: dict[str, str] = {}
pending_note_map: dict[str, dict] = {}
topic_recency_map: dict[str, datetime] = {}
_pending_counter = 0
_done_picker_counter = 0
_todo_picker_counter = 0
_v10_counter = 0
habit_cache: dict[str, dict] = STATE.habit_cache
notes_pending: set[int] = STATE.notes_pending  # chat_ids currently in note-capture mode
_tmdb_http_client: httpx.AsyncClient | None = None
sync_status: dict[str, dict] = init_sync_status()
weather_cache: dict[str, dict] = {
    "current": {"timestamp": None, "data": None},
    "today": {"timestamp": None, "data": None},
    "tomorrow": {"timestamp": None, "data": None},
}
_digest_jobs: list = []
_habit_jobs: list = []
_scheduler: AsyncIOScheduler | None = None
_digest_slots_last_load_succeeded = False
_digest_catchup_sent: set[str] = set()
mute_until: datetime | None = None
STATE_DIR = _resolve_state_dir()
mute_state_file = STATE_DIR / "mute_state.json"
current_location: str = WEATHER_LOCATION
current_lat: float | None = None
current_lon: float | None = None
location_state_file = STATE_DIR / "location_state.json"

# ── Constants ────────────────────────────────────────────────────────────────
HORIZON_DEADLINE_OFFSETS = {"t": 0, "w": 6, "m": 30, "b": None}
HORIZON_LABELS = {
    "t": "🔴 Today", "w": "🟠 This Week",
    "m": "🟡 This Month", "b": "⚪ Backburner",
}
NUMBER_EMOJIS = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
REPEAT_DAY_TO_WEEKDAY  = {"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}
REPEAT_DAY_TO_MONTHDAY = {"1st":1,"5th":5,"10th":10,"15th":15,"20th":20,"25th":25,"Last":-1}
_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)
BTN_REFRESH = "📜Digest"
BTN_ALL_OPEN = "✅To Do"
BTN_HABITS = "🏃 Habits"
BTN_NOTES = "📝 Notes"
BTN_WEATHER = "🌤️ Weather"
BTN_MUTE = "🔕 Mute"
LEGACY_BTN_ALL_OPEN = "📋 All Open"
TOPIC_OPTIONS = [
    "🎵 Acoustics", "💼 Work", "🏠 Personal",
    "💪 Health", "🏢 LEED", "✅ WELL", "💡 Ideas", "📚 Research",
]
_URL_RE = re.compile(r"https?://[^\s\)\]>\"']+", re.IGNORECASE)

def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    today = date.today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


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


def _utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_note_topics_from_notion() -> list[str]:
    """Read Topic multi-select options directly from the Notion Notes DB schema."""
    if not NOTION_NOTES_DB:
        return []

    db = notion.databases.retrieve(database_id=NOTION_NOTES_DB)
    topic_prop = db.get("properties", {}).get("Topic", {})
    if topic_prop.get("type") != "multi_select":
        return []

    options = topic_prop.get("multi_select", {}).get("options", [])
    return [opt.get("name", "").strip() for opt in options if opt.get("name", "").strip()]


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


def save_location_state() -> None:
    """Persist current weather location to disk."""
    try:
        payload = {"location": current_location, "lat": current_lat, "lon": current_lon}
        location_state_file.write_text(json.dumps(payload))
    except Exception as e:
        log.error("Failed saving location state: %s", e)


def load_location_state() -> None:
    """Load weather location from disk or fallback environment defaults."""
    global current_location, current_lat, current_lon
    current_location = WEATHER_LOCATION
    current_lat = None
    current_lon = None
    try:
        if not location_state_file.exists():
            return
        payload = json.loads(location_state_file.read_text() or "{}")
        current_location = payload.get("location") or WEATHER_LOCATION
        lat_raw = payload.get("lat")
        lon_raw = payload.get("lon")
        current_lat = float(lat_raw) if lat_raw not in (None, "") else None
        current_lon = float(lon_raw) if lon_raw not in (None, "") else None
    except Exception as e:
        log.error("Failed loading location state: %s", e)


def set_location(location: str) -> bool:
    """Geocode a location and persist if valid."""
    global current_location, current_lat, current_lon
    if not OPENWEATHER_KEY:
        return False
    try:
        resp = httpx.get(
            "https://api.openweathermap.org/geo/1.0/direct",
            params={"q": location, "limit": 1, "appid": OPENWEATHER_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return False
        row = rows[0]
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            return False
        display_name = row.get("name") or location
        state = row.get("state")
        country = row.get("country")
        pretty = ", ".join([p for p in [display_name, state, country] if p])
        current_location = pretty
        current_lat = float(lat)
        current_lon = float(lon)
        weather_cache["current"] = {"timestamp": None, "data": None}
        weather_cache["today"] = {"timestamp": None, "data": None}
        weather_cache["tomorrow"] = {"timestamp": None, "data": None}
        save_location_state()
        return True
    except Exception as e:
        log.error("Location geocode failed for %s: %s", location, e)
        return False


def _location_candidates(text: str) -> list[str]:
    """Generate high-probability OpenWeather geocode query variants."""
    cleaned = (text or "").strip()
    if not cleaned:
        return []

    candidates: list[str] = [cleaned]
    normalized = re.sub(r"\s+", " ", cleaned)
    if normalized != cleaned:
        candidates.append(normalized)

    # Extract likely location phrase from conversational weather commands.
    phrase_patterns = [
        r"(?:weather|forecast)\s+(?:for|in|at)\s+(.+)$",
        r"(?:set|use|change|update)\s+(?:my\s+)?location\s+(?:to|as)\s+(.+)$",
        r"(?:i(?:'| a)?m|im)\s+in\s+(.+)$",
        r"(?:for|in|at)\s+(.+)$",
    ]
    for pattern in phrase_patterns:
        m = re.search(pattern, normalized, flags=re.IGNORECASE)
        if m:
            fragment = m.group(1).strip(" .!?")
            if fragment:
                candidates.append(fragment)

    slash_fixed = re.sub(r"\s*/\s*", ", ", normalized)
    if slash_fixed != normalized:
        candidates.append(slash_fixed)

    comma_spaced = re.sub(r"\s*,\s*", ", ", slash_fixed)
    if comma_spaced != slash_fixed:
        candidates.append(comma_spaced)

    no_zip = re.sub(r"\b\d{5}(?:-\d{4})?\b", "", comma_spaced).strip(" ,")
    if no_zip and no_zip != comma_spaced:
        candidates.append(no_zip)
    # Keep just ZIP if user provides extra words around it.
    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", comma_spaced)
    if zip_match:
        candidates.append(zip_match.group(0))

    state_map = {
        "illinois": "IL", "california": "CA", "new york": "NY", "texas": "TX",
        "florida": "FL", "washington": "WA", "massachusetts": "MA", "georgia": "GA",
        "colorado": "CO", "arizona": "AZ",
    }
    lowered = no_zip.lower()
    for full, abbr in state_map.items():
        if full in lowered:
            candidates.append(re.sub(rf"\b{re.escape(full)}\b", abbr, no_zip, flags=re.IGNORECASE))

    deduped: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        key = c.strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(c.strip())
    return deduped


def normalize_location_with_claude(text: str) -> list[str]:
    """Ask Claude for structured location parse and return query candidates."""
    prompt = f"""Extract a weather location query from user input.
Input: "{text}"

Return ONLY valid JSON:
{{
  "city": "city name or null",
  "state_code": "2-letter US state code or null",
  "country_code": "2-letter country code or null",
  "postal_code": "postal/zip code or null",
  "normalized_query": "best query for OpenWeather geocoding, e.g. Chicago, IL, US",
  "alternates": ["up to 3 alternate queries"]
}}"""
    try:
        resp = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=180,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        payload = json.loads(raw)
        candidates = []
        normalized = (payload.get("normalized_query") or "").strip()
        if normalized:
            candidates.append(normalized)
        alt = payload.get("alternates") or []
        if isinstance(alt, list):
            candidates.extend(str(a).strip() for a in alt if str(a).strip())
        city = (payload.get("city") or "").strip()
        state_code = (payload.get("state_code") or "").strip().upper()
        country_code = (payload.get("country_code") or "").strip().upper()
        if city:
            if state_code and country_code:
                candidates.append(f"{city}, {state_code}, {country_code}")
            if state_code:
                candidates.append(f"{city}, {state_code}")
            if country_code:
                candidates.append(f"{city}, {country_code}")
            candidates.append(city)
        merged: list[str] = []
        seen: set[str] = set()
        for c in candidates + _location_candidates(text):
            key = c.strip().lower()
            if key and key not in seen:
                seen.add(key)
                merged.append(c.strip())
        return merged
    except Exception as e:
        log.warning("Claude location normalization failed for %r: %s", text, e)
        return _location_candidates(text)


def set_location_smart(user_text: str) -> bool:
    """Resolve user-provided location with Claude-assisted normalization."""
    for query in normalize_location_with_claude(user_text):
        if set_location(query):
            return True

    # Fallback: if a ZIP code is present, try OpenWeather ZIP geocoding route.
    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", user_text or "")
    if zip_match and OPENWEATHER_KEY:
        zip_value = zip_match.group(0)
        try:
            resp = httpx.get(
                "https://api.openweathermap.org/geo/1.0/zip",
                params={"zip": zip_value, "appid": OPENWEATHER_KEY},
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            lat = payload.get("lat")
            lon = payload.get("lon")
            if lat is not None and lon is not None:
                return set_location(f"{payload.get('name') or zip_value}, {payload.get('country') or 'US'}")
        except Exception as e:
            log.warning("ZIP location fallback failed for %s: %s", zip_value, e)
    return False


def fetch_weather(forecast_type: str = "current", force_refresh: bool = False) -> dict | None:
    """
    Fetch current/today/tomorrow weather from OpenWeatherMap.
    Returns normalized weather dict or None if weather is unavailable.
    """
    if forecast_type not in {"current", "today", "tomorrow"}:
        return None
    if not OPENWEATHER_KEY:
        return None

    cache_entry = weather_cache.get(forecast_type, {"timestamp": None, "data": None})
    now = datetime.now(TZ)
    ttl = timedelta(hours=24 if forecast_type == "tomorrow" else 3)
    if not force_refresh and cache_entry.get("timestamp") and cache_entry.get("data"):
        if now - cache_entry["timestamp"] <= ttl:
            return cache_entry["data"]

    try:
        if current_lat is None or current_lon is None:
            if not set_location_smart(current_location):
                return None

        if forecast_type == "current":
            resp = httpx.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"lat": current_lat, "lon": current_lon, "appid": OPENWEATHER_KEY, "units": "metric"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            result = {
                "temp": round(data.get("main", {}).get("temp", 0)),
                "feels_like": round(data.get("main", {}).get("feels_like", 0)),
                "condition": (data.get("weather") or [{}])[0].get("main", "Unknown"),
                "precip_chance": int(round((data.get("pop") or 0) * 100)),
            }
        else:
            resp = httpx.get(
                "https://api.openweathermap.org/data/2.5/forecast",
                params={"lat": current_lat, "lon": current_lon, "appid": OPENWEATHER_KEY, "units": "metric"},
                timeout=10,
            )
            resp.raise_for_status()
            rows = resp.json().get("list", [])
            target = (datetime.now(TZ).date() + timedelta(days=1 if forecast_type == "tomorrow" else 0))
            bucket = []
            for row in rows:
                dt_utc = datetime.utcfromtimestamp(row["dt"]).replace(tzinfo=pytz.utc)
                local_dt = dt_utc.astimezone(TZ)
                if local_dt.date() == target:
                    bucket.append(row)
            if not bucket:
                return None
            highs = [r.get("main", {}).get("temp_max", 0) for r in bucket]
            lows = [r.get("main", {}).get("temp_min", 0) for r in bucket]
            pops = [r.get("pop", 0) for r in bucket]
            conds = [(r.get("weather") or [{}])[0].get("main", "Unknown") for r in bucket]
            mode_condition = max(set(conds), key=conds.count)
            result = {
                "temp_high": round(max(highs)),
                "temp_low": round(min(lows)),
                "condition": mode_condition,
                "precip_chance": int(round(max(pops) * 100)),
            }

        weather_cache[forecast_type] = {"timestamp": now, "data": result}
        return result
    except Exception as e:
        log.error("Weather fetch failed (%s): %s", forecast_type, e)
        return None


def format_weather_block(weather: dict | None, label: str = "🌤️") -> str:
    """Format weather payload into digest-friendly text."""
    if not weather:
        return ""
    if "temp_high" in weather and "temp_low" in weather:
        return (
            f"{label} {weather['condition']} · High {weather['temp_high']}°C / "
            f"Low {weather['temp_low']}°C · 💧{weather.get('precip_chance', 0)}%"
        )
    return f"{label} {weather['temp']}°C ({weather['condition']})"


def digest_location_label() -> str:
    """Compact location label for digest weather line (City, ST or country)."""
    parts = [p.strip() for p in (current_location or "").split(",") if p.strip()]
    if not parts:
        return ""
    if len(parts) >= 3:
        city, state, country = parts[0], parts[1], parts[2]
        country_upper = country.upper()
        if country_upper in {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
            state_abbr = state.upper() if len(state) <= 3 else state[:2].upper()
            return f"{city}, {state_abbr}"
        return f"{city}, {country}"
    if len(parts) == 2:
        return f"{parts[0]}, {parts[1]}"
    return parts[0]


def format_weather_snapshot() -> str:
    """Compose a compact weather summary for quick access."""
    lines = [f"📍 *Weather for {current_location}*"]
    current = format_weather_block(fetch_weather("current"), label="🌤️ Now")
    today = format_weather_block(fetch_weather("today"), label="📅 Today")
    tomorrow = format_weather_block(fetch_weather("tomorrow"), label="🌙 Tomorrow")
    for line in (current, today, tomorrow):
        if line:
            lines.append(line)
    if len(lines) == 1:
        if not OPENWEATHER_KEY:
            lines.append("Weather is unavailable: OPENWEATHER_KEY is missing or invalid.")
        else:
            lines.append("Weather is unavailable. Verify OpenWeather location (try /location) and API key access.")
    return "\n".join(lines)


def weather_unavailable_digest_line() -> str:
    """Digest fallback text when weather cannot be rendered."""
    if current_lat is not None and current_lon is not None and current_location:
        return f"🌤️ Weather unavailable for {current_location} — send /weather to retry or /location to update"
    if current_location:
        return f"🌤️ Weather unavailable. Last location: {current_location} — send /location (city/state/country or ZIP)"
    return "🌤️ Weather unavailable — set with /location (city/state/country or ZIP)"


def mute_status_text() -> str:
    """Human-friendly mute status line."""
    if is_muted() and mute_until:
        return f"🔕 Digests paused until {mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
    return "🔔 Digests are active."


async def fetch_weather_cache(bot) -> None:
    """Scheduled prefetch a few minutes before digest sends."""
    _ = bot
    if not OPENWEATHER_KEY:
        return
    fetch_weather("current", force_refresh=True)
    fetch_weather("today", force_refresh=True)
    fetch_weather("tomorrow", force_refresh=True)
    log.debug("Weather cache refreshed")


# ══════════════════════════════════════════════════════════════════════════════
# HABIT CACHE
# ══════════════════════════════════════════════════════════════════════════════

def load_habit_cache() -> None:
    global habit_cache
    try:
        results = notion.databases.query(
            database_id=NOTION_HABIT_DB,
            filter={"property": "Active", "checkbox": {"equals": True}},
        )
        habit_cache = {}
        for page in results.get("results", []):
            p = page["properties"]
            title_parts = p.get("Habit", {}).get("title", [])
            name = title_parts[0]["text"]["content"] if title_parts else None
            if not name:
                continue
            def sel(key):
                s = p.get(key, {}).get("select")
                return s["name"] if s else None
            def num(key):
                return p.get(key, {}).get("number")
            def txt(key):
                parts = p.get(key, {}).get("rich_text", [])
                return parts[0]["text"]["content"] if parts else None
            habit_cache[name] = {
                "page_id":         page["id"],
                "name":            name,
                "time":            sel("Time"),
                "color":           sel("Color"),
                "freq_per_week":   num("Frequency Per Week"),
                "frequency_label": txt("Frequency Label"),
                "description":     txt("Description"),
                "sort":            num("Sort") or 99,
            }
        log.info(f"Habit cache loaded: {sorted(habit_cache.keys())}")
    except Exception as e:
        log.error(f"Failed to load habit cache: {e}")


def habits_by_time(time_str: str) -> list[dict]:
    return [h for h in habit_cache.values() if h["time"] == time_str]


def get_active_habits_for_trigger() -> list[dict]:
    """
    Fetch active habits sorted by clock time and filtered by weekly frequency quota.
    """
    try:
        results = notion_query_all(
            database_id=NOTION_HABIT_DB,
            filter={"property": "Active", "checkbox": {"equals": True}},
        )
    except Exception as e:
        log.error("get_active_habits_for_trigger query error: %s", e)
        return []

    habits: list[dict] = []
    for page in results:
        try:
            props = page.get("properties", {})
            title_parts = props.get("Habit", {}).get("title", [])
            name = title_parts[0].get("plain_text") if title_parts else None
            if not name:
                name = "Unknown"

            time_prop = props.get("Time", {})
            time_str = ""
            if time_prop.get("type") == "select":
                time_str = (time_prop.get("select") or {}).get("name") or ""
            elif time_prop.get("type") == "rich_text":
                rich = time_prop.get("rich_text", [])
                time_str = (rich[0].get("plain_text") if rich else "") or ""
            time_str = time_str.strip() or "—"
            time_minutes = _parse_time_to_minutes(time_str if time_str != "—" else None)

            frequency: int | None = None
            freq_prop = props.get("Frequency", {})
            freq_text = ""
            if freq_prop.get("type") == "select":
                freq_text = (freq_prop.get("select") or {}).get("name") or ""
            elif freq_prop.get("type") == "rich_text":
                rich = freq_prop.get("rich_text", [])
                freq_text = (rich[0].get("plain_text") if rich else "") or ""
            elif freq_prop.get("type") == "number":
                raw_num = freq_prop.get("number")
                if isinstance(raw_num, (int, float)) and raw_num > 0:
                    frequency = int(raw_num)
            if frequency is None and freq_text:
                m = re.search(r"\d+", freq_text)
                if m:
                    frequency = int(m.group(0))

            completion_count = _count_habit_completions_this_week(page["id"])
            if frequency and frequency > 0 and completion_count >= frequency:
                continue

            habits.append(
                {
                    "page_id": page["id"],
                    "name": name,
                    "time_minutes": time_minutes,
                    "time_str": time_str,
                    "frequency": frequency,
                    "completion_count": completion_count,
                }
            )
        except Exception as e:
            log.error("get_active_habits_for_trigger parse error for %s: %s", page.get("id"), e)

    habits.sort(key=lambda h: (h["time_minutes"] < 0, h["time_minutes"] if h["time_minutes"] >= 0 else 10**9, h["name"].lower()))
    return habits


def get_habits_by_time(time_filter: str) -> list[dict]:
    """Legacy wrapper kept for compatibility."""
    del time_filter
    return get_active_habits_for_trigger()


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
        if ww_norm in {"weekday", "weekdays", "mon-fri"}:
            is_weekday = True
        elif ww_norm in {"weekend", "weekends", "sat,sun", "sat/sun"}:
            is_weekday = False
        else:
            log.warning("Skipping digest selector row with invalid Weekday/Weekend=%r", ww_name)
            continue

        include_habits = bool(props.get("Habits", {}).get("checkbox", False))
        max_items_raw = props.get("Max Items", {}).get("number")
        max_items = int(max_items_raw) if isinstance(max_items_raw, (int, float)) else None

        selected_contexts = [
            context_label
            for prop_name, context_label in context_map.items()
            if bool(props.get(prop_name, {}).get("checkbox", False))
        ]
        contexts = selected_contexts or None

        if contexts is None and not include_habits:
            continue

        slots.append(
            {
                "time": slot_time,
                "is_weekday": is_weekday,
                "include_habits": include_habits,
                "max_items": max_items,
                "contexts": contexts,
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
        return lines
    return [text]


def looks_like_task_batch(text: str) -> bool:
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

def classify_message(text: str) -> dict:
    habit_names = list(habit_cache.keys())
    prompt = f"""You are a personal assistant classifier for a second brain system.
Today is {date.today().strftime("%A, %B %-d, %Y")}.

Message: "{text}"

Active habits to detect: {habit_names}
Workout types that count as 💪 Workout: soccer, crossfit, hyrox, rowing, snowboard, skiing, gym, run, jog, trained

First determine if this is:
A) HABIT LOG — person saying they completed something NOW
B) TASK — something to be done in the future

Return ONLY valid JSON, no markdown:

If HABIT:
{{"type": "habit", "habit_name": "exact name from {habit_names} or null", "confidence": "high or low"}}

If TASK:
{{
  "type": "task",
  "task_name": "clean concise action",
  "deadline_days": <integer or null>,
  "context": "one of: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab",
  "confidence": "high or low",
  "recurring": "one of: None | 🔁 Daily | 📅 Weekly | 🗓️ Monthly",
  "repeat_day": "Mon|Tue|Wed|Thu|Fri|Sat|Sun|1st|5th|10th|15th|20th|25th|Last or null"
}}

deadline_days: 0=today, 1=tomorrow, 5=this week, 20=this month, null=no urgency/low confidence"""

    resp = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOK,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


def classify_message_v10(text: str) -> dict:
    """Classifier with watchlist/wantslist/photo intents in addition to habit/task."""
    habit_names = list(habit_cache.keys())

    watchlist_enabled = bool(NOTION_WATCHLIST_DB)
    wantslist_enabled = bool(NOTION_WANTSLIST_V2_DB)
    photo_enabled = bool(NOTION_PHOTO_DB)
    notes_enabled = bool(NOTION_NOTES_DB)

    enabled_intents = ["habit", "task"]
    if watchlist_enabled:
        enabled_intents.append("watchlist")
    if wantslist_enabled:
        enabled_intents.append("wantslist")
    if photo_enabled:
        enabled_intents.append("photo")
    if notes_enabled:
        enabled_intents.append("note")

    prompt = f"""You are a personal assistant classifier for a second brain system.
Today is {date.today().strftime("%A, %B %-d, %Y")}.

Message: "{text}"

Active habits: {habit_names}
Workout types that count as 💪 Workout: soccer, crossfit, hyrox, rowing, snowboard, skiing, gym, run, jog, trained

Enabled intent types: {enabled_intents}

Classify this message into EXACTLY ONE intent. Rules:

WATCHLIST — user wants to watch a TV series, film, anime, or documentary in the future.
  Signals: "want to watch", "add to watchlist", "watch:", "should watch", title + "is good", "put X on my list"
  media_type: one of Series | Film | Anime | Documentary

WANTSLIST — user wants to buy or acquire a physical product/item.
  Signals: "want to buy", "want to get", "need a", "looking for", product names (gadgets, clothes, furniture, gear)
  category: one of Tech | Home | Clothes | Health | Other

PHOTO — user wants to capture a photography scene/subject/location.
  Signals: "want to shoot", "want to photograph", "photo spot", "add to bucketlist", photography subjects

NOTE — user wants to save information/reference/thought without an action.
  Signals: "note:", "idea:", "code:", "remember this", summaries, ideas, code snippets, links/articles to keep, journaling.
  Should NOT be used for actionable commitments with due timing (those are TASKs).

HABIT — user saying they completed a recurring habit RIGHT NOW.
  Signals: "did", "took", "went to", "had", "completed" + habit name

TASK — something to be done in the future (default if nothing else matches).

If confidence is low on watchlist/wantslist/photo, return task instead.
"Watch:" prefix = always watchlist, high confidence.
"want:" prefix = always wantslist, high confidence.
"photo:" prefix = always photo, high confidence.
"note:" prefix = always note, high confidence.
"idea:" prefix = always note, high confidence.
"code:" prefix = always note, high confidence.

Return ONLY valid JSON, no markdown:

If WATCHLIST:
{{"type": "watchlist", "title": "clean title only, no year", "media_type": "Series|Film|Anime|Documentary", "confidence": "high|low"}}

If WANTSLIST:
{{"type": "wantslist", "item": "clean item name", "category": "Tech|Home|Clothes|Health|Other", "confidence": "high|low"}}

If PHOTO:
{{"type": "photo", "subject": "clean scene/subject description", "confidence": "high|low"}}

If NOTE:
{{"type": "note", "content": "clean note content", "confidence": "high|low"}}

If HABIT:
{{"type": "habit", "habit_name": "exact name from {habit_names} or null", "confidence": "high|low"}}

If TASK:
{{
  "type": "task",
  "task_name": "clean concise action",
  "deadline_days": <integer or null>,
  "context": "one of: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab",
  "confidence": "high|low",
  "recurring": "None|🔁 Daily|📅 Weekly|🗓️ Monthly",
  "repeat_day": "Mon|Tue|Wed|Thu|Fri|Sat|Sun|1st|5th|10th|15th|20th|25th|Last or null"
}}"""

    resp = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=250,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


async def start_note_capture_flow(message, text: str) -> None:
    if not NOTION_NOTES_DB:
        await create_or_prompt_task(message, text)
        return

    global _v10_counter
    note_key = str(_v10_counter)
    _v10_counter += 1
    try:
        topics = fetch_note_topics_from_notion()
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
        create_note_entry(text)
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


def classify_note(title: str, description: str, url: str, raw_text: str) -> dict:
    """Ask Claude to pick Topic tags and a clean title. Returns {title, topics}."""
    context = title or description or raw_text or url
    prompt = f"""You are classifying a saved note/link for a second brain system.

Note context: "{context}"
URL: {url}

Available topics: {TOPIC_OPTIONS}

Return ONLY valid JSON, no markdown:
{{
  "title": "short descriptive title (max 80 chars, use the page title if good)",
  "topics": ["pick 1-2 most relevant topics from the list above"]
}}"""
    try:
        resp = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        result = json.loads(raw)
        valid_topics = [t for t in result.get("topics", []) if t in TOPIC_OPTIONS]
        return {
            "title": result.get("title", title or url)[:200],
            "topics": valid_topics or ["💡 Ideas"],
        }
    except Exception as e:
        log.error(f"classify_note error: {e}")
        return {"title": title or url[:200], "topics": ["💡 Ideas"]}


def save_note(title: str, url: str | None, content: str, topics: list[str], note_type: str) -> str:
    """Write a note to the 📒 Notes Notion DB. Returns page_id."""
    today = date.today().isoformat()
    props: dict = {
        "Title": {"title": [{"text": {"content": title or "Untitled"}}]},
        "Type": {"select": {"name": note_type}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Date Created": {"date": {"start": today}},
        "Processed": {"checkbox": False},
    }
    if url:
        props["Link"] = {"url": url}
    if content:
        props["Content"] = {"rich_text": [{"text": {"content": content[:2000]}}]}
    if topics:
        props["Topic"] = {"multi_select": [{"name": t} for t in topics]}
    page = notion.pages.create(
        parent={"database_id": NOTION_NOTES_DB},
        properties=props,
    )
    return page["id"]


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
            meta = await asyncio.get_event_loop().run_in_executor(
                None, fetch_url_metadata, url
            )
            classified = await asyncio.get_event_loop().run_in_executor(
                None, classify_note,
                meta["title"], meta["description"], url, text,
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

        save_note(note_title, url, content, topics, note_type)
        icon = "🔗" if url else "📝"
        topic_str = "  ".join(topics)
        await thinking.edit_text(
            f"📒 Saved!\n\n{icon} *{note_title}*\n🏷 {topic_str}\n\n_Saved to Notion_",
            parse_mode="Markdown",
        )
    except Exception as e:
        log.error(f"save_note error: {e}")
        await thinking.edit_text(f"⚠️ Couldn't save note to Notion.\n_{e}_", parse_mode="Markdown")


def classify_task(text: str) -> dict:
    prompt = f"""You are a personal task classifier for a second brain system.
Today is {date.today().strftime("%A, %B %-d, %Y")}.

Message: \"{text}\"

Extract the ACTUAL TASK — the thing the person needs to DO.

Return ONLY valid JSON, no markdown:
{{
  "task_name": "clean concise action",
  "deadline_days": <integer days from today, or null if no urgency>,
  "context": "one of exactly: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab",
  "confidence": "high or low",
  "recurring": "one of exactly: None | 🔁 Daily | 📅 Weekly | 🗓️ Monthly",
  "repeat_day": "one of: Mon|Tue|Wed|Thu|Fri|Sat|Sun|1st|5th|10th|15th|20th|25th|Last or null"
}}

deadline_days: 0=today, 1=tomorrow, 5=this week, 20=this month, null=no urgency"""

    resp = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


def deadline_days_to_label(days: int | None) -> str:
    if days is None: return "⚪ Backburner"
    if days <= 0:    return "🔴 Today"
    if days <= 7:    return "🟠 This Week"
    if days <= 31:   return "🟡 This Month"
    return "⚪ Backburner"


# ══════════════════════════════════════════════════════════════════════════════
# V10 REFERENCE DATABASE FLOWS
# ══════════════════════════════════════════════════════════════════════════════

async def tmdb_search(title: str, media_type: str = "multi") -> list[dict]:
    if not TMDB_API_KEY:
        return []
    try:
        client = _get_tmdb_http_client()
        resp = await client.get(
            f"{TMDB_BASE}/search/{media_type}",
            params={"api_key": TMDB_API_KEY, "query": title, "page": 1},
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])[:5]

        candidates: list[dict] = []
        for r in results:
            mtype = r.get("media_type") or media_type
            if mtype not in ("tv", "movie"):
                continue
            candidate = {
                "tmdb_id": str(r.get("id", "")),
                "title": r.get("name") or r.get("title") or title,
                "media_type": mtype,
                "year": (r.get("first_air_date") or r.get("release_date") or "")[:4],
                "seasons": None,
                "episodes": None,
                "runtime": None,
            }
            if mtype == "tv":
                try:
                    det = await client.get(
                        f"{TMDB_BASE}/tv/{r['id']}",
                        params={"api_key": TMDB_API_KEY},
                    )
                    det.raise_for_status()
                    d = det.json()
                    candidate["seasons"] = d.get("number_of_seasons")
                    candidate["episodes"] = d.get("number_of_episodes")
                    rt = d.get("episode_run_time") or []
                    candidate["runtime"] = rt[0] if rt else None
                except Exception as e:
                    log.warning("TMDB TV detail lookup failed for id=%s: %s", r.get("id"), e)
            candidates.append(candidate)
        return candidates
    except Exception as e:
        log.warning(f"TMDB search failed for '{title}': {e}")
        return []


def _notion_type_from_tmdb(media_type: str) -> str:
    return {"tv": "Series", "movie": "Film"}.get(media_type, "Series")


def _tmdb_media_slug(media_type: str) -> str:
    normalized = (media_type or "").strip().lower()
    if normalized in {"film", "movie"}:
        return "movie"
    if normalized in {"series", "tv", "tv series", "anime", "documentary"}:
        return "tv"
    return ""


def _get_tmdb_http_client() -> httpx.AsyncClient:
    global _tmdb_http_client
    if _tmdb_http_client is None:
        _tmdb_http_client = httpx.AsyncClient(timeout=8.0)
    return _tmdb_http_client


def create_watchlist_entry(
    title: str,
    media_type: str = "Series",
    tmdb_id: str = "",
    seasons: int | None = None,
    episodes: int | None = None,
    runtime: int | None = None,
) -> str:
    tmdb_url = ""
    tmdb_id_str = str(tmdb_id).strip() if tmdb_id is not None else ""
    if tmdb_id_str:
        media_slug = _tmdb_media_slug(media_type)
        if media_slug:
            tmdb_url = f"https://www.themoviedb.org/{media_slug}/{tmdb_id_str}"
    props: dict = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Type": {"select": {"name": media_type}},
        "Status": {"select": {"name": "Queued"}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Added": {"date": {"start": date.today().isoformat()}},
    }
    if tmdb_id_str:
        props["TMDB ID"] = {"rich_text": [{"text": {"content": tmdb_id_str}}]}
    if tmdb_url:
        props["TMDB URL"] = {"url": tmdb_url}
    if seasons is not None:
        props["Seasons"] = {"number": seasons}
    if episodes is not None:
        props["Episodes"] = {"number": episodes}
    if runtime is not None:
        props["Runtime (mins/ep)"] = {"number": runtime}

    page = notion.pages.create(parent={"database_id": NOTION_WATCHLIST_DB}, properties=props)
    return page["id"]


def watchlist_duplicate(title: str) -> bool:
    results = notion.databases.query(
        database_id=NOTION_WATCHLIST_DB,
        filter={"property": "Title", "title": {"equals": title}},
    )
    return len(results.get("results", [])) > 0


def create_wantslist_entry(
    item: str,
    category: str = "Other",
    priority: str = "Medium",
    est_cost: float | None = None,
    url: str | None = None,
    notes: str | None = None,
) -> str:
    props: dict = {
        "Item": {"title": [{"text": {"content": item}}]},
        "Category": {"select": {"name": category}},
        "Priority": {"select": {"name": priority}},
        "Status": {"select": {"name": "Wanted"}},
        "Source": {"select": {"name": "📱 Telegram"}},
    }
    if est_cost is not None:
        props["Est. Cost"] = {"number": est_cost}
    if url:
        props["userDefined:URL"] = {"url": url}
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}

    page = notion.pages.create(parent={"database_id": NOTION_WANTSLIST_V2_DB}, properties=props)
    return page["id"]


def create_photo_entry(
    subject: str,
    location: str | None = None,
    season: str | None = None,
    time_of_day: str | None = None,
    notes: str | None = None,
) -> str:
    props: dict = {
        "Subject": {"title": [{"text": {"content": subject}}]},
        "Status": {"select": {"name": "Wishlist"}},
        "Source": {"select": {"name": "📱 Telegram"}},
    }
    if location:
        props["Location"] = {"rich_text": [{"text": {"content": location}}]}
    if season:
        props["Season"] = {"select": {"name": season}}
    if time_of_day:
        props["Time of Day"] = {"select": {"name": time_of_day}}
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}

    page = notion.pages.create(parent={"database_id": NOTION_PHOTO_DB}, properties=props)
    return page["id"]


def wantslist_confirm_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Save to Wantslist", callback_data=f"wl_save:{key}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"wl_cancel:{key}"),
    ]])


def tmdb_candidates_keyboard(key: str, candidates: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for i, c in enumerate(candidates[:5]):
        label = f"{c['title']} ({c['year']}) · {_notion_type_from_tmdb(c['media_type'])}"
        if len(label) > 38:
            label = label[:35] + "..."
        rows.append([InlineKeyboardButton(label, callback_data=f"tmdb_pick:{key}:{i}")])
    rows.append([InlineKeyboardButton("➕ Save title only", callback_data=f"tmdb_skip:{key}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"tmdb_cancel:{key}")])
    return InlineKeyboardMarkup(rows)


def _save_watchlist_from_candidate(c: dict, fallback_title: str) -> str:
    return create_watchlist_entry(
        title=c.get("title") or fallback_title,
        media_type=_notion_type_from_tmdb(c.get("media_type", "tv")),
        tmdb_id=c.get("tmdb_id", ""),
        seasons=c.get("seasons"),
        episodes=c.get("episodes"),
        runtime=c.get("runtime"),
    )


async def handle_watchlist_intent(message, title: str, media_type: str) -> None:
    global _v10_counter
    if not NOTION_WATCHLIST_DB:
        await message.reply_text("📺 Watchlist isn't configured yet — NOTION_WATCHLIST_DB missing.")
        return
    if watchlist_duplicate(title):
        await message.reply_text(f"📺 *{title}* is already on your watchlist!", parse_mode="Markdown")
        return

    thinking = await message.reply_text("📺 Searching TMDB...")
    candidates = await tmdb_search(
        title,
        media_type="tv" if media_type == "Series" else "movie" if media_type == "Film" else "multi",
    )

    if not candidates:
        create_watchlist_entry(title, media_type=media_type)
        await thinking.edit_text(
            f"📺 Added to watchlist!\n\n*{title}* · {media_type}\n_No TMDB metadata found — saved title only_",
            parse_mode="Markdown",
        )
        return

    if len(candidates) == 1:
        c = candidates[0]
        _save_watchlist_from_candidate(c, title)
        seasons_str = f" · {c['seasons']} seasons" if c.get("seasons") else ""
        episodes_str = f" · {c['episodes']} eps" if c.get("episodes") else ""
        runtime_str = f" · {c['runtime']} min/ep" if c.get("runtime") else ""
        await thinking.edit_text(
            f"📺 Added to watchlist!\n\n*{c['title']}* ({c['year']}) · {_notion_type_from_tmdb(c['media_type'])}"
            f"{seasons_str}{episodes_str}{runtime_str}\n_Saved to Notion_",
            parse_mode="Markdown",
        )
        return

    key = str(_v10_counter)
    _v10_counter += 1
    pending_tmdb_map[key] = candidates
    await thinking.edit_text(
        f"📺 Found a few matches for *{title}* — which one?",
        parse_mode="Markdown",
        reply_markup=tmdb_candidates_keyboard(key, candidates),
    )


async def handle_wantslist_intent(message, item: str, category: str) -> None:
    global _v10_counter
    if not NOTION_WANTSLIST_V2_DB:
        await message.reply_text("🎁 Wantslist isn't configured yet — NOTION_WANTSLIST_V2_DB missing.")
        return
    key = str(_v10_counter)
    _v10_counter += 1
    pending_wantslist_map[key] = {"item": item, "category": category}
    await message.reply_text(
        f"🎁 Save *{item}* to your Wantslist?\n_Category: {category}_",
        parse_mode="Markdown",
        reply_markup=wantslist_confirm_keyboard(key),
    )


async def handle_photo_intent(message, subject: str) -> None:
    global _v10_counter
    if not NOTION_PHOTO_DB:
        await message.reply_text("📷 Photo Bucketlist isn't configured yet — NOTION_PHOTO_DB missing.")
        return

    key = str(_v10_counter)
    _v10_counter += 1
    pending_photo_map[key] = {"subject": subject}
    await message.reply_text(
        f"📷 *{subject}* added to your photo bucketlist!\n\n"
        "_Optionally reply with location and/or best season — e.g. `Kyoto, Autumn` — "
        "or just ignore this and fill it in Notion later._\n\n"
        f"_Reference: `photo_key:{key}`_",
        parse_mode="Markdown",
    )
    page_id = create_photo_entry(subject)
    pending_photo_map[key]["page_id"] = page_id


def _parse_photo_followup(text: str) -> tuple[str | None, str | None, str | None]:
    seasons = {"spring", "summer", "autumn", "fall", "winter", "any"}
    season_map = {"fall": "Autumn"}
    times = {"golden hour", "blue hour", "midday", "night", "any"}
    time_labels = {
        "golden hour": "Golden Hour",
        "blue hour": "Blue Hour",
        "midday": "Midday",
        "night": "Night",
        "any": "Any",
    }
    parts = [p.strip() for p in re.split(r"[,/|·]+", text) if p.strip()]
    location, season, time_of_day = None, None, None
    for part in parts:
        lower = part.lower()
        if lower in seasons:
            season = season_map.get(lower, lower.capitalize())
        elif lower in times:
            time_of_day = time_labels[lower]
        elif not location:
            location = part
    return location, season, time_of_day


async def handle_photo_followup(message, text: str) -> bool:
    key = None
    if message.reply_to_message:
        replied = message.reply_to_message.text or ""
        m = re.search(r"photo_key:(\w+)", replied)
        if m:
            key = m.group(1)

    if key and key in pending_photo_map:
        entry = pending_photo_map[key]
        page_id = entry.get("page_id")
        if not page_id:
            return False
        location, season, time_of_day = _parse_photo_followup(text)
        props: dict = {}
        if location:
            props["Location"] = {"rich_text": [{"text": {"content": location}}]}
        if season:
            props["Season"] = {"select": {"name": season}}
        if time_of_day:
            props["Time of Day"] = {"select": {"name": time_of_day}}
        if props:
            notion.pages.update(page_id=page_id, properties=props)
            parts = []
            if location:
                parts.append(f"📍 {location}")
            if season:
                parts.append(f"🗓️ {season}")
            if time_of_day:
                parts.append(f"🕐 {time_of_day}")
            await message.reply_text(f"📷 Updated: {' · '.join(parts)}\n_Saved to Notion_", parse_mode="Markdown")
            del pending_photo_map[key]
            return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
# NOTION — HABIT LOG
# ══════════════════════════════════════════════════════════════════════════════

def log_habit(habit_page_id: str, habit_name: str, source: str = "📱 Telegram") -> None:
    today = datetime.now(TZ).date().isoformat()
    notion.pages.create(
        parent={"database_id": NOTION_LOG_DB},
        properties={
            "Entry":     {"title":    [{"text": {"content": f"{habit_name} — {today}"}}]},
            "Habit":     {"relation": [{"id": habit_page_id}]},
            "Completed": {"checkbox": True},
            "Date":      {"date":     {"start": today}},
            "Source":    {"select":   {"name": source}},
        },
    )
    log.info(f"Habit logged: {habit_name} on {today} via {source}")


def already_logged_today(habit_page_id: str) -> bool:
    today = datetime.now(TZ).date().isoformat()
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

def _deadline_prop(days: int | None) -> dict:
    if days is None:
        return {"date": None}
    return {"date": {"start": (date.today() + timedelta(days=days)).isoformat()}}


def create_task(name: str, deadline_days: int | None, context: str,
                recurring: str = "None", repeat_day: str | None = None) -> str:
    props = {
        "Name":      {"title":  [{"text": {"content": name}}]},
        "Deadline":  _deadline_prop(deadline_days),
        "Context":   {"select": {"name": context}},
        "Source":    {"select": {"name": "📱 Telegram"}},
        "Recurring": {"select": {"name": recurring}},
    }
    if repeat_day:
        props["Repeat Day"] = {"select": {"name": repeat_day}}
    page = notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
    return page["id"]


def mark_done(page_id: str) -> None:
    notion.pages.update(page_id=page_id, properties={"Done": {"checkbox": True}})


def set_deadline_from_horizon_code(page_id: str, code: str) -> None:
    days = HORIZON_DEADLINE_OFFSETS.get(code)
    if days is None:
        notion.pages.update(page_id=page_id, properties={"Deadline": {"date": None}})
    else:
        target = date.today() + timedelta(days=days)
        notion.pages.update(page_id=page_id, properties={"Deadline": {"date": {"start": target.isoformat()}}})


def set_focus(page_id: str, focused: bool) -> None:
    notion.pages.update(page_id=page_id, properties={"Focus": {"checkbox": focused}})


def set_last_generated(page_id: str, d: date) -> None:
    notion.pages.update(page_id=page_id, properties={"Last Generated": {"date": {"start": d.isoformat()}}})


def _get_prop(props: dict, key: str, kind: str):
    prop = props.get(key, {})
    if kind == "title":
        parts = prop.get("title", [])
        return parts[0]["text"]["content"] if parts else None
    if kind == "select":
        sel = prop.get("select")
        return sel["name"] if sel else None
    if kind == "formula":
        f = prop.get("formula", {})
        return f.get("string") or f.get("number") or None
    if kind == "date":
        d = prop.get("date")
        return d["start"] if d else None
    if kind == "checkbox":
        return prop.get("checkbox", False)
    return None


def _normalize_task_name(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r"\b(today|tonight|tomorrow|this week|this month|asap|urgent|by eod)\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s.endswith("s") and len(s) > 4:
        s = s[:-1]
    return s


def query_tasks_by_auto_horizon(horizons: list[str]) -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={
            "and": [
                {"property": "Done", "checkbox": {"equals": False}},
                {"or": [
                    {"property": "Auto Horizon", "formula": {"string": {"equals": h}}}
                    for h in horizons
                ]},
            ]
        },
    )
    tasks = []
    for page in results.get("results", []):
        p = page["properties"]
        tasks.append({
            "page_id":      page["id"],
            "name":         _get_prop(p, "Name",         "title")   or "Untitled",
            "auto_horizon": _get_prop(p, "Auto Horizon", "formula") or "",
            "context":      _get_prop(p, "Context",      "select")  or "",
            "deadline":     _get_prop(p, "Deadline",     "date"),
        })
    return tasks


def get_all_active_tasks() -> list[dict]:
    results = notion_query_all(
        NOTION_DB_ID,
        filter={"property": "Done", "checkbox": {"equals": False}},
    )
    return [
        {
            "page_id":      p["id"],
            "name":         _get_prop(p["properties"], "Name",         "title")   or "Untitled",
            "auto_horizon": _get_prop(p["properties"], "Auto Horizon", "formula") or "",
            "context":      _get_prop(p["properties"], "Context",      "select")  or "",
            "deadline":     _get_prop(p["properties"], "Deadline",     "date"),
        }
        for p in results
    ]


def _parse_deadline(raw_deadline: str | None) -> date | None:
    if not raw_deadline:
        return None
    try:
        return datetime.fromisoformat(raw_deadline).date()
    except Exception:
        try:
            return date.fromisoformat(raw_deadline[:10])
        except Exception:
            return None


def _context_label(task: dict) -> str:
    return (task.get("context") or "").strip()


def _task_sort_key(task: dict) -> tuple[int, str, str]:
    deadline = _parse_deadline(task.get("deadline"))
    deadline_ord = deadline.toordinal() if deadline else 99999999
    context = (task.get("context") or "").lower()
    name = (task.get("name") or "").lower()
    return (deadline_ord, context, name)


def _get_tasks_by_deadline_horizon() -> tuple[list, list, list, list]:
    """
    Returns: (overdue, today, this_week, backlog)

    Overdue: deadline < date.today()
    Today: deadline == date.today()
    This Week: 1 <= days_until_deadline <= 7
    Backlog: days_until_deadline > 7 OR no deadline set
    """
    tasks = get_all_active_tasks()
    today = date.today()

    overdue: list[dict] = []
    today_tasks: list[dict] = []
    this_week: list[dict] = []
    backlog: list[dict] = []

    for task in tasks:
        deadline = _parse_deadline(task.get("deadline"))
        if deadline is None:
            backlog.append(task)
            continue

        if deadline < today:
            overdue.append(task)
            continue

        if deadline == today:
            today_tasks.append(task)
            continue

        days_until = (deadline - today).days
        if 1 <= days_until <= 7:
            this_week.append(task)
        else:
            backlog.append(task)

    return (
        sorted(overdue, key=_task_sort_key),
        sorted(today_tasks, key=_task_sort_key),
        sorted(this_week, key=_task_sort_key),
        sorted(backlog, key=_task_sort_key),
    )


def format_hybrid_digest(tasks: list[dict]) -> tuple[str, list[dict]]:
    """Main digest message with status peek and critical sections."""
    del tasks  # counts and sections are always computed fresh
    overdue, today_tasks, this_week, backlog = _get_tasks_by_deadline_horizon()

    now_dt = datetime.now(TZ)
    date_str = now_dt.strftime("%A, %B %-d")

    summary_parts = []
    if overdue:
        summary_parts.append(f"{len(overdue)} overdue")
    if today_tasks:
        summary_parts.append(f"{len(today_tasks)} due today")
    if this_week:
        summary_parts.append(f"{len(this_week)} this week")
    if backlog:
        summary_parts.append(f"{len(backlog)} backlog")
    if not summary_parts:
        summary_parts = ["0 due today"]

    lines = [
        f"☀️ *{date_str}*",
        "",
        f"📊 {', '.join(summary_parts)}",
        "",
    ]

    ordered: list[dict] = []
    n = 1

    lines.append("🚨 *Overdue*")
    if overdue:
        for task in overdue:
            lines.append(f"{num_emoji(n)} {task['name']}  {_context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")
    lines.append("")

    lines.append("📌 *Due Today*")
    if today_tasks:
        for task in today_tasks:
            lines.append(f"{num_emoji(n)} {task['name']}  {_context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")

    lines.append("")
    if ordered:
        lines.append("_Reply `done 1`, `done 1,3`, or `done: task name` to complete_")

    return "\n".join(lines).strip(), ordered


def format_week_view(view_type: str) -> tuple[str, list[dict]]:
    """Return the This Week or Backlog expanded view."""
    _, _, this_week, backlog = _get_tasks_by_deadline_horizon()

    if view_type == "week":
        title = "🟠 *This Week (2–7 days)*"
        tasks = this_week
        max_display = None
    elif view_type == "backlog":
        title = "⚪ *Backlog (7+ days)*"
        tasks = backlog
        max_display = 20
    else:
        raise ValueError("view_type must be 'week' or 'backlog'")

    lines = [title]
    if not tasks:
        lines.append("✅ Nothing — all clear!")
        lines.append("")
        lines.append("_Tap items below to adjust urgency 👇_")
        return "\n".join(lines), []

    shown = tasks
    hidden_count = 0
    if max_display is not None and len(tasks) > max_display:
        shown = tasks[:max_display]
        hidden_count = len(tasks) - max_display

    for i, task in enumerate(shown, 1):
        lines.append(f"{num_emoji(i)} {task['name']}  {_context_label(task)}")

    if hidden_count:
        lines.append("")
        lines.append(f"... and {hidden_count} more (view in Notion)")

    lines.append("")
    lines.append("_Tap items below to adjust urgency 👇_")

    return "\n".join(lines), shown


def format_command_palette() -> InlineKeyboardMarkup:
    """Returns the 6-button command palette."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Digest", callback_data="qp:digest"),
            InlineKeyboardButton("✅ To Do", callback_data="qp:todo"),
            InlineKeyboardButton("🎯 Habits", callback_data="qp:habits"),
        ],
        [
            InlineKeyboardButton("📝 Notes", callback_data="qp:notes"),
            InlineKeyboardButton("🌤️ Weather", callback_data="qp:weather"),
            InlineKeyboardButton("🔇 Mute", callback_data="qp:mute"),
        ],
    ])


def back_to_palette_keyboard() -> InlineKeyboardMarkup:
    """Single button to return to command palette."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Back to Palette", callback_data="qp:back")],
    ])


def _get_today_tasks_for_palette() -> list[dict]:
    """Get today's tasks only (for To Do view)."""
    tasks = get_today_and_overdue_tasks()
    today_str = date.today().isoformat()
    return [t for t in tasks if t.get("deadline") == today_str]


def format_digest_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build digest view for today + next 7 calendar days, grouped by date."""
    today = date.today()
    cutoff = today + timedelta(days=7)
    tasks = get_all_active_tasks()
    groups: dict[str, list[dict]] = defaultdict(list)
    beyond_count = 0

    for task in tasks:
        raw_deadline = task.get("deadline")
        parsed_deadline = _parse_deadline(raw_deadline)
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
            day_tasks = sorted(groups[d], key=_task_sort_key)
            date_label = date.fromisoformat(d).strftime("%A, %B %-d")
            lines.append(f"📌 {date_label} ({len(day_tasks)})")
            for task in day_tasks:
                lines.append(f"  • {task.get('name', 'Untitled')}  {_context_label(task)}")
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    if beyond_count:
        lines.extend(["", f"...and {beyond_count} more beyond 7 days (view in Notion)"])

    return "\n".join(lines).strip(), back_to_palette_keyboard()


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
                [InlineKeyboardButton(f"{num_emoji(idx + 1)} {label}", callback_data=f"qp:done:{idx}")]
            )
        if len(marked_done_indices) >= len(tasks):
            lines.append("✅ All today's tasks marked done! 🎉")

    keyboard_rows.append([InlineKeyboardButton("📖 Back to Palette", callback_data="qp:back")])
    return "\n".join(lines).strip(), InlineKeyboardMarkup(keyboard_rows)


def quick_access_keyboard() -> InlineKeyboardMarkup:
    """Keyboard with live This Week and Backlog counts."""
    _, _, this_week, backlog = _get_tasks_by_deadline_horizon()
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(f"🟠 This Week ({len(this_week)})", callback_data="qv:week"),
            InlineKeyboardButton(f"⚪ Backlog ({len(backlog)})", callback_data="qv:backlog"),
        ]]
    )


def horizon_view_back_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for expanded horizon views."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("← Back to Today", callback_data="digest:today")],
            [InlineKeyboardButton("📅 Full Sunday Review", callback_data="digest:sunday")],
        ]
    )


def get_today_and_overdue_tasks(limit: int | None = 10) -> list[dict]:
    tasks = get_all_active_tasks()
    today = date.today()
    today_str = today.isoformat()
    cutoff_str = (today + timedelta(days=7)).isoformat()
    selected = []

    def context_rank(task: dict) -> tuple[int, str]:
        ctx = (task.get("context") or "").lower()
        if "personal" in ctx or "🏠" in ctx:
            return (0, task.get("name", "").lower())
        if "work" in ctx or "💼" in ctx:
            return (2, task.get("name", "").lower())
        return (1, task.get("name", "").lower())

    for t in tasks:
        deadline = t.get("deadline")
        is_today = t["auto_horizon"] == "🔴 Today"
        is_overdue = bool(deadline and deadline < today_str)
        is_this_week = t["auto_horizon"] == "🟠 This Week"
        due_within_7_days = bool(deadline and today_str <= deadline <= cutoff_str)
        if is_today or is_overdue or is_this_week or due_within_7_days:
            selected.append(t)

    overdue = [t for t in selected if t["deadline"] and t["deadline"] < today_str]
    today_only = [t for t in selected if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    carryover = [
        t for t in selected
        if t not in overdue and t not in today_only
    ]

    overdue = sorted(overdue, key=context_rank)
    today_only = sorted(today_only, key=context_rank)
    carryover = sorted(carryover, key=context_rank)
    ordered = overdue + today_only + carryover
    return ordered[:limit] if isinstance(limit, int) else ordered


def get_quick_refresh_tasks(limit: int = 10) -> list[dict]:
    """
    Tasks for quick Refresh / To Do:
    - must have a due date
    - include overdue and next 7 days (inclusive)
    - order Personal first, then Work, then anything else
    """
    tasks = get_all_active_tasks()
    today_str = date.today().isoformat()
    cutoff_str = (date.today() + timedelta(days=7)).isoformat()

    def in_window(task: dict) -> bool:
        deadline = task.get("deadline")
        if not deadline:
            return False
        return deadline < today_str or today_str <= deadline <= cutoff_str

    def context_rank(task: dict) -> int:
        ctx = (task.get("context") or "").lower()
        if "personal" in ctx or "🏠" in ctx:
            return 0
        if "work" in ctx or "💼" in ctx:
            return 1
        return 2

    visible = [t for t in tasks if in_window(t)]
    ordered = sorted(
        visible,
        key=lambda t: (context_rank(t), t.get("deadline") or "9999-12-31", t.get("name", "").lower()),
    )
    return ordered[:limit]


def get_recurring_templates() -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={
            "and": [
                {"property": "Recurring", "select":   {"does_not_equal": "None"}},
                {"property": "Done",      "checkbox": {"equals": False}},
            ]
        },
    )
    templates = []
    for page in results.get("results", []):
        p = page["properties"]
        templates.append({
            "page_id":        page["id"],
            "name":           _get_prop(p, "Name",           "title")   or "Untitled",
            "auto_horizon":   _get_prop(p, "Auto Horizon",   "formula") or "🔴 Today",
            "context":        _get_prop(p, "Context",        "select")  or "🏠 Personal",
            "recurring":      _get_prop(p, "Recurring",      "select")  or "None",
            "repeat_day":     _get_prop(p, "Repeat Day",     "select"),
            "last_generated": _get_prop(p, "Last Generated", "date"),
            "deadline":       _get_prop(p, "Deadline",       "date"),
        })
    return templates


def fuzzy_match(query: str, tasks: list[dict]) -> dict | None:
    q = _normalize_task_name(query)
    if not q:
        return None
    exact = next((t for t in tasks if _normalize_task_name(t["name"]) == q), None)
    if exact:
        return exact
    return next(
        (t for t in tasks if q in _normalize_task_name(t["name"]) or _normalize_task_name(t["name"]) in q),
        None,
    )


def find_duplicate_active_task(name: str) -> dict | None:
    return fuzzy_match(name, get_all_active_tasks())


def parse_done_numbers_command(text: str) -> list[int] | None:
    normalized = text.strip().lower()
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


def recover_digest_items_from_text(text: str) -> dict[int, dict]:
    """
    Rebuild digest numbering from a replied digest message so number-based completion
    still works after a bot restart (when in-memory digest_map is empty).
    """
    if not text:
        return {}

    emoji_to_num = {emoji: i + 1 for i, emoji in enumerate(NUMBER_EMOJIS)}
    numbered_names: dict[int, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        n = None
        remainder = ""

        for emoji, value in emoji_to_num.items():
            if line.startswith(f"{emoji} "):
                n = value
                remainder = line[len(emoji):].strip()
                break

        if n is None:
            m = re.match(r"^(\d+)[\.\)]?\s+(.+)$", line)
            if m:
                n = int(m.group(1))
                remainder = m.group(2).strip()

        if n is None or not remainder:
            continue

        # Digest lines are formatted as "<num> <n>  <context>".
        task_name = remainder.split("  ")[0].strip()
        if task_name:
            numbered_names[n] = task_name

    if not numbered_names:
        return {}

    active_tasks = get_all_active_tasks()
    recovered: dict[int, dict] = {}
    for n, name in numbered_names.items():
        matched = fuzzy_match(name, active_tasks)
        if matched:
            recovered[n] = matched
    return recovered


# ══════════════════════════════════════════════════════════════════════════════
# RECURRING LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def should_spawn_today(template: dict, today: date) -> bool:
    recurring  = template["recurring"]
    repeat_day = template["repeat_day"]
    last_gen   = template["last_generated"]
    if last_gen == today.isoformat():
        return False
    if recurring == "🔁 Daily":
        return True
    if recurring == "📅 Weekly":
        if not repeat_day or repeat_day not in REPEAT_DAY_TO_WEEKDAY:
            return False
        return today.weekday() == REPEAT_DAY_TO_WEEKDAY[repeat_day]
    if recurring == "🗓️ Monthly":
        if not repeat_day or repeat_day not in REPEAT_DAY_TO_MONTHDAY:
            return False
        target = REPEAT_DAY_TO_MONTHDAY[repeat_day]
        if target == -1:
            return today.day == calendar.monthrange(today.year, today.month)[1]
        return today.day == target
    return False


def spawn_recurring_instance(template: dict) -> None:
    today = date.today()
    notion.pages.create(
        parent={"database_id": NOTION_DB_ID},
        properties={
            "Name":     {"title":  [{"text": {"content": template["name"]}}]},
            "Deadline": {"date":   {"start": today.isoformat()}},
            "Context":  {"select": {"name": template["context"]}},
            "Source":   {"select": {"name": "✏️ Manual"}},
        },
    )
    set_last_generated(template["page_id"], today)
    log.info(f"Spawned recurring: {template['name']}")


def process_recurring_tasks() -> int:
    today   = date.today()
    spawned = 0
    for t in get_recurring_templates():
        if should_spawn_today(t, today):
            spawn_recurring_instance(t)
            spawned += 1
    return spawned


def handle_done_recurring(page_id: str) -> bool:
    result    = notion.pages.retrieve(page_id=page_id)
    p         = result["properties"]
    recurring = _get_prop(p, "Recurring", "select") or "None"
    if recurring == "None":
        return False
    spawn_recurring_instance({
        "page_id":        page_id,
        "name":           _get_prop(p, "Name",           "title")   or "Untitled",
        "auto_horizon":   _get_prop(p, "Auto Horizon",   "formula") or "🔴 Today",
        "context":        _get_prop(p, "Context",        "select")  or "🏠 Personal",
        "recurring":      recurring,
        "repeat_day":     _get_prop(p, "Repeat Day",     "select"),
        "last_generated": _get_prop(p, "Last Generated", "date"),
        "deadline":       _get_prop(p, "Deadline",       "date"),
    })
    return True


# ══════════════════════════════════════════════════════════════════════════════
# BATCH CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

def _run_capture(raw_text: str, force_create: bool = False,
                 context_override: str | None = None,
                 deadline_override: int | None = None) -> dict:
    try:
        result        = classify_task(raw_text)
        task_name     = result.get("task_name", raw_text)
        deadline_days = result.get("deadline_days")
        ctx           = context_override or result.get("context", "🏠 Personal")
        recurring     = result.get("recurring", "None") or "None"
        repeat_day    = result.get("repeat_day")
        if recurring == "📅 Weekly" and repeat_day in REPEAT_DAY_TO_WEEKDAY:
            if deadline_days is None:
                target        = next_weekday(REPEAT_DAY_TO_WEEKDAY[repeat_day])
                deadline_days = (target - date.today()).days
        if deadline_override is not None:
            deadline_days = deadline_override
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error(f"Claude error for '{raw_text}': {e}")
        return {"status": "error", "name": raw_text, "error": str(e)}

    if not force_create:
        dup = find_duplicate_active_task(task_name)
        if dup:
            return {"status": "duplicate", "name": task_name, "duplicate": dup}

    try:
        page_id = create_task(task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
        return {
            "status": "captured", "name": task_name,
            "horizon_label": horizon_label, "context": ctx,
            "recurring": recurring, "page_id": page_id,
        }
    except Exception as e:
        log.error(f"Notion error for '{task_name}': {e}")
        return {"status": "error", "name": task_name, "error": str(e)}


def format_batch_summary(results: list[dict]) -> str:
    captured   = [r for r in results if r["status"] == "captured"]
    duplicates = [r for r in results if r["status"] == "duplicate"]
    errors     = [r for r in results if r["status"] == "error"]
    lines = []
    if captured:
        groups: dict[tuple, list[dict]] = {}
        for r in captured:
            groups.setdefault((r["horizon_label"], r["context"]), []).append(r)
        lines.append("✅ Captured!")
        for (horizon, ctx), items in groups.items():
            for r in items:
                recur_tag = f"  _{r['recurring']}_" if r.get("recurring", "None") != "None" else ""
                lines.append(f"📝 {r['name']}{recur_tag}")
            lines.append(f"🕐 {horizon}  {ctx}  · _Saved to Notion_")
            lines.append("")
    if duplicates:
        lines.append("⚠️ *Already on your list* (skipped):")
        for r in duplicates:
            dup = r["duplicate"]
            lines.append(f"  · {r['name']}  _{dup.get('auto_horizon','')} {dup.get('context','')}_")
        lines.append("")
    if errors:
        lines.append("❌ *Couldn't capture*:")
        for r in errors:
            lines.append(f"  · {r['name']}")
    return "\n".join(lines).strip()


def create_note_entry(content: str, topic: str | None = None) -> str:
    if not NOTION_NOTES_DB:
        raise ValueError("NOTION_NOTES_DB is not configured")
    base_props = create_note_payload(content, topic=topic)
    db = notion.databases.retrieve(database_id=NOTION_NOTES_DB)
    schema_props = db.get("properties", {})

    def schema_type(prop_name: str) -> str | None:
        return schema_props.get(prop_name, {}).get("type")

    props: dict = {}

    # Map title payload to whichever title property exists in the DB.
    title_payload = base_props.get("Title")
    title_prop_name = next((name for name, p in schema_props.items() if p.get("type") == "title"), None)
    if title_payload and title_prop_name:
        props[title_prop_name] = title_payload

    if "Content" in base_props and schema_type("Content") == "rich_text":
        props["Content"] = base_props["Content"]
    if "Date Created" in base_props and schema_type("Date Created") == "date":
        props["Date Created"] = base_props["Date Created"]
    if "Processed" in base_props and schema_type("Processed") == "checkbox":
        props["Processed"] = base_props["Processed"]
    if "Link" in base_props and schema_type("Link") == "url":
        props["Link"] = base_props["Link"]

    if "Type" in base_props and schema_type("Type") == "select":
        desired = base_props["Type"]["select"]["name"]
        options = schema_props["Type"].get("select", {}).get("options", [])
        names = {o.get("name") for o in options}
        if desired in names:
            props["Type"] = base_props["Type"]
        elif options:
            props["Type"] = {"select": {"name": options[0]["name"]}}

    if "Source" in base_props and schema_type("Source") == "select":
        desired = base_props["Source"]["select"]["name"]
        options = schema_props["Source"].get("select", {}).get("options", [])
        names = {o.get("name") for o in options}
        if desired in names:
            props["Source"] = base_props["Source"]
        elif options:
            props["Source"] = {"select": {"name": options[0]["name"]}}

    if "Topic" in base_props and schema_type("Topic") == "multi_select":
        desired_topics = [t.get("name") for t in base_props["Topic"].get("multi_select", []) if t.get("name")]
        options = schema_props["Topic"].get("multi_select", {}).get("options", [])
        names = {o.get("name") for o in options}
        selected = [{"name": t} for t in desired_topics if t in names]
        # Notion can create missing multi_select options on write, so include any
        # user-provided topics that are not in the current schema yet.
        props["Topic"] = {"multi_select": selected or [{"name": t} for t in desired_topics]}

    if not props:
        raise ValueError("Notes DB schema has no writable matching properties for note payload")

    page = notion.pages.create(parent={"database_id": NOTION_NOTES_DB}, properties=props)
    return page["id"]


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def pending_habits_for_digest(time_str: str | None = None) -> list[dict]:
    habits = habit_cache.values() if time_str is None else habits_by_time(time_str)
    pending: list[dict] = []
    for habit in sorted(habits, key=lambda h: h["sort"]):
        pid = habit["page_id"]
        if already_logged_today(pid):
            continue
        if is_on_pace(habit):
            continue
        pending.append(habit)
    return pending


def format_daily_digest(
    tasks: list[dict],
    habits: list[dict] | None = None,
    weather_mode: str = "today",
) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    habits = habits or []
    if not tasks and not habits:
        return f"☀️ *{date_str}*\n\nAll clear — no tasks or habits pending right now! 🎉", []

    today_str = date.today().isoformat()
    overdue = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now = [t for t in tasks if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    carryover = [t for t in tasks if t not in overdue and t not in today_now]

    lines, ordered, n = [f"☀️ *{date_str}*"], [], 1
    weather_block = format_weather_block(fetch_weather(weather_mode), label="🌤️")
    lines.append(weather_block or weather_unavailable_digest_line())
    lines.append("")

    if overdue:
        lines.append("🚨 *Overdue*")
        for t in overdue:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']}")
            ordered.append(t); n += 1
        lines.append("")

    if today_now:
        lines.append("📌 *Today*")
        for t in today_now:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']}")
            ordered.append(t); n += 1
        lines.append("")

    if carryover:
        lines.append("🔁 *Carry-over (still open)*")
        for t in carryover:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']} · {t['auto_horizon']}")
            ordered.append(t); n += 1
        lines.append("")

    if habits:
        lines.append("⏰ *Reminders*")
        for habit in habits:
            freq_label = habit.get("frequency_label") or ""
            desc = habit.get("description") or ""
            detail = " · ".join(filter(None, [freq_label, desc]))
            lines.append(f"• {habit['name']}" + (f" — _{detail}_" if detail else ""))

    if ordered:
        lines.append("\n_Reply `done 1`, `done 1,3`, `mark 1,3 done`, or `done: task name` to mark complete_")
    return "\n".join(lines), ordered


def format_sunday_intro(week_tasks: list[dict], month_tasks: list[dict]) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    if not week_tasks and not month_tasks:
        return f"🔁 *Weekly Review — {date_str}*\n\nNothing in This Week or This Month — clean slate! 🎉", []
    lines, ordered, n = [f"🔁 *Weekly Review — {date_str}*\n"], [], 1
    if week_tasks:
        lines.append("🟠 *This Week*")
        for t in week_tasks:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
        lines.append("")
    if month_tasks:
        lines.append("🟡 *This Month*")
        for t in month_tasks:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
    lines.append("\n_Tap each item below to reassign its urgency 👇_")
    return "\n".join(lines), ordered


def quick_actions_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS], [BTN_NOTES, BTN_WEATHER, BTN_MUTE]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Type a task, or tap a quick action…",
    )


def notes_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📝 Quick Note", callback_data="nq:quick")],
            [InlineKeyboardButton("💡 Save Idea", callback_data="nq:idea")],
            [InlineKeyboardButton("💻 Save Code", callback_data="nq:code")],
            [InlineKeyboardButton("🔗 Save Link", callback_data="nq:link")],
            [InlineKeyboardButton("❌ Cancel", callback_data="nq:cancel")],
        ]
    )


def mute_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1 day", callback_data="mq:1"),
                InlineKeyboardButton("3 days", callback_data="mq:3"),
                InlineKeyboardButton("7 days", callback_data="mq:7"),
            ],
            [
                InlineKeyboardButton("Status", callback_data="mq:status"),
                InlineKeyboardButton("Unmute", callback_data="mq:unmute"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="mq:cancel")],
        ]
    )


def format_reminder_snapshot(mode: str = "priority", limit: int = 8) -> str:
    today_str = date.today().isoformat()
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    all_tasks = get_all_active_tasks()
    overdue = [t for t in all_tasks if t["deadline"] and t["deadline"] < today_str]
    today_tasks = [t for t in all_tasks if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    quick_refresh_tasks = get_quick_refresh_tasks(limit=max(limit, 10))
    open_count = len(all_tasks)

    if mode == "all_open":
        ordered = quick_refresh_tasks
        header = f"📋 *To Do (Due ≤ 7 Days) — {date_str}*"
    else:
        ordered = quick_refresh_tasks
        header = f"🔔 *Reminder — {date_str}*"

    lines = []

    if mode == "all_open":
        five_day_cutoff = (date.today() + timedelta(days=5)).isoformat()

        def is_personal(task: dict) -> bool:
            ctx = (task.get("context") or "").lower()
            return "personal" in ctx or "🏠" in ctx

        week_focus = [
            t for t in all_tasks
            if t.get("deadline")
            and today_str <= t["deadline"] <= five_day_cutoff
            and (t.get("auto_horizon") == "🔴 Today" or is_personal(t))
        ]
        week_focus = sorted(
            week_focus,
            key=lambda t: (t.get("deadline") or "9999-12-31", t.get("name", "").lower()),
        )

        if week_focus:
            lines.append("🟠 *This Week*")
            for t in week_focus[:5]:
                lines.append(f"{t['name']} | {t['deadline']}")
            lines.append("")

    lines.extend([
        header,
        "",
        f"Open: *{open_count}*  ·  Overdue: *{len(overdue)}*  ·  Today: *{len(today_tasks)}*",
        "",
    ])

    if not ordered:
        lines.append("✅ No Personal/Work tasks due within the next 7 days.")
    else:
        for idx, task in enumerate(ordered[:limit], start=1):
            deadline = f" · due {task['deadline']}" if task.get("deadline") else ""
            lines.append(f"{num_emoji(idx)} {task['name']}  {task['context']} · {task['auto_horizon']}{deadline}")
        if len(ordered) > limit:
            lines.append(f"\n…and *{len(ordered) - limit}* more.")

    lines.append("\n_You can still type normally to add tasks anytime._")
    return "\n".join(lines)


async def send_quick_reminder(message, mode: str = "priority") -> None:
    await message.reply_text(
        format_reminder_snapshot(mode=mode),
        parse_mode="Markdown",
        reply_markup=quick_actions_keyboard(),
    )


# ══════════════════════════════════════════════════════════════════════════════
# INLINE KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════

def _clean_pid(pid: str) -> str: return pid.replace("-", "")
def _restore_pid(pid: str) -> str: return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def review_keyboard(page_id: str) -> InlineKeyboardMarkup:
    p = _clean_pid(page_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Today",      callback_data=f"h:{p}:t"),
         InlineKeyboardButton("🟠 This Week",  callback_data=f"h:{p}:w")],
        [InlineKeyboardButton("🟡 This Month", callback_data=f"h:{p}:m"),
         InlineKeyboardButton("⚪ Backburner",  callback_data=f"h:{p}:b")],
        [InlineKeyboardButton("✅ Done",        callback_data=f"d:{p}")],
    ])


def new_task_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Today",      callback_data=f"nt:{key}:t"),
         InlineKeyboardButton("🟠 This Week",  callback_data=f"nt:{key}:w")],
        [InlineKeyboardButton("🟡 This Month", callback_data=f"nt:{key}:m"),
         InlineKeyboardButton("⚪ Backburner",  callback_data=f"nt:{key}:b")],
    ])


def habit_buttons(habits: list[dict], prefix: str, page: int = 0, page_size: int = 8) -> InlineKeyboardMarkup:
    start = max(0, page) * page_size
    end = start + page_size
    page_habits = habits[start:end]

    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for habit in page_habits:
        p = _clean_pid(habit["page_id"])
        row.append(InlineKeyboardButton(habit["name"], callback_data=f"{prefix}:{p}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if len(habits) > page_size:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"hpag:{prefix}:{page-1}"))
        if end < len(habits):
            nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"hpag:{prefix}:{page+1}"))
        if nav:
            rows.append(nav)

    return InlineKeyboardMarkup(rows)


def done_picker_keyboard(key: str, page: int = 0, page_size: int = 5) -> InlineKeyboardMarkup:
    tasks  = done_picker_map.get(key, [])
    start  = page * page_size
    end    = start + page_size
    rows   = []
    for idx, task in enumerate(tasks[start:end], start=start):
        label = task["name"]
        if len(label) > 28:
            label = label[:25] + "..."
        rows.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"dp:{key}:{idx}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"dpp:{key}:{page-1}"))
    if end < len(tasks):
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"dpp:{key}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("✖️ Cancel", callback_data=f"dpc:{key}")])
    return InlineKeyboardMarkup(rows)


def context_emoji(context: str | None) -> str:
    ctx = (context or "").strip().lower()
    if ctx == "💼 work":
        return "💼"
    if ctx == "🏠 personal":
        return "🏠"
    if ctx == "🏃 health":
        return "🏃"
    if ctx == "🤝 hk":
        return "🤝"
    return "📝"


def todo_picker_keyboard(key: str) -> InlineKeyboardMarkup:
    tasks = todo_picker_map.get(key, [])
    rows: list[list[InlineKeyboardButton]] = []
    for idx, task in enumerate(tasks):
        if task.get("_done"):
            continue
        label = f"{context_emoji(task.get('context'))} {task.get('name', 'Untitled')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"td:{key}:{idx}")])
    return InlineKeyboardMarkup(rows)


# ══════════════════════════════════════════════════════════════════════════════
# CAPTURE ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def complete_task_by_page_id(message, page_id: str, name: str) -> None:
    mark_done(page_id)
    suffix = "\n↻ Next instance created" if handle_done_recurring(page_id) else ""
    await message.reply_text(f"✅ Done: {name}{suffix}")


async def create_or_prompt_task(message, raw_text: str, force_create: bool = False) -> None:
    global _pending_counter
    task_texts = split_tasks(raw_text)
    is_multi   = len(task_texts) > 1
    thinking   = await message.reply_text(
        f"🧠 Classifying {len(task_texts)} tasks..." if is_multi else "🧠 Classifying..."
    )

    if is_multi:
        overrides = infer_batch_overrides(raw_text)
        context_override = overrides.get("context")
        deadline_override = overrides.get("deadline_days")
        loop    = asyncio.get_event_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, _run_capture, t, force_create, context_override, deadline_override)
            for t in task_texts
        ])
        await thinking.edit_text(format_batch_summary(list(results)), parse_mode="Markdown")
        return

    try:
        result        = classify_task(raw_text)
        task_name     = result.get("task_name", raw_text)
        deadline_days = result.get("deadline_days")
        ctx           = result.get("context", "🏠 Personal")
        confidence    = result.get("confidence", "low")
        recurring     = result.get("recurring", "None") or "None"
        repeat_day    = result.get("repeat_day")
        if recurring == "📅 Weekly" and repeat_day in REPEAT_DAY_TO_WEEKDAY:
            if deadline_days is None:
                target        = next_weekday(REPEAT_DAY_TO_WEEKDAY[repeat_day])
                deadline_days = (target - date.today()).days
        horizon_label = deadline_days_to_label(deadline_days)
    except Exception as e:
        log.error(f"Claude error: {e}")
        await thinking.edit_text("⚠️ Couldn't classify that. Try rephrasing?")
        return

    if not force_create:
        dup = find_duplicate_active_task(task_name)
        if dup:
            await thinking.edit_text(
                f"⚠️ Already on your list:\n\n📝 {dup['name']}\n🕐 {dup.get('auto_horizon','')}  {dup.get('context','')}\n\nSend `force: {task_name}` to add anyway.",
                parse_mode="Markdown",
            )
            return

    recur_tag = f"\n🔁 {recurring}" if recurring != "None" else ""

    if confidence == "high":
        try:
            page_id = create_task(task_name, deadline_days, ctx, recurring=recurring, repeat_day=repeat_day)
            await thinking.edit_text(
                f"✅ Captured!\n\n📝 {task_name}\n🕐 {horizon_label}  {ctx}{recur_tag}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            capture_map[thinking.message_id] = {"page_id": page_id, "name": task_name}
        except Exception as e:
            log.error(f"Notion error: {e}")
            await thinking.edit_text("⚠️ Classified but couldn't write to Notion.")
    else:
        key = str(_pending_counter); _pending_counter += 1
        pending_map[key] = {"name": task_name, "context": ctx, "recurring": recurring, "repeat_day": repeat_day}
        await thinking.edit_text(
            f"📝 *{task_name}*  {ctx}{recur_tag}\n\nWhen should this happen?",
            parse_mode="Markdown",
            reply_markup=new_task_keyboard(key),
        )


async def open_done_picker(message) -> None:
    global _done_picker_counter
    tasks = get_today_and_overdue_tasks()
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
        reply_markup=habit_buttons(pending_habits, "hl"),
    )


async def cmd_refresh(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    load_habit_cache()
    if _scheduler is not None:
        register_habit_schedules(_scheduler, message.get_bot())
        build_digest_schedule(_scheduler, message.get_bot())
    await send_daily_digest(message.get_bot(), include_habits=True)
    if _scheduler is not None:
        build_digest_schedule(_scheduler, message.get_bot())


async def cmd_todo(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    global _todo_picker_counter
    if message.chat_id != MY_CHAT_ID:
        return
    tasks = get_today_and_overdue_tasks()
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


async def cmd_notes_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await message.reply_text(
        "📝 Notes options:",
        reply_markup=notes_options_keyboard(),
    )


async def cmd_weather_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    del context
    if message.chat_id != MY_CHAT_ID:
        return
    await message.reply_text(format_weather_snapshot(), parse_mode="Markdown")


async def cmd_mute_text(message, context: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    if message.chat_id != MY_CHAT_ID:
        return
    if context is not None:
        context.user_data["awaiting_mute_days"] = False
    await message.reply_text(
        "🔕 Mute options for scheduled digests:",
        reply_markup=mute_options_keyboard(),
    )


async def handle_v10_callback(q, parts: list[str]) -> bool:
    if parts[0] == "wl_save" and len(parts) == 2:
        key = parts[1]
        if key not in pending_wantslist_map:
            await q.edit_message_text("⚠️ This confirmation expired — please re-send.")
            return True
        item_data = pending_wantslist_map.pop(key)
        try:
            create_wantslist_entry(item_data["item"], category=item_data["category"])
            await q.edit_message_text(
                f"🎁 Saved!\n\n*{item_data['item']}*\n_{item_data['category']} · Wantslist_\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Wantslist save error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return True

    if parts[0] == "wl_cancel" and len(parts) == 2:
        pending_wantslist_map.pop(parts[1], None)
        await q.edit_message_text("🎁 Cancelled — not saved.")
        return True

    if parts[0] == "tmdb_pick" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in pending_tmdb_map:
            await q.edit_message_text("⚠️ This picker expired — please re-send.")
            return True
        candidates = pending_tmdb_map.pop(key)
        try:
            c = candidates[int(idx_str)]
            _save_watchlist_from_candidate(c, c["title"])
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
        candidates = pending_tmdb_map.pop(key, [])
        title = candidates[0]["title"] if candidates else "Unknown"
        try:
            create_watchlist_entry(title)
            await q.edit_message_text(
                f"📺 Added!\n\n*{title}*\n_Title only — no TMDB metadata_\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Watchlist title-only save error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return True

    if parts[0] == "tmdb_cancel" and len(parts) == 2:
        pending_tmdb_map.pop(parts[1], None)
        await q.edit_message_text("📺 Cancelled — not saved.")
        return True

    return False


async def route_classified_message_v10(message, text: str) -> None:
    if await handle_photo_followup(message, text):
        return

    thinking = await message.reply_text("🧠 Got it...")
    try:
        result = classify_message_v10(text)
    except Exception as e:
        log.error(f"Claude v10 classify error: {e}")
        await thinking.delete()
        await create_or_prompt_task(message, text)
        return

    intent = result.get("type")

    if intent == "watchlist":
        await thinking.delete()
        await handle_watchlist_intent(message, title=result.get("title", text), media_type=result.get("media_type", "Series"))
        return

    if intent == "wantslist":
        await thinking.delete()
        await handle_wantslist_intent(message, item=result.get("item", text), category=result.get("category", "Other"))
        return

    if intent == "photo":
        await thinking.delete()
        await handle_photo_intent(message, subject=result.get("subject", text))
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
        else:
            all_habits = [{"page_id": h["page_id"], "name": name} for name, h in habit_cache.items()]
            all_habits.sort(key=lambda h: h["name"].lower())
            await thinking.edit_text("Which habit did you complete?", reply_markup=habit_buttons(all_habits, "hl"))
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
    "📝 notes": cmd_notes_text,
    "🌤️ weather": cmd_weather_text,
    "🔕 mute": cmd_mute_text,
}


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def handle_message_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return
    message = update.message
    text    = (message.text or "").strip()
    if not text:
        return
    lower = text.lower().strip()

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
        if set_location_smart(text):
            context.user_data["awaiting_location"] = False
            await message.reply_text(f"📍 Location updated to {current_location}.")
        else:
            await message.reply_text(
                "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
            )
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
            create_note_entry(entry["content"], custom_topic)
            topic_recency_map[custom_topic] = datetime.utcnow()
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
            create_note_entry(text)
            kind_label_map = {
                "quick": "note",
                "idea": "idea",
                "code": "code snippet",
                "link": "link",
            }
            kind_label = kind_label_map.get(awaiting_note_capture, "note")
            await message.reply_text(
                f"✅ {kind_label.capitalize()} saved to Notes.",
                reply_markup=quick_actions_keyboard(),
            )
        except Exception as e:
            log.error("fn=handle_message_text event=note_quick_save_failed err=%s", e)
            await reply_notion_error(message, "save note")
        finally:
            context.user_data["awaiting_note_capture"] = None
        return

    # Quick-action Notes button (ReplyKeyboard sends plain text "📝 Notes")
    if lower in ("📝 notes", "notes"):
        notes_pending.add(update.effective_chat.id)
        await message.reply_text(
            "📒 *Notes* — send me a link or type a note:",
            parse_mode="Markdown",
        )
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
                    mark_done(pid)
                    suffix = " ↻ next queued" if handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")
        elif message.reply_to_message:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            recovered = recover_digest_items_from_text(replied_text)
            for n in numbers:
                task = recovered.get(n)
                if task:
                    pid = task["page_id"]
                    name = task["name"]
                    mark_done(pid)
                    suffix = " ↻ next queued" if handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")

        if done_names:
            msg = "Marked done:\n" + "\n".join(f"✅ {n}" for n in done_names)
            await message.reply_text(msg)
        else:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    match_name = re.match(r"done:\s*(.+)$", text, re.IGNORECASE)
    if match_name:
        matched = fuzzy_match(match_name.group(1).strip(), get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_name.group(1).strip()}\".")
        return

    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        matched = fuzzy_match(match_mark_done.group(1).strip(), get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_mark_done.group(1).strip()}\".")
        return

    match_focus = re.match(r"focus:\s*(.+)$", text, re.IGNORECASE)
    if match_focus:
        matched = fuzzy_match(match_focus.group(1).strip(), get_all_active_tasks())
        if matched:
            set_focus(matched["page_id"], True)
            await message.reply_text(f"🎯 Focused: {matched['name']} → *Doing*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_focus.group(1).strip()}\".")
        return

    match_unfocus = re.match(r"unfocus:\s*(.+)$", text, re.IGNORECASE)
    if match_unfocus:
        matched = fuzzy_match(match_unfocus.group(1).strip(), get_all_active_tasks())
        if matched:
            set_focus(matched["page_id"], False)
            await message.reply_text(f"⬜ Unfocused: {matched['name']} → *To Do*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_unfocus.group(1).strip()}\".")
        return

    match_force = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if match_force:
        await create_or_prompt_task(message, match_force.group(1).strip(), force_create=True); return

    await route_classified_message_v10(message, text)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q     = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if await handle_v10_callback(q, parts):
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
            await q.edit_message_text(mute_status_text())
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
            create_note_entry(entry["content"], selected_topic)
            if selected_topic:
                topic_recency_map[selected_topic] = datetime.utcnow()
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

    if parts[0] in ("hl", "hc") and len(parts) == 2:
        habit_page_id = _restore_pid(parts[1])
        habit_name    = next((n for n, h in habit_cache.items() if h["page_id"] == habit_page_id), "Unknown")
        try:
            if already_logged_today(habit_page_id):
                await q.edit_message_text(f"Already logged {habit_name} today! ✅")
            else:
                log_habit(habit_page_id, habit_name)
                await q.edit_message_text(f"✅ {habit_name} logged!")
        except Exception as e:
            log.error(f"Habit log error: {e}"); await q.edit_message_text("⚠️ Couldn't log to Notion.")
        return

    if parts[0] == "hpag" and len(parts) == 3:
        _, prefix, page_str = parts
        all_habits = get_active_habits_for_trigger()
        try:
            await q.edit_message_reply_markup(
                reply_markup=habit_buttons(all_habits, prefix, page=int(page_str))
            )
        except Exception as e:
            log.error(f"Habit pagination error: {e}")
            await q.edit_message_text("⚠️ Couldn't update habits view.")
        return

    if parts[0] == "nt" and len(parts) == 3:
        _, key, code = parts
        if key not in pending_map:
            await q.edit_message_text("⚠️ This task expired — please re-send it."); return
        task          = pending_map.pop(key)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        days          = HORIZON_DEADLINE_OFFSETS.get(code)
        recurring     = task.get("recurring", "None")
        repeat_day    = task.get("repeat_day")
        dup = find_duplicate_active_task(task["name"])
        if dup:
            await q.edit_message_text(
                f"⚠️ Already on your list:\n\n📝 {dup['name']}\n🕐 {dup.get('auto_horizon','')}  {dup.get('context','')}\n\nSend `force: {task['name']}` to add anyway.",
                parse_mode="Markdown",
            ); return
        recur_tag = f"\n🔁 {recurring}" if recurring != "None" else ""
        try:
            page_id = create_task(task["name"], days, task["context"], recurring=recurring, repeat_day=repeat_day)
            await q.edit_message_text(
                f"✅ Captured!\n\n📝 {task['name']}\n🕐 {horizon_label}  {task['context']}{recur_tag}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            capture_map[q.message.message_id] = {"page_id": page_id, "name": task["name"]}
        except Exception as e:
            log.error(f"Notion error: {e}"); await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return

    if parts[0] == "d" and len(parts) == 2:
        page_id = _restore_pid(parts[1])
        try:
            mark_done(page_id)
            suffix = "\n↻ Next instance created" if handle_done_recurring(page_id) else ""
            await q.edit_message_text(f"✅ Marked as done!{suffix}")
        except Exception as e:
            log.error(f"Notion done error: {e}"); await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "h" and len(parts) == 3:
        _, pid_clean, code = parts
        page_id       = _restore_pid(pid_clean)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        try:
            set_deadline_from_horizon_code(page_id, code)
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
            mark_done(task["page_id"])
            handle_done_recurring(task["page_id"])
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
            mark_done(task["page_id"])
            suffix = "\n↻ Next instance created" if handle_done_recurring(task["page_id"]) else ""
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
                    mark_done(task["page_id"])
                    handle_done_recurring(task["page_id"])
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
                reply_markup=format_command_palette(),
            )
            return

        if action == "habits":
            await q.edit_message_text("🎯 Loading habits…")
            await send_daily_habits_list(q.bot)
            return

        if action == "notes":
            if NOTION_NOTES_DB:
                await q.edit_message_text("📝 Notes connected. Choose an option:", reply_markup=notes_options_keyboard())
            else:
                await q.edit_message_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return

        if action == "weather":
            await q.edit_message_text(format_weather_snapshot(), parse_mode="Markdown")
            return

        if action == "mute":
            await q.edit_message_text(
                "🔕 Choose a mute option:",
                reply_markup=mute_options_keyboard(),
            )
            return

    if parts[0] == "qv" and len(parts) == 2 and parts[1] in {"week", "backlog"}:
        try:
            message, ordered = format_week_view(parts[1])
            await q.edit_message_text(
                text=message,
                parse_mode="Markdown",
                reply_markup=horizon_view_back_keyboard(),
            )
            if ordered and q.message:
                digest_map[q.message.message_id] = ordered
        except Exception as e:
            log.error("Quick-view callback error (%s): %s", q.data, e)
            await q.edit_message_text("⚠️ Couldn't load that view right now.")
        return

    if q.data == "digest:today":
        try:
            tasks = get_today_and_overdue_tasks()
            message, ordered = format_hybrid_digest(tasks)
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
    if is_muted():
        log.info("Recurring check skipped (muted)")
        return
    load_habit_cache()
    spawned = process_recurring_tasks()
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
    config = await get_digest_config(slot["time"], slot["is_weekday"])
    if config.get("contexts") is None and config.get("include_habits") is False:
        return
    await send_daily_digest(bot, include_habits=slot["include_habits"], config=config)


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


def build_digest_schedule(scheduler, bot) -> int:
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

    for slot in slots:
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


async def send_daily_digest(bot, include_habits: bool = True, config: dict | None = None) -> None:
    global last_digest_msg_id
    if is_muted():
        log.info("Daily digest skipped (muted)")
        return
    tasks = _filter_digest_tasks(get_today_and_overdue_tasks(limit=None), config=config)
    today_str = date.today().isoformat()
    overdue = [t for t in tasks if t.get("deadline") and t["deadline"] < today_str]
    today_tasks = [t for t in tasks if t not in overdue and t.get("auto_horizon") == "🔴 Today"]
    this_week_tasks = [t for t in tasks if t not in overdue and t not in today_tasks]
    ordered = overdue + today_tasks + this_week_tasks
    max_items = config.get("max_items") if config else None
    if isinstance(max_items, int):
        ordered = ordered[:max_items]
        overdue = [t for t in ordered if t.get("deadline") and t["deadline"] < today_str]
        today_tasks = [t for t in ordered if t not in overdue and t.get("auto_horizon") == "🔴 Today"]
        this_week_tasks = [t for t in ordered if t not in overdue and t not in today_tasks]

    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    lines = [f"☀️ *{date_str}*", ""]
    weather_block = format_weather_block(fetch_weather("today"), label="🌤️")
    location_label = digest_location_label()
    if weather_block and location_label:
        lines.append(f"{weather_block} · 📍{location_label}")
    else:
        lines.append(weather_block or weather_unavailable_digest_line())
    lines.append("")
    n = 1

    habits: list[dict] = []
    habits_enabled = include_habits
    if config and config.get("include_habits") is not None:
        habits_enabled = bool(config.get("include_habits"))
    if habits_enabled:
        habits = get_active_habits_for_trigger()

    if overdue:
        lines.append("🚨 *Overdue*")
        for task in overdue:
            lines.append(f"{num_emoji(n)}{context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if today_tasks:
        lines.append("📌 *Today*")
        for task in today_tasks:
            lines.append(f"{num_emoji(n)}{context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if this_week_tasks:
        lines.append("📅 *This Week*")
        for task in this_week_tasks:
            lines.append(f"{num_emoji(n)}{context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    message = "\n".join(lines).strip()
    sent_digest = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=None,
    )

    if habits:
        habit_text = "🌅 *Morning habits* — tap to log:\n\n"
        for h in habits[:5]:
            habit_text += f"⏰ {h['time_str']} — {h['name']}\n"
        if len(habits) > 5:
            habit_text += f"\n_+{len(habits) - 5} more_"
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text=habit_text.rstrip(),
            parse_mode="Markdown",
            reply_markup=habit_buttons(habits, "hc"),
        )

    if ordered:
        digest_map[sent_digest.message_id] = ordered
    last_digest_msg_id = sent_digest.message_id
    log.info("Consolidated daily digest sent — %d tasks, %d habits", len(ordered), len(habits))


async def send_evening_checkin(bot) -> None:
    """Evening habit check-in with time display and frequency status."""
    evening_habits = get_active_habits_for_trigger()
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
        reply_markup=habit_buttons(evening_habits, "hc"),
    )
    log.info("Evening check-in sent — %d habits", len(evening_habits))


async def send_sunday_review(bot) -> None:
    if is_muted():
        log.info("Sunday review skipped (muted)")
        return
    await send_daily_digest(bot, include_habits=True, config=None)
    week_tasks  = query_tasks_by_auto_horizon(["🟠 This Week"])
    month_tasks = query_tasks_by_auto_horizon(["🟡 This Month"])
    header, ordered = format_sunday_intro(week_tasks, month_tasks)
    await bot.send_message(chat_id=MY_CHAT_ID, text=header, parse_mode="Markdown")
    for n, task in enumerate(ordered, 1):
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text=f"{num_emoji(n)} *{task['name']}*  {task['context']}\n_Currently: {task['auto_horizon']}_",
            parse_mode="Markdown",
            reply_markup=review_keyboard(task["page_id"]),
        )
    log.info(f"Sunday review sent — {len(ordered)} items")


async def send_habit_reminder(bot, time_str: str) -> None:
    global last_digest_msg_id
    if is_muted():
        log.info("Habit reminder skipped (muted)")
        return
    habits = get_active_habits_for_trigger()
    if not habits:
        return

    tasks = get_today_and_overdue_tasks()
    try:
        weekday = datetime.now(TZ).weekday() < 5
        config = await get_digest_config(time_str, weekday)
    except Exception:
        config = None
    if config and (config.get("contexts") is not None or config.get("max_items") is not None):
        tasks = _filter_digest_tasks(tasks, config=config)
        if isinstance(config.get("max_items"), int):
            tasks = tasks[:config["max_items"]]
    message, ordered = format_daily_digest(tasks, habits, weather_mode="current")
    sent = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=habit_buttons(habits, "hc"),
    )
    if ordered:
        digest_map[sent.message_id] = ordered
        last_digest_msg_id = sent.message_id
    log.info(f"Combined digest sent at {time_str} — {len(ordered)} tasks, {len(habits)} habits")


async def send_daily_habits_list(bot) -> None:
    """Fetch all active habits for today and send as clickable buttons."""
    habits = get_active_habits_for_trigger()
    if not habits:
        await bot.send_message(chat_id=MY_CHAT_ID, text="🎯 No habits for today.")
        return

    await bot.send_message(
        chat_id=MY_CHAT_ID,
        text="🎯 *Daily habits* — tap to log:",
        parse_mode="Markdown",
        reply_markup=habit_buttons(habits, "hc"),
    )
    log.info("Habits list sent — %s available habits", len(habits))


def register_habit_schedules(scheduler: AsyncIOScheduler, bot) -> None:
    for job in _habit_jobs:
        try:
            job.remove()
        except Exception:
            pass
    _habit_jobs.clear()

    times_seen = set()
    for habit in habit_cache.values():
        time_str = habit.get("time")
        if not time_str or time_str in times_seen:
            continue
        times_seen.add(time_str)
        try:
            h, m = map(int, time_str.split(":"))
            job = scheduler.add_job(
                send_habit_reminder, "cron",
                hour=h, minute=m,
                args=[bot, time_str],
                id=f"habit_{time_str}",
            )
            _habit_jobs.append(job)
            log.info(f"Registered habit reminder job at {time_str}")
        except Exception as e:
            log.error(f"Failed to register habit job for {time_str}: {e}")


async def run_asana_sync(bot) -> None:
    """
    Bi-directional Asana ↔ Notion reconcile.
    Offloads blocking I/O to thread pool so Telegram event loop stays responsive.
    Self-contained: does not touch Telegram, does not read habit_cache.
    """
    if not ASANA_PAT:
        return  # Sync disabled — bot still works without Asana

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
                asana_workspace_gid=ASANA_WORKSPACE_GID,   # v9.2: required for my_tasks mode
                source_mode=ASANA_SYNC_SOURCE,
                archive_orphans=ASANA_ARCHIVE_ORPHANS,
            ),
        )
        # Only log when something happened — keeps logs readable at 15s polling
        if any(v for k, v in stats.items() if k != "skipped"):
            log.info(f"Asana sync: {stats}")
        sync_status["asana"]["ok"] = True
        sync_status["asana"]["error"] = None
        sync_status["asana"]["stats"] = stats
    except AsanaSyncError as e:
        log.error(f"Asana sync config error: {e}")
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)
    except Exception as e:
        log.exception(f"Asana sync failed: {e}")
        sync_status["asana"]["ok"] = False
        sync_status["asana"]["error"] = str(e)


async def run_cinema_sync(bot) -> None:
    """Daily sync for Cinema Log → Favourite Shows."""
    if not CINEMA_DB_ID:
        return

    sync_status["cinema"]["last_run"] = utc_now_iso()
    try:
        stats = await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id=CINEMA_DB_ID,
            fave_db_id=FAVE_DB_ID,
            tmdb_api_key=TMDB_API_KEY,
        )
        log.info(
            "Cinema sync: new=%s, tmdb_found=%s, tmdb_missing=%s, added_to_fave=%s",
            stats["new_entries"],
            stats["tmdb_found"],
            stats["tmdb_missing"],
            stats["added_to_fave"],
        )
        if stats["new_entries"] > 0:
            await _try_send_telegram(
                bot,
                f"📺 Cinema Sync Report\n\n"
                f"Entries processed: {stats['new_entries']}\n"
                f"✅ TMDB URLs filled: {stats['tmdb_found']}\n"
                f"⭐ Added to Favourite Shows: {stats['added_to_fave']}\n"
                f"⚠️ TMDB not found: {stats['tmdb_missing']}",
            )
        sync_status["cinema"]["ok"] = True
        sync_status["cinema"]["error"] = None
        sync_status["cinema"]["stats"] = stats
    except Exception as e:
        log.exception("Cinema sync failed: %s", e)
        sync_status["cinema"]["ok"] = False
        sync_status["cinema"]["error"] = str(e)


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
            habits_out.append({
                "id":          habit["page_id"],
                "name":        habit["name"],
                "color":       habit.get("color") or "pink",
                "description": habit.get("description") or "",
                "frequency":   habit.get("frequency_label") or "",
                "sort":        habit.get("sort"),
                "days":        days,
                "todayDone":   days[-1] == 1,
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
            headers=_cors_headers(),
        )
    except Exception as e:
        log.error(f"/habits-data error: {e}")
        return web.Response(status=500, text=str(e), headers=_cors_headers())


def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


async def log_habit_http_handler(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=_cors_headers())

    try:
        body = await request.json()
        habit_id = (body.get("habitId") or "").strip()
        if not habit_id:
            return web.Response(
                status=400,
                text=json.dumps({"ok": False, "error": "habitId is required"}),
                content_type="application/json",
                headers=_cors_headers(),
            )

        matched = next((h for h in habit_cache.values() if h["page_id"] == habit_id), None)
        if not matched:
            return web.Response(
                status=404,
                text=json.dumps({"ok": False, "error": "Habit not found"}),
                content_type="application/json",
                headers=_cors_headers(),
            )

        if already_logged_today(matched["page_id"]):
            return web.Response(
                text=json.dumps({"ok": True, "alreadyLogged": True, "habitName": matched["name"]}),
                content_type="application/json",
                headers=_cors_headers(),
            )

        log_habit(matched["page_id"], matched["name"], source="🌐 HabitKit")
        return web.Response(
            text=json.dumps({"ok": True, "alreadyLogged": False, "habitName": matched["name"]}),
            content_type="application/json",
            headers=_cors_headers(),
        )
    except Exception as e:
        log.error(f"/log-habit error: {e}")
        return web.Response(
            status=500,
            text=json.dumps({"ok": False, "error": str(e)}),
            content_type="application/json",
            headers=_cors_headers(),
        )


async def start_http_server() -> None:
    app    = web.Application()
    app.router.add_get("/habits-data", habits_data_handler)
    app.router.add_post("/log-habit", log_habit_http_handler)
    app.router.add_options("/log-habit", log_habit_http_handler)
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
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
    """Fail fast if required Notion databases are unreachable."""
    dbs = {
        "NOTION_DB_ID": NOTION_DB_ID,
        "NOTION_HABIT_DB": NOTION_HABIT_DB,
        "NOTION_LOG_DB": NOTION_LOG_DB,
        "NOTION_NOTES_DB": NOTION_NOTES_DB,
        "NOTION_DIGEST_SELECTOR_DB": NOTION_DIGEST_SELECTOR_DB,
        "NOTION_WATCHLIST_DB": NOTION_WATCHLIST_DB,
    }
    for label, db_id in dbs.items():
        if not db_id:
            log.warning("startup_health_check fn=startup_notion_health_check db=%s status=skipped_empty", label)
            continue
        try:
            notion_call(notion.databases.retrieve, database_id=db_id)
            log.info("startup_health_check fn=startup_notion_health_check db=%s status=ok", label)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Startup health check failed for {label} ({db_id}): {exc}") from exc


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    global _scheduler
    startup_notion_health_check()
    load_mute_state()
    load_location_state()
    if OPENWEATHER_KEY and (current_lat is None or current_lon is None):
        set_location_smart(current_location)
    load_habit_cache()
    await start_http_server()
    scheduler = AsyncIOScheduler(timezone=TZ)
    if FEATURES.get("FEATURE_RECURRING", True):
        scheduler.add_job(run_recurring_check, "cron", hour=_rc_h, minute=_rc_m, args=[app.bot])
    if FEATURES.get("FEATURE_SUNDAY_REVIEW", True):
        scheduler.add_job(send_sunday_review, "cron", day_of_week="sun", hour=_we_h, minute=_we_m, args=[app.bot])
    if FEATURES.get("FEATURE_HABITS", True):
        register_habit_schedules(scheduler, app.bot)
    build_digest_schedule(scheduler, app.bot)
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
        fetch_weather_cache,
        "cron",
        day_of_week="mon-fri",
        hour=(_wk_h if _wk_m >= 3 else (_wk_h - 1) % 24),
        minute=(_wk_m - 3) % 60,
        args=[app.bot],
        id="weather_prefetch_weekday",
    )
    scheduler.add_job(
        fetch_weather_cache,
        "cron",
        day_of_week="sat,sun",
        hour=(_we_h if _we_m >= 3 else (_we_h - 1) % 24),
        minute=(_we_m - 3) % 60,
        args=[app.bot],
        id="weather_prefetch_weekend",
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
                    loop = asyncio.get_event_loop()
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
                max_instances=1,                   # Skip a tick if previous still running
                coalesce=True,                     # Don't backfill missed runs
                next_run_time=datetime.now(TZ),    # Fire once immediately on startup
            )
            asana_status = f"ON ({ASANA_SYNC_INTERVAL}s, mode={ASANA_SYNC_SOURCE})"
            log.info("Notion schema validation passed ✓")

    # ── Cinema sync — config validation + daily schedule ──
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
            cinema_sync_hour=CINEMA_SYNC_HOUR,
            cinema_sync_minute=CINEMA_SYNC_MINUTE,
            sync_buffer_minutes=SYNC_BUFFER_MINUTES,
            tz=TZ,
            now_fn=datetime.now,
        )
        log.info(
            "Cinema sync jobs registered (daily %02d:%02d UTC + every %d minutes)",
            CINEMA_SYNC_HOUR,
            CINEMA_SYNC_MINUTE,
            SYNC_BUFFER_MINUTES,
        )

    scheduler.start()
    _scheduler = scheduler
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"weekday={_wk_h:02d}:{_wk_m:02d}  weekend={_we_h:02d}:{_we_m:02d}  "
        f"afternoon=15:00  "
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
    tasks = get_today_and_overdue_tasks()
    if not pending_habits and not tasks:
        await update.message.reply_text("✅ Everything done for today — nothing left to log!")
        return
    if pending_habits:
        await update.message.reply_text(
            "🏃 *Which habit did you complete?*",
            parse_mode="Markdown",
            reply_markup=habit_buttons(pending_habits, "hl"),
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
            reply_markup=quick_actions_keyboard(),
        )
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
        reply_markup=quick_actions_keyboard(),
    )


async def handle_remind_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/r and /remind — quick to-do reminder snapshot."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await send_quick_reminder(update.message, mode="priority")


async def handle_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/sync — manual catch-up trigger for core sync pipelines."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    status = await update.message.reply_text("🔄 Running full sync (Asana + Cinema + Habit cache)…")
    try:
        load_habit_cache()
        await run_asana_sync(context.bot)
        await run_cinema_sync(context.bot)
        await status.edit_text("✅ Full sync finished.")
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
    """Prompt for a new weather location."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    context.user_data["awaiting_location"] = True
    await update.message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/weather — show current + upcoming forecast snapshot."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await update.message.reply_text(format_weather_snapshot(), parse_mode="Markdown")


async def cmd_notes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/notes — open note capture shortcuts and show connection status."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    if NOTION_NOTES_DB:
        await update.message.reply_text("📝 Notes connected. Choose an option:", reply_markup=notes_options_keyboard())
    else:
        await update.message.reply_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")


async def cmd_habits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/habits — show incomplete habits as one-tap check-ins."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    await send_daily_habits_list(context.bot)


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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_text))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info(f"🤖 Second Brain bot starting ({APP_VERSION})...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
