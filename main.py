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
from datetime import date, datetime, timedelta
from collections import defaultdict

import pytz
from aiohttp import web
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
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
    CINEMA_ENABLED,
    CINEMA_DB_ID,
    FAVE_DB_ID,
    TMDB_API_KEY,
    CINEMA_SYNC_HOUR,
    CINEMA_SYNC_MINUTE,
    validate_config as validate_cinema_config,
)

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

TZ           = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
_wk_h, _wk_m = _parse_hhmm_env("DIGEST_TIME_WEEKDAY", "8:15")
_we_h, _we_m = _parse_hhmm_env("DIGEST_TIME_WEEKEND", "12:00")
_rc_h, _rc_m = _parse_hhmm_env("RECURRING_CHECK_TIME", "7:00")

CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))
HTTP_PORT      = int(os.environ.get("PORT", "8080"))
WEEKS_HISTORY  = int(os.environ.get("WEEKS_HISTORY", "52"))
APP_VERSION    = os.environ.get("APP_VERSION", "v10.0.0")
SYNC_BUFFER_MINUTES = max(1, int(os.environ.get("SYNC_BUFFER_MINUTES", "5")))

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
pending_map: dict[str, dict] = {}
capture_map: dict[int, dict] = {}
done_picker_map: dict[str, list[dict]] = {}
pending_wantslist_map: dict[str, dict] = {}
pending_photo_map: dict[str, dict] = {}
pending_tmdb_map: dict[str, list[dict]] = {}
_pending_counter = 0
_done_picker_counter = 0
_v10_counter = 0
habit_cache: dict[str, dict] = {}
_tmdb_http_client: httpx.AsyncClient | None = None
sync_status: dict[str, dict] = {
    "asana": {"last_run": None, "ok": None, "error": None, "stats": None},
    "cinema": {"last_run": None, "ok": None, "error": None, "stats": None},
}

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
BTN_REFRESH = "🔄 Refresh"
BTN_ALL_OPEN = "📋 All Open"
BTN_PRIORITY = "🔥 Priority"


def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    today = date.today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


def _utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


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

    enabled_intents = ["habit", "task"]
    if watchlist_enabled:
        enabled_intents.append("watchlist")
    if wantslist_enabled:
        enabled_intents.append("wantslist")
    if photo_enabled:
        enabled_intents.append("photo")

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

HABIT — user saying they completed a recurring habit RIGHT NOW.
  Signals: "did", "took", "went to", "had", "completed" + habit name

TASK — something to be done in the future (default if nothing else matches).

If confidence is low on watchlist/wantslist/photo, return task instead.
"Watch:" prefix = always watchlist, high confidence.
"want:" prefix = always wantslist, high confidence.
"photo:" prefix = always photo, high confidence.

Return ONLY valid JSON, no markdown:

If WATCHLIST:
{{"type": "watchlist", "title": "clean title only, no year", "media_type": "Series|Film|Anime|Documentary", "confidence": "high|low"}}

If WANTSLIST:
{{"type": "wantslist", "item": "clean item name", "category": "Tech|Home|Clothes|Health|Other", "confidence": "high|low"}}

If PHOTO:
{{"type": "photo", "subject": "clean scene/subject description", "confidence": "high|low"}}

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
    props: dict = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Type": {"select": {"name": media_type}},
        "Status": {"select": {"name": "Queued"}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Added": {"date": {"start": date.today().isoformat()}},
    }
    if tmdb_id:
        props["TMDB ID"] = {"rich_text": [{"text": {"content": tmdb_id}}]}
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
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
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
        for p in results.get("results", [])
    ]


def get_today_and_overdue_tasks() -> list[dict]:
    tasks     = get_all_active_tasks()
    today_str = date.today().isoformat()
    selected  = []
    for t in tasks:
        is_today    = t["auto_horizon"] == "🔴 Today"
        is_overdue  = bool(t["deadline"] and t["deadline"] < today_str)
        is_carryover = t["auto_horizon"] in {"🟠 This Week", "🟡 This Month"}
        if is_today or is_overdue or is_carryover:
            selected.append(t)
    overdue = [t for t in selected if t["deadline"] and t["deadline"] < today_str]
    today_only = [t for t in selected if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    carryover = [
        t for t in selected
        if t not in overdue and t not in today_only and t["auto_horizon"] in {"🟠 This Week", "🟡 This Month"}
    ]
    return overdue + today_only + carryover


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


def format_daily_digest(tasks: list[dict], habits: list[dict] | None = None) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    habits = habits or []
    if not tasks and not habits:
        return f"☀️ *{date_str}*\n\nAll clear — no tasks or habits pending right now! 🎉", []

    today_str = date.today().isoformat()
    overdue = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now = [t for t in tasks if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    carryover = [t for t in tasks if t not in overdue and t not in today_now]

    lines, ordered, n = [f"☀️ *{date_str}*\n"], [], 1

    if overdue:
        lines.append("🚨 *Overdue*")
        for t in overdue:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
        lines.append("")

    if today_now:
        lines.append("📌 *Today*")
        for t in today_now:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
        lines.append("")

    if carryover:
        lines.append("🔁 *Carry-over (still open)*")
        for t in carryover:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']} · {t['auto_horizon']}")
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
        [[BTN_REFRESH, BTN_ALL_OPEN, BTN_PRIORITY]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Type a task, or tap a quick action…",
    )


def format_reminder_snapshot(mode: str = "priority", limit: int = 8) -> str:
    today_str = date.today().isoformat()
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    all_tasks = get_all_active_tasks()
    overdue = [t for t in all_tasks if t["deadline"] and t["deadline"] < today_str]
    today_tasks = [t for t in all_tasks if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    priority_tasks = get_today_and_overdue_tasks()
    open_count = len(all_tasks)

    if mode == "all_open":
        ordered = (
            sorted(overdue, key=lambda t: (t.get("deadline") or "", t["name"]))
            + sorted(today_tasks, key=lambda t: t["name"])
            + sorted(
                [t for t in all_tasks if t not in overdue and t not in today_tasks],
                key=lambda t: (t["auto_horizon"], t["name"]),
            )
        )
        header = f"📋 *All Open Tasks — {date_str}*"
    else:
        ordered = priority_tasks
        header = f"🔔 *Reminder — {date_str}*"

    lines = [
        header,
        "",
        f"Open: *{open_count}*  ·  Overdue: *{len(overdue)}*  ·  Today: *{len(today_tasks)}*",
        "",
    ]

    if not ordered:
        lines.append("✅ Nothing urgent right now.")
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


def habit_buttons(habits: list[dict], prefix: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for habit in habits:
        p = _clean_pid(habit["page_id"])
        row.append(InlineKeyboardButton(habit["name"], callback_data=f"{prefix}:{p}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
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
            all_habits = sorted(habit_cache.values(), key=lambda h: h["sort"])
            await thinking.edit_text("Which habit did you complete?", reply_markup=habit_buttons(all_habits, "hl"))
        return

    await thinking.delete()
    await create_or_prompt_task(message, text)


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return
    message = update.message
    text    = (message.text or "").strip()
    if not text:
        return
    lower = text.lower().strip()

    if text == BTN_REFRESH:
        await send_quick_reminder(message, mode="priority")
        return
    if text == BTN_ALL_OPEN:
        await send_quick_reminder(message, mode="all_open")
        return
    if text == BTN_PRIORITY:
        await send_quick_reminder(message, mode="priority")
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

    if lower == "done":
        await open_done_picker(message); return

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

    if looks_like_task_batch(text):
        await create_or_prompt_task(message, text)
        return

    await route_classified_message_v10(message, text)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q     = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    if await handle_v10_callback(q, parts):
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


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ══════════════════════════════════════════════════════════════════════════════

async def run_recurring_check(bot) -> None:
    load_habit_cache()
    spawned = process_recurring_tasks()
    log.info(f"Recurring check: {spawned} task(s) spawned")


async def send_daily_digest(bot) -> None:
    global last_digest_msg_id
    tasks = get_today_and_overdue_tasks()
    habits = pending_habits_for_digest()
    message, ordered = format_daily_digest(tasks, habits)
    reply_markup = habit_buttons(habits, "hc") if habits else None
    sent = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )
    if ordered:
        digest_map[sent.message_id] = ordered
        last_digest_msg_id = sent.message_id
    log.info(f"Daily digest sent — {len(ordered)} tasks, {len(habits)} habits")


async def send_sunday_review(bot) -> None:
    await send_daily_digest(bot)
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
    habits = pending_habits_for_digest(time_str)
    if not habits:
        return

    tasks = get_today_and_overdue_tasks()
    message, ordered = format_daily_digest(tasks, habits)
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


def register_habit_schedules(scheduler: AsyncIOScheduler, bot) -> None:
    times_seen = set()
    for habit in habit_cache.values():
        time_str = habit.get("time")
        if not time_str or time_str in times_seen:
            continue
        times_seen.add(time_str)
        try:
            h, m = map(int, time_str.split(":"))
            scheduler.add_job(
                send_habit_reminder, "cron",
                hour=h, minute=m,
                args=[bot, time_str],
                id=f"habit_{time_str}",
            )
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
    sync_status["asana"]["last_run"] = _utc_now_iso()
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
    if not CINEMA_ENABLED or not CINEMA_DB_ID or not FAVE_DB_ID:
        return

    sync_status["cinema"]["last_run"] = _utc_now_iso()
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
    ]
    return "  ".join(flags)


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    load_habit_cache()
    await start_http_server()
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(run_recurring_check, "cron",
                      hour=_rc_h, minute=_rc_m, args=[app.bot])
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="mon-fri", hour=_wk_h, minute=_wk_m, args=[app.bot])
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="sat", hour=_we_h, minute=_we_m, args=[app.bot])
    scheduler.add_job(send_sunday_review, "cron",
                      day_of_week="sun", hour=_we_h, minute=_we_m, args=[app.bot])
    register_habit_schedules(scheduler, app.bot)

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
    elif CINEMA_ENABLED and CINEMA_DB_ID and FAVE_DB_ID:
        scheduler.add_job(
            run_cinema_sync,
            "cron",
            hour=CINEMA_SYNC_HOUR,
            minute=CINEMA_SYNC_MINUTE,
            args=[app.bot],
            id="cinema_sync",
        )
        scheduler.add_job(
            run_cinema_sync,
            "interval",
            minutes=SYNC_BUFFER_MINUTES,
            args=[app.bot],
            id="cinema_sync_buffer",
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(TZ) + timedelta(minutes=SYNC_BUFFER_MINUTES),
        )
        log.info(
            "Cinema sync jobs registered (daily %02d:%02d UTC + every %d minutes)",
            CINEMA_SYNC_HOUR,
            CINEMA_SYNC_MINUTE,
            SYNC_BUFFER_MINUTES,
        )

    scheduler.start()
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"weekday={_wk_h:02d}:{_wk_m:02d}  weekend={_we_h:02d}:{_we_m:02d}  "
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
            "👋 *Second Brain Bot*\n\nSend me any task or habit to capture it.\nUse /done to mark completions.\nUse /r or /remind for your quick snapshot.",
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


def _fmt_sync_block(name: str, info: dict) -> str:
    ok = info.get("ok")
    if ok is True:
        state = "✅ OK"
    elif ok is False:
        state = "❌ Failed"
    else:
        state = "— Not yet run"
    last_run = info.get("last_run") or "n/a"
    error = info.get("error")
    stats = info.get("stats")
    lines = [f"*{name}*: {state}", f"last_run: `{last_run}`"]
    if stats:
        lines.append(f"stats: `{json.dumps(stats, separators=(',', ':'))}`")
    if error:
        lines.append(f"error: `{error}`")
    return "\n".join(lines)


async def handle_sync_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/syncstatus — show latest sync telemetry for Asana + Cinema."""
    if update.effective_chat.id != MY_CHAT_ID:
        return
    msg = [
        "📊 *Sync Status*",
        "",
        _fmt_sync_block("Asana", sync_status["asana"]),
        "",
        _fmt_sync_block("Cinema", sync_status["cinema"]),
    ]
    await update.message.reply_text("\n".join(msg), parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — after all handlers are defined
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    from telegram.ext import CommandHandler
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", handle_start_command))
    app.add_handler(CommandHandler("r", handle_remind_command))
    app.add_handler(CommandHandler("remind", handle_remind_command))
    app.add_handler(CommandHandler("sync", handle_sync_command))
    app.add_handler(CommandHandler("syncstatus", handle_sync_status_command))
    app.add_handler(CommandHandler("done",  handle_done_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info("🤖 Second Brain bot starting (v9.2)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
