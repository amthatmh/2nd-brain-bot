#!/usr/bin/env python3
"""
Second Brain — Telegram Bot (v7)
- AI task capture into Notion
- Duplicate guard
- Reply-to-done support
- Bare `done` opens a completion picker for Today + overdue tasks
- `done 1,3`, `done: task name`, and `mark ... done` supported
- Multi-task capture: bullet lists (-, •, 1.) or multi-line messages
  are split, classified concurrently, and reported in a single summary
- Habit logging via relation to 🎯 Habits database
- Morning/evening habit check-in buttons with digest
- `focus:` / `unfocus:` Telegram commands

v7 changes (habit layer added on top of v6):
- load_habit_cache() fetches active habits from 🎯 Habits DB at startup
- log_habit() creates entries in 📅 Habit Log with relation field
- already_logged_today() prevents duplicate habit logs
- Morning habits appended to daily digest with tap buttons
- Evening check-in scheduled separately (19:00 weekdays, 20:00 weekends)
- classify_message() detects habit vs task before routing
"""

import asyncio
import os
import json
import re
import logging
import calendar
from datetime import date, datetime, timedelta

import pytz
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import anthropic
from notion_client import Client as NotionClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]          # 🆕 To-Do
NOTION_HABIT_DB = os.environ["NOTION_HABIT_DB"]    # 🎯 Habits
NOTION_LOG_DB = os.environ["NOTION_LOG_DB"]        # 📅 Habit Log

TZ = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
_wk_h, _wk_m = map(int, os.environ.get("DIGEST_TIME_WEEKDAY", "8:15").split(":"))
_we_h, _we_m = map(int, os.environ.get("DIGEST_TIME_WEEKEND", "12:00").split(":"))
_rc_h, _rc_m = map(int, os.environ.get("RECURRING_CHECK_TIME", "7:00").split(":"))
_ev_wk_h, _ev_wk_m = map(int, os.environ.get("EVENING_CHECK_WEEKDAY", "19:00").split(":"))
_ev_we_h, _ev_we_m = map(int, os.environ.get("EVENING_CHECK_WEEKEND", "20:00").split(":"))

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "200"))

# ── Clients ──────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ── In-memory state ──────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = {}
last_digest_msg_id: int | None = None
pending_map: dict[str, dict] = {}
capture_map: dict[int, dict] = {}
done_picker_map: dict[str, list[dict]] = {}
_pending_counter = 0
_done_picker_counter = 0

# Habit cache: name → page_id (refreshed at startup + nightly)
habit_cache: dict[str, str] = {}

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


def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    today = date.today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


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
            title_parts = page["properties"].get("Habit", {}).get("title", [])
            name = title_parts[0]["text"]["content"] if title_parts else None
            if name:
                habit_cache[name] = page["id"]
        log.info(f"Habit cache loaded: {list(habit_cache.keys())}")
    except Exception as e:
        log.error(f"Failed to load habit cache: {e}")


def get_habits_by_time(time_filter: str) -> list[dict]:
    try:
        results = notion.databases.query(
            database_id=NOTION_HABIT_DB,
            filter={
                "and": [
                    {"property": "Active", "checkbox": {"equals": True}},
                    {"property": "Time",   "select":   {"equals": time_filter}},
                ]
            },
        )
        habits = []
        for page in results.get("results", []):
            title_parts = page["properties"].get("Habit", {}).get("title", [])
            name = title_parts[0]["text"]["content"] if title_parts else "Unknown"
            habits.append({"page_id": page["id"], "name": name})
        return habits
    except Exception as e:
        log.error(f"get_habits_by_time error: {e}")
        return []


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


# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def classify_message(text: str) -> dict:
    """
    First determines if message is a habit log or task.
    Returns {type: 'habit'|'task', ...fields}
    """
    habit_names = list(habit_cache.keys())
    prompt = f"""You are a personal assistant classifier for a second brain system.
Today is {date.today().strftime("%A, %B %-d, %Y")}.

Message: "{text}"

Active habits to detect: {habit_names}
Workout types that count as 💪 Workout: soccer, crossfit, hyrox, rowing, snowboard, skiing, gym, run, jog, trained

First determine if this is:
A) HABIT LOG — person saying they completed something NOW (e.g. "took creatine", "did workout", "went to crossfit", "had protein shake")
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


def classify_task(text: str) -> dict:
    """Task-only classification for multi-task batch processing."""
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
# NOTION — HABIT LOG
# ══════════════════════════════════════════════════════════════════════════════

def log_habit(habit_page_id: str, habit_name: str) -> None:
    today = date.today().isoformat()
    notion.pages.create(
        parent={"database_id": NOTION_LOG_DB},
        properties={
            "Entry":     {"title":    [{"text": {"content": f"{habit_name} — {today}"}}]},
            "Habit":     {"relation": [{"id": habit_page_id}]},
            "Completed": {"checkbox": True},
            "Date":      {"date":     {"start": today}},
            "Source":    {"select":   {"name": "📱 Telegram"}},
        },
    )
    log.info(f"Habit logged: {habit_name} on {today}")


def already_logged_today(habit_page_id: str) -> bool:
    today = date.today().isoformat()
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
    tasks = get_all_active_tasks()
    today_str = date.today().isoformat()
    selected = []
    for t in tasks:
        is_today   = t["auto_horizon"] == "🔴 Today"
        is_overdue = bool(t["deadline"] and t["deadline"] < today_str)
        if is_today or is_overdue:
            selected.append(t)
    overdue    = [t for t in selected if t["deadline"] and t["deadline"] < today_str]
    today_only = [t for t in selected if t not in overdue]
    return overdue + today_only


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
    today = date.today()
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

def _run_capture(raw_text: str, force_create: bool = False) -> dict:
    try:
        result       = classify_task(raw_text)
        task_name    = result.get("task_name", raw_text)
        deadline_days = result.get("deadline_days")
        ctx          = result.get("context", "🏠 Personal")
        recurring    = result.get("recurring", "None") or "None"
        repeat_day   = result.get("repeat_day")
        if recurring == "📅 Weekly" and repeat_day in REPEAT_DAY_TO_WEEKDAY:
            if deadline_days is None:
                target = next_weekday(REPEAT_DAY_TO_WEEKDAY[repeat_day])
                deadline_days = (target - date.today()).days
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

def format_daily_digest(tasks: list[dict]) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    if not tasks:
        return f"☀️ *{date_str}*\n\nAll clear — no tasks due today! 🎉", []
    today_str = date.today().isoformat()
    overdue   = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now = [t for t in tasks if t not in overdue]
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
        loop    = asyncio.get_event_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, _run_capture, t, force_create)
            for t in task_texts
        ])
        await thinking.edit_text(format_batch_summary(list(results)), parse_mode="Markdown")
        return

    # Single task
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

    # 1. Reply `done` to a capture message
    if lower == "done" and message.reply_to_message:
        replied_id = message.reply_to_message.message_id
        if replied_id in capture_map:
            captured = capture_map[replied_id]
            await complete_task_by_page_id(message, captured["page_id"], captured["name"])
            return
        if replied_id in digest_map:
            await message.reply_text("Reply with `done 1` or `done 1,3`, or use `done: task name`.", parse_mode="Markdown")
            return

    # 2. bare `done` → picker
    if lower == "done":
        await open_done_picker(message); return

    # 3. done/mark/complete + number list
    numbers = parse_done_numbers_command(text)
    if numbers:
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        if source_id and source_id in digest_map:
            items, done_names = digest_map[source_id], []
            for n in numbers:
                if 1 <= n <= len(items):
                    pid  = items[n - 1]["page_id"]
                    name = items[n - 1]["name"]
                    mark_done(pid)
                    suffix = " ↻ next queued" if handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")
            msg = "Marked done:\n" + "\n".join(f"✅ {n}" for n in done_names) if done_names else "Couldn't find those items."
            await message.reply_text(msg)
        else:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    # 4. done: task name
    match_name = re.match(r"done:\s*(.+)$", text, re.IGNORECASE)
    if match_name:
        matched = fuzzy_match(match_name.group(1).strip(), get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_name.group(1).strip()}\".")
        return

    # 5. mark ... done
    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        matched = fuzzy_match(match_mark_done.group(1).strip(), get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_mark_done.group(1).strip()}\".")
        return

    # 6. focus: / unfocus:
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

    # 7. force:
    match_force = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if match_force:
        await create_or_prompt_task(message, match_force.group(1).strip(), force_create=True); return

    # 8. Classify: habit or task
    thinking = await message.reply_text("🧠 Got it...")
    try:
        result = classify_message(text)
    except Exception as e:
        log.error(f"Claude error: {e}")
        await thinking.edit_text("⚠️ Couldn't process that. Try rephrasing?")
        return

    if result.get("type") == "habit":
        habit_name = result.get("habit_name")
        confidence = result.get("confidence", "low")
        if habit_name and habit_name in habit_cache and confidence == "high":
            habit_page_id = habit_cache[habit_name]
            if already_logged_today(habit_page_id):
                await thinking.edit_text(f"Already logged {habit_name} today! ✅")
            else:
                log_habit(habit_page_id, habit_name)
                await thinking.edit_text(f"✅ Logged!\n\n{habit_name}\n📅 {date.today().strftime('%B %-d')}")
        else:
            all_habits = [{"page_id": pid, "name": name} for name, pid in habit_cache.items()]
            await thinking.edit_text(
                "Which habit did you complete?",
                reply_markup=habit_buttons(all_habits, "hl"),
            )
        return

    # Task path — hand off to existing capture flow
    await thinking.delete()
    await create_or_prompt_task(message, text)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q     = update.callback_query
    await q.answer()
    parts = q.data.split(":")

    # Habit log button (from ambiguous prompt or check-in)
    if parts[0] in ("hl", "hc") and len(parts) == 2:
        habit_page_id = _restore_pid(parts[1])
        habit_name    = next((n for n, pid in habit_cache.items() if pid == habit_page_id), "Unknown")
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
    load_habit_cache()  # Refresh habit cache daily
    spawned = process_recurring_tasks()
    log.info(f"Recurring check: {spawned} task(s) spawned")


async def send_daily_digest(bot, include_habits: bool = True) -> None:
    global last_digest_msg_id
    tasks            = get_today_and_overdue_tasks()
    message, ordered = format_daily_digest(tasks)
    sent = await bot.send_message(chat_id=MY_CHAT_ID, text=message, parse_mode="Markdown")
    if ordered:
        digest_map[sent.message_id] = ordered
        last_digest_msg_id          = sent.message_id

    if include_habits:
        morning_habits = get_habits_by_time("🌅 Morning") + get_habits_by_time("🕐 Anytime")
        if morning_habits:
            await bot.send_message(
                chat_id=MY_CHAT_ID,
                text="🌅 *Morning habits* — tap to log:",
                parse_mode="Markdown",
                reply_markup=habit_buttons(morning_habits, "hc"),
            )
    log.info(f"Daily digest sent — {len(ordered)} tasks")


async def send_evening_checkin(bot) -> None:
    evening_habits = get_habits_by_time("🌙 Evening") + get_habits_by_time("🕐 Anytime")
    if not evening_habits:
        return
    await bot.send_message(
        chat_id=MY_CHAT_ID,
        text="🌙 *Evening check-in* — did you do these today?",
        parse_mode="Markdown",
        reply_markup=habit_buttons(evening_habits, "hc"),
    )
    log.info(f"Evening check-in sent — {len(evening_habits)} habits")


async def send_sunday_review(bot) -> None:
    await send_daily_digest(bot, include_habits=True)
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


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    load_habit_cache()

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(run_recurring_check, "cron",
                      hour=_rc_h, minute=_rc_m, args=[app.bot])
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="mon-fri", hour=_wk_h, minute=_wk_m,
                      args=[app.bot], kwargs={"include_habits": True})
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="sat", hour=_we_h, minute=_we_m,
                      args=[app.bot], kwargs={"include_habits": True})
    scheduler.add_job(send_sunday_review, "cron",
                      day_of_week="sun", hour=_we_h, minute=_we_m, args=[app.bot])
    scheduler.add_job(send_evening_checkin, "cron",
                      day_of_week="mon-fri", hour=_ev_wk_h, minute=_ev_wk_m, args=[app.bot])
    scheduler.add_job(send_evening_checkin, "cron",
                      day_of_week="sat,sun", hour=_ev_we_h, minute=_ev_we_m, args=[app.bot])
    scheduler.start()
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"weekday={_wk_h:02d}:{_wk_m:02d}  weekend={_we_h:02d}:{_we_m:02d}  "
        f"evening_wk={_ev_wk_h:02d}:{_ev_wk_m:02d}  evening_we={_ev_we_h:02d}:{_ev_we_m:02d}"
    )


def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info("🤖 Second Brain bot starting (v7)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
