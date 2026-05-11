#!/usr/bin/env python3
"""Deployment entrypoint for Second Brain bot.

v5 changes:
- Removed `Horizon` field; horizon is computed by `Auto Horizon` formula.
- `Status` is now a Notion formula. All select writes removed.
- `mark_done()` only writes the `Done` checkbox.
- Sunday review buttons write a Deadline date instead of a Horizon select.
- Added `focus:` / `unfocus:` Telegram commands.

v6 changes:
- Multi-task detection via `split_tasks()`: bullet markers (-, -, *, 1.)
  or multiple non-empty lines are each treated as a separate task.
- All tasks in a batch are classified concurrently via asyncio + executor.
- Results are grouped by (horizon, context) and formatted as one summary.
- Low-confidence multi-tasks default to Backburner (no picker spam).
- Single-task low-confidence flow unchanged (shows horizon picker).
"""

import asyncio
import os
import json
import re
import logging
import calendar
import threading
from datetime import date, datetime, timedelta

import pytz
from dotenv import load_dotenv
from flask import Flask, request, jsonify
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
from health.steps import parse_log_date as _steps_parse_date, upsert_steps
from health.metrics import parse_log_date as _metrics_parse_date, upsert_metrics, METRIC_MAP

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]

TZ = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
_wk_h, _wk_m = map(int, os.environ.get("DIGEST_TIME_WEEKDAY", "8:15").split(":"))
_we_h, _we_m = map(int, os.environ.get("DIGEST_TIME_WEEKEND", "12:00").split(":"))
_rc_h, _rc_m = map(int, os.environ.get("RECURRING_CHECK_TIME", "7:00").split(":"))

CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOK = int(os.environ.get("CLAUDE_MAX_TOKENS", "150"))
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]

# ── Clients ──────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ── Flask web server (Health Auto Export webhooks) ───────────────────────────
flask_app = Flask(__name__)


def _authorized() -> bool:
    return request.headers.get("Authorization") == f"Bearer {WEBHOOK_SECRET}"


@flask_app.route("/api/v1/steps-sync", methods=["POST"])
def steps_sync():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload or "data" not in payload:
        return jsonify({"error": "missing 'data' key"}), 400

    metrics = payload["data"]
    if not metrics:
        return jsonify({"error": "empty data array"}), 400

    try:
        first_entry = metrics[0]["data"][0]
        log_date = _steps_parse_date(first_entry["date"])
        steps = int(first_entry["qty"])
    except (KeyError, IndexError, ValueError) as exc:
        log.warning("steps-sync: malformed payload — %s", exc)
        return jsonify({"error": "malformed payload"}), 400

    log.info("steps-sync: received  date=%s  steps=%d", log_date, steps)

    try:
        outcome = upsert_steps(log_date, steps)
    except Exception as exc:
        log.error("steps-sync: Notion error — %s", exc, exc_info=True)
        return jsonify({"error": "notion write failed"}), 500

    return jsonify({"status": outcome, "date": log_date.isoformat(), "steps": steps}), 200


@flask_app.route("/api/v1/health-sync", methods=["POST"])
def health_sync():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload or "data" not in payload:
        return jsonify({"error": "missing 'data' key"}), 400

    raw_metrics = payload["data"]
    if not raw_metrics:
        return jsonify({"error": "empty data array"}), 400

    # Determine the log date from the first data point of the first metric.
    try:
        first_date_str = raw_metrics[0]["data"][0]["date"]
        log_date = _metrics_parse_date(first_date_str)
    except (KeyError, IndexError, ValueError) as exc:
        log.warning("health-sync: malformed payload — %s", exc)
        return jsonify({"error": "malformed payload"}), 400

    received_names = [m.get("name", "<unknown>") for m in raw_metrics]
    log.info("health-sync: received  date=%s  metrics=%s", log_date, received_names)

    # Map incoming metric names to Notion property names; skip unknowns.
    metrics: dict[str, float] = {}
    for entry in raw_metrics:
        name = entry.get("name", "")
        notion_prop = METRIC_MAP.get(name)
        if notion_prop is None:
            log.warning("health-sync: unknown metric '%s' — skipping", name)
            continue
        try:
            value = float(entry["data"][0]["qty"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            log.warning("health-sync: could not read qty for '%s' — %s", name, exc)
            continue
        metrics[notion_prop] = value
        log.info("health-sync: %s = %s", notion_prop, value)

    if not metrics:
        return jsonify({"error": "no recognised metrics in payload"}), 400

    try:
        outcome = upsert_metrics(log_date, metrics)
    except Exception as exc:
        log.error("health-sync: Notion error — %s", exc, exc_info=True)
        return jsonify({"error": "notion write failed"}), 500

    return jsonify({
        "status": outcome,
        "date": log_date.isoformat(),
        "written": list(metrics.keys()),
    }), 200


# ── In-memory state ──────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = {}
last_digest_msg_id: int | None = None
pending_map: dict[str, dict] = {}
capture_map: dict[int, dict] = {}
done_picker_map: dict[str, list[dict]] = {}
_pending_counter = 0
_done_picker_counter = 0

# ── Constants ────────────────────────────────────────────────────────────────
HORIZON_DEADLINE_OFFSETS = {
    "t": 0,
    "w": 6,
    "m": 30,
    "b": None,
}
HORIZON_LABELS = {
    "t": "🔴 Today",
    "w": "🟠 This Week",
    "m": "🟡 This Month",
    "b": "⚪ Backburner",
}

NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
REPEAT_DAY_TO_WEEKDAY = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}
REPEAT_DAY_TO_MONTHDAY = {"1st": 1, "5th": 5, "10th": 10, "15th": 15, "20th": 20, "25th": 25, "Last": -1}

# Matches bullet/number prefixes: "- ", "• ", "* ", "1. ", "2) ", "3: " etc.
_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)


def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    """
    Return the next occurrence of a weekday (0=Mon ... 6=Sun) from today.
    If today IS that weekday, returns next week's occurrence (not today),
    so "every Friday" captured on a Friday means next Friday.
    """
    today = date.today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


# ══════════════════════════════════════════════════════════════════════════════
# MULTI-TASK PARSING
# ══════════════════════════════════════════════════════════════════════════════

def split_tasks(text: str) -> list[str]:
    """
    Split a message into individual task strings.

    Rules (in priority order):
    1. If any line has a bullet/number prefix → strip markers, keep marked lines.
    2. If 2+ non-empty lines exist with no bullets → treat each line as a task.
    3. Single line → return as-is (single task, normal flow).

    Examples:
      "- Buy milk\n- Call dentist"     → ["Buy milk", "Call dentist"]
      "1. Buy milk\n2. Call dentist"   → ["Buy milk", "Call dentist"]
      "Buy milk\nCall dentist"         → ["Buy milk", "Call dentist"]
      "Buy milk"                       → ["Buy milk"]
    """
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

def classify_task(text: str) -> dict:
    prompt = f"""You are a personal task classifier for a second brain system.
Today is {date.today().strftime("%A, %B %-d, %Y")}.

Message: \"{text}\"

Your job is to extract the ACTUAL TASK from the message — the thing the person needs to DO.
Strip away scheduling language, app instructions, and meta-words.

Examples of task extraction:
- "Add recurring tasks on every Friday to text my grandpa" → task_name: "Text grandpa"
- "Make Brain absorb multiple to-do messages tomorrow" → task_name: "Process multiple to-do messages in Brain"
- "Remind me to call dentist this week" → task_name: "Call dentist"
- "Buy milk today" → task_name: "Buy milk"

Return ONLY valid JSON, no markdown, no explanation:
{{
  "task_name": "clean concise action — the thing to DO, not meta-instructions",
  "deadline_days": <integer days from today, or null if no urgency>,
  "context": "one of exactly: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab",
  "confidence": "high or low",
  "recurring": "one of exactly: None | 🔁 Daily | 📅 Weekly | 🗓️ Monthly",
  "repeat_day": "one of: Mon | Tue | Wed | Thu | Fri | Sat | Sun | 1st | 5th | 10th | 15th | 20th | 25th | Last — or null if not recurring/not specified"
}}

deadline_days rules:
- today/tonight/now/urgent/ASAP/by EOD → 0
- tomorrow → 1
- this week/in a few days → 5
- this month/next few weeks → 20
- someday/eventually/no urgency → null
- For recurring tasks: deadline_days = days until the NEXT occurrence (e.g. "every Friday" → days until next Friday)
- NO time signal at all → null and confidence "low"

recurring rules:
- "every day / daily" → 🔁 Daily, repeat_day: null
- "every Monday / each Tuesday / weekly on Wed" → 📅 Weekly, repeat_day: the day
- "every month / monthly / on the 1st" → 🗓️ Monthly, repeat_day: the date
- no recurrence signal → None, repeat_day: null

context rules:
- meetings/clients/projects/reports → 💼 Work
- gym/doctor/dentist/food/workout → 🏃 Health
- family/friends/home/errands → 🏠 Personal
- collaborations/shared tasks → 🤝 Collab"""

    resp = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


def deadline_days_to_label(days: int | None) -> str:
    if days is None:
        return "⚪ Backburner"
    if days <= 0:
        return "🔴 Today"
    if days <= 7:
        return "🟠 This Week"
    if days <= 31:
        return "🟡 This Month"
    return "⚪ Backburner"


# ══════════════════════════════════════════════════════════════════════════════
# NOTION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _deadline_prop(days: int | None) -> dict:
    if days is None:
        return {"date": None}
    return {"date": {"start": (date.today() + timedelta(days=days)).isoformat()}}


def create_task(name: str, deadline_days: int | None, context: str,
                recurring: str = "None", repeat_day: str | None = None) -> str:
    props = {
        "Name": {"title": [{"text": {"content": name}}]},
        "Deadline": _deadline_prop(deadline_days),
        "Context": {"select": {"name": context}},
        "Source": {"select": {"name": "📱 Telegram"}},
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
            "page_id": page["id"],
            "name": _get_prop(p, "Name", "title") or "Untitled",
            "auto_horizon": _get_prop(p, "Auto Horizon", "formula") or "",
            "context": _get_prop(p, "Context", "select") or "",
            "deadline": _get_prop(p, "Deadline", "date"),
        })
    return tasks


def get_all_active_tasks() -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={"property": "Done", "checkbox": {"equals": False}},
    )
    return [
        {
            "page_id": p["id"],
            "name": _get_prop(p["properties"], "Name", "title") or "Untitled",
            "auto_horizon": _get_prop(p["properties"], "Auto Horizon", "formula") or "",
            "context": _get_prop(p["properties"], "Context", "select") or "",
            "deadline": _get_prop(p["properties"], "Deadline", "date"),
        }
        for p in results.get("results", [])
    ]


def get_today_and_overdue_tasks() -> list[dict]:
    tasks = get_all_active_tasks()
    today_str = date.today().isoformat()
    selected = []
    for t in tasks:
        is_today = t["auto_horizon"] == "🔴 Today"
        is_overdue = bool(t["deadline"] and t["deadline"] < today_str)
        if is_today or is_overdue:
            selected.append(t)
    overdue = [t for t in selected if t["deadline"] and t["deadline"] < today_str]
    today_only = [t for t in selected if t not in overdue]
    return overdue + today_only


def get_recurring_templates() -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={
            "and": [
                {"property": "Recurring", "select": {"does_not_equal": "None"}},
                {"property": "Done", "checkbox": {"equals": False}},
            ]
        },
    )
    templates = []
    for page in results.get("results", []):
        p = page["properties"]
        templates.append({
            "page_id": page["id"],
            "name": _get_prop(p, "Name", "title") or "Untitled",
            "auto_horizon": _get_prop(p, "Auto Horizon", "formula") or "🔴 Today",
            "context": _get_prop(p, "Context", "select") or "🏠 Personal",
            "recurring": _get_prop(p, "Recurring", "select") or "None",
            "repeat_day": _get_prop(p, "Repeat Day", "select"),
            "last_generated": _get_prop(p, "Last Generated", "date"),
            "deadline": _get_prop(p, "Deadline", "date"),
        })
    return templates


def fuzzy_match(query: str, tasks: list[dict]) -> dict | None:
    q = _normalize_task_name(query)
    if not q:
        return None
    exact = next((t for t in tasks if _normalize_task_name(t["name"]) == q), None)
    if exact:
        return exact
    return next((t for t in tasks if q in _normalize_task_name(t["name"]) or _normalize_task_name(t["name"]) in q), None)


def find_duplicate_active_task(name: str) -> dict | None:
    return fuzzy_match(name, get_all_active_tasks())


# ══════════════════════════════════════════════════════════════════════════════
# RECURRING LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def should_spawn_today(template: dict, today: date) -> bool:
    recurring = template["recurring"]
    repeat_day = template["repeat_day"]
    last_gen = template["last_generated"]
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
            "Name": {"title": [{"text": {"content": template["name"]}}]},
            "Deadline": {"date": {"start": today.isoformat()}},
            "Context": {"select": {"name": template["context"]}},
            "Source": {"select": {"name": "✏️ Manual"}},
        },
    )
    set_last_generated(template["page_id"], today)
    log.info(f"Spawned recurring: {template['name']}")


def process_recurring_tasks() -> int:
    today = date.today()
    templates = get_recurring_templates()
    spawned = 0
    for t in templates:
        if should_spawn_today(t, today):
            spawn_recurring_instance(t)
            spawned += 1
    return spawned


def handle_done_recurring(page_id: str) -> bool:
    result = notion.pages.retrieve(page_id=page_id)
    p = result["properties"]
    recurring = _get_prop(p, "Recurring", "select") or "None"
    if recurring == "None":
        return False
    spawn_recurring_instance({
        "page_id": page_id,
        "name": _get_prop(p, "Name", "title") or "Untitled",
        "auto_horizon": _get_prop(p, "Auto Horizon", "formula") or "🔴 Today",
        "context": _get_prop(p, "Context", "select") or "🏠 Personal",
        "recurring": recurring,
        "repeat_day": _get_prop(p, "Repeat Day", "select"),
        "last_generated": _get_prop(p, "Last Generated", "date"),
        "deadline": _get_prop(p, "Deadline", "date"),
    })
    return True


# ══════════════════════════════════════════════════════════════════════════════
# BATCH CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

def _run_capture(raw_text: str, force_create: bool = False) -> dict:
    """
    Classify and create a single task synchronously.
    Designed to be called via run_in_executor for concurrent batches.

    Returns a result dict:
      status: "captured" | "duplicate" | "error"
      + relevant fields per status
    """
    try:
        result = classify_task(raw_text)
        task_name = result.get("task_name", raw_text)
        deadline_days = result.get("deadline_days")
        ctx = result.get("context", "🏠 Personal")
        recurring = result.get("recurring", "None") or "None"
        repeat_day = result.get("repeat_day")  # e.g. "Fri", "1st", or None

        # For weekly recurring tasks, compute deadline as next occurrence
        # if Claude didn't already give a specific deadline_days
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
            "status": "captured",
            "name": task_name,
            "horizon_label": horizon_label,
            "context": ctx,
            "recurring": recurring,
            "page_id": page_id,
        }
    except Exception as e:
        log.error(f"Notion error for '{task_name}': {e}")
        return {"status": "error", "name": task_name, "error": str(e)}


def format_batch_summary(results: list[dict]) -> str:
    """
    Format a multi-task capture into a single summary message.
    Groups by (horizon_label, context). Recurring tasks get a 🔁 tag.
    """
    captured = [r for r in results if r["status"] == "captured"]
    duplicates = [r for r in results if r["status"] == "duplicate"]
    errors = [r for r in results if r["status"] == "error"]

    lines = []

    if captured:
        groups: dict[tuple, list[dict]] = {}
        for r in captured:
            key = (r["horizon_label"], r["context"])
            groups.setdefault(key, []).append(r)

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
            lines.append(f"  · {r['name']}  _{dup.get('auto_horizon', '')} {dup.get('context', '')}_")
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
    overdue = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now = [t for t in tasks if t not in overdue]
    lines, ordered, n = [f"☀️ *{date_str}*\n"], [], 1

    if overdue:
        lines.append("🚨 *Overdue*")
        for t in overdue:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t)
            n += 1
        lines.append("")

    if today_now:
        lines.append("📌 *Today*")
        for t in today_now:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t)
            n += 1

    lines.append("\n_Reply `done 1`, `done 1,3`, or `done: task name` to mark complete_")
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
            ordered.append(t)
            n += 1
        lines.append("")

    if month_tasks:
        lines.append("🟡 *This Month*")
        for t in month_tasks:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t)
            n += 1

    lines.append("\n_Tap each item below to reassign its urgency 👇_")
    return "\n".join(lines), ordered


# ══════════════════════════════════════════════════════════════════════════════
# INLINE KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════

def _clean_pid(pid: str) -> str:
    return pid.replace("-", "")


def _restore_pid(pid: str) -> str:
    return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def review_keyboard(page_id: str) -> InlineKeyboardMarkup:
    p = _clean_pid(page_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Today", callback_data=f"h:{p}:t"),
         InlineKeyboardButton("🟠 This Week", callback_data=f"h:{p}:w")],
        [InlineKeyboardButton("🟡 This Month", callback_data=f"h:{p}:m"),
         InlineKeyboardButton("⚪ Backburner", callback_data=f"h:{p}:b")],
        [InlineKeyboardButton("✅ Done", callback_data=f"d:{p}")],
    ])


def new_task_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Today", callback_data=f"nt:{key}:t"),
         InlineKeyboardButton("🟠 This Week", callback_data=f"nt:{key}:w")],
        [InlineKeyboardButton("🟡 This Month", callback_data=f"nt:{key}:m"),
         InlineKeyboardButton("⚪ Backburner", callback_data=f"nt:{key}:b")],
    ])


def done_picker_keyboard(key: str, page: int = 0, page_size: int = 5) -> InlineKeyboardMarkup:
    tasks = done_picker_map.get(key, [])
    start = page * page_size
    end = start + page_size
    page_tasks = tasks[start:end]

    rows = []
    for idx, task in enumerate(page_tasks, start=start):
        label = task["name"]
        if len(label) > 28:
            label = label[:25] + "..."
        rows.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"dp:{key}:{idx}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"dpp:{key}:{page - 1}"))
    if end < len(tasks):
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"dpp:{key}:{page + 1}"))
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
    """
    Main capture dispatcher.

    - Multi-task (2+ items detected): classify all concurrently, create all,
      reply with a grouped batch summary. No pickers shown for low-confidence
      items — they land in Backburner with a note in the summary.
    - Single task, high confidence: create and confirm.
    - Single task, low confidence: show horizon picker as before.
    """
    global _pending_counter

    task_texts = split_tasks(raw_text)
    is_multi = len(task_texts) > 1

    thinking = await message.reply_text(
        f"🧠 Classifying {len(task_texts)} tasks..." if is_multi else "🧠 Classifying..."
    )

    if is_multi:
        loop = asyncio.get_event_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, _run_capture, t, force_create)
            for t in task_texts
        ])
        summary = format_batch_summary(list(results))
        await thinking.edit_text(summary, parse_mode="Markdown")
        return

    # ── Single task path ─────────────────────────────────────────────────────
    try:
        result = classify_task(raw_text)
        task_name = result.get("task_name", raw_text)
        deadline_days = result.get("deadline_days")
        ctx = result.get("context", "🏠 Personal")
        confidence = result.get("confidence", "low")
        recurring = result.get("recurring", "None") or "None"
        repeat_day = result.get("repeat_day")

        # Compute deadline from next occurrence for weekly recurring tasks
        if recurring == "📅 Weekly" and repeat_day in REPEAT_DAY_TO_WEEKDAY:
            if deadline_days is None:
                target = next_weekday(REPEAT_DAY_TO_WEEKDAY[repeat_day])
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
                f"⚠️ Already on your list:\n\n📝 {dup['name']}\n🕐 {dup.get('auto_horizon', '')}  {dup.get('context', '')}\n\nSend `force: {task_name}` if you want to add it anyway.",
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
        key = str(_pending_counter)
        _pending_counter += 1
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
    key = str(_done_picker_counter)
    _done_picker_counter += 1
    done_picker_map[key] = tasks
    await message.reply_text("Which task should be marked done?", reply_markup=done_picker_keyboard(key, page=0))


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != MY_CHAT_ID:
        return

    message = update.message
    text = (message.text or "").strip()
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
        await open_done_picker(message)
        return

    # 3. done 1,3
    match_nums = re.match(r"done\s+([\d,\s]+)$", text, re.IGNORECASE)
    if match_nums:
        numbers = [int(n.strip()) for n in match_nums.group(1).split(",") if n.strip().isdigit()]
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        if source_id and source_id in digest_map:
            items = digest_map[source_id]
            done_names = []
            for n in numbers:
                if 1 <= n <= len(items):
                    pid = items[n - 1]["page_id"]
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
        query = match_name.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # 5. mark ... done
    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        query = match_mark_done.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # 6. focus: task name
    match_focus = re.match(r"focus:\s*(.+)$", text, re.IGNORECASE)
    if match_focus:
        query = match_focus.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            set_focus(matched["page_id"], True)
            await message.reply_text(f"🎯 Focused: {matched['name']} → *Doing*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # 7. unfocus: task name
    match_unfocus = re.match(r"unfocus:\s*(.+)$", text, re.IGNORECASE)
    if match_unfocus:
        query = match_unfocus.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            set_focus(matched["page_id"], False)
            await message.reply_text(f"⬜ Unfocused: {matched['name']} → *To Do*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # 8. force:
    match_force = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if match_force:
        await create_or_prompt_task(message, match_force.group(1).strip(), force_create=True)
        return

    # 9. normal capture — single or multi
    await create_or_prompt_task(message, text, force_create=False)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")

    if parts[0] == "nt" and len(parts) == 3:
        _, key, code = parts
        if key not in pending_map:
            await q.edit_message_text("⚠️ This task expired — please re-send it.")
            return
        task = pending_map.pop(key)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        days = HORIZON_DEADLINE_OFFSETS.get(code)
        recurring = task.get("recurring", "None")
        repeat_day = task.get("repeat_day")
        dup = find_duplicate_active_task(task["name"])
        if dup:
            await q.edit_message_text(
                f"⚠️ Already on your list:\n\n📝 {dup['name']}\n🕐 {dup.get('auto_horizon', '')}  {dup.get('context', '')}\n\nSend `force: {task['name']}` if you want to add it anyway.",
                parse_mode="Markdown",
            )
            return
        recur_tag = f"\n🔁 {recurring}" if recurring != "None" else ""
        try:
            page_id = create_task(task["name"], days, task["context"], recurring=recurring, repeat_day=repeat_day)
            await q.edit_message_text(
                f"✅ Captured!\n\n📝 {task['name']}\n🕐 {horizon_label}  {task['context']}{recur_tag}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            capture_map[q.message.message_id] = {"page_id": page_id, "name": task["name"]}
        except Exception as e:
            log.error(f"Notion error: {e}")
            await q.edit_message_text("⚠️ Couldn't save to Notion.")
        return

    if parts[0] == "d" and len(parts) == 2:
        page_id = _restore_pid(parts[1])
        try:
            mark_done(page_id)
            suffix = "\n↻ Next instance created" if handle_done_recurring(page_id) else ""
            await q.edit_message_text(f"✅ Marked as done!{suffix}")
        except Exception as e:
            log.error(f"Notion done error: {e}")
            await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "h" and len(parts) == 3:
        _, pid_clean, code = parts
        page_id = _restore_pid(pid_clean)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        try:
            set_deadline_from_horizon_code(page_id, code)
            await q.edit_message_text(f"Updated → {horizon_label} ✓")
        except Exception as e:
            log.error(f"Notion horizon error: {e}")
            await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "dp" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown")
            return
        try:
            idx = int(idx_str)
            task = done_picker_map[key][idx]
            mark_done(task["page_id"])
            suffix = "\n↻ Next instance created" if handle_done_recurring(task["page_id"]) else ""
            await q.edit_message_text(f"✅ Done: {task['name']}{suffix}")
        except Exception as e:
            log.error(f"Done picker error: {e}")
            await q.edit_message_text("⚠️ Couldn't mark that task done.")
        return

    if parts[0] == "dpp" and len(parts) == 3:
        _, key, page_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown")
            return
        await q.edit_message_reply_markup(reply_markup=done_picker_keyboard(key, page=int(page_str)))
        return

    if parts[0] == "dpc" and len(parts) == 2:
        _, key = parts
        done_picker_map.pop(key, None)
        await q.edit_message_text("Done picker closed.")
        return


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ══════════════════════════════════════════════════════════════════════════════

async def run_recurring_check(bot) -> None:
    spawned = process_recurring_tasks()
    log.info(f"Recurring check: {spawned} task(s) spawned")


async def send_daily_digest(bot) -> None:
    global last_digest_msg_id
    tasks = get_today_and_overdue_tasks()
    message, ordered = format_daily_digest(tasks)
    sent = await bot.send_message(chat_id=MY_CHAT_ID, text=message, parse_mode="Markdown")
    if ordered:
        digest_map[sent.message_id] = ordered
        last_digest_msg_id = sent.message_id
    log.info(f"Daily digest sent — {len(ordered)} tasks")


async def send_sunday_review(bot) -> None:
    await send_daily_digest(bot)
    week_tasks = query_tasks_by_auto_horizon(["🟠 This Week"])
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
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(run_recurring_check, "cron", hour=_rc_h, minute=_rc_m, args=[app.bot])
    scheduler.add_job(send_daily_digest, "cron", day_of_week="mon-fri", hour=_wk_h, minute=_wk_m, args=[app.bot])
    scheduler.add_job(send_daily_digest, "cron", day_of_week="sat", hour=_we_h, minute=_we_m, args=[app.bot])
    scheduler.add_job(send_sunday_review, "cron", day_of_week="sun", hour=_we_h, minute=_we_m, args=[app.bot])
    scheduler.start()
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"weekday={_wk_h:02d}:{_wk_m:02d}  "
        f"weekend={_we_h:02d}:{_we_m:02d}  "
        f"recurring={_rc_h:02d}:{_rc_m:02d}"
    )


def main() -> None:
    port = int(os.environ.get("PORT", 8080))
    flask_thread = threading.Thread(
        target=lambda: flask_app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
        name="flask-health",
    )
    flask_thread.start()
    log.info("Flask health server started on port %d", port)

    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info("🤖 Second Brain bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
