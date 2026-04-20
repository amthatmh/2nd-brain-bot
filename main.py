#!/usr/bin/env python3
"""
Second Brain — Telegram Bot (v3 — fully env-configurable)
────────────────────────────────────────────────────────────────
All tuneable variables live in .env / Railway environment.
No need to edit this script for config changes.
"""

import os, json, re, logging, calendar
from datetime import date
import pytz
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import anthropic
from notion_client import Client as NotionClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
MY_CHAT_ID       = int(os.environ["TELEGRAM_CHAT_ID"])
ANTHROPIC_KEY    = os.environ["ANTHROPIC_API_KEY"]
NOTION_TOKEN     = os.environ["NOTION_TOKEN"]
NOTION_DB_ID     = os.environ["NOTION_DB_ID"]

# Scheduling
TZ               = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
_wk_h, _wk_m    = map(int, os.environ.get("DIGEST_TIME_WEEKDAY", "8:15").split(":"))
_we_h, _we_m    = map(int, os.environ.get("DIGEST_TIME_WEEKEND", "12:00").split(":"))
_rc_h, _rc_m    = map(int, os.environ.get("RECURRING_CHECK_TIME", "7:00").split(":"))

# AI
CLAUDE_MODEL     = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOK   = int(os.environ.get("CLAUDE_MAX_TOKENS", "150"))

# ── Clients ───────────────────────────────────────────────────────────────────
notion = NotionClient(auth=NOTION_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ── In-memory state ───────────────────────────────────────────────────────────
digest_map: dict[int, list[dict]] = {}
last_digest_msg_id: int | None = None
pending_map: dict[str, dict] = {}
_pending_counter = 0
capture_map: dict[int, dict] = {}

# ── Constants ─────────────────────────────────────────────────────────────────
HORIZON_MAP = {
    "t": "🔴 Today",
    "w": "🟠 This Week",
    "m": "🟡 This Month",
    "b": "⚪ Backburner",
}
NUMBER_EMOJIS = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
REPEAT_DAY_TO_WEEKDAY  = {"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}
REPEAT_DAY_TO_MONTHDAY = {"1st":1,"5th":5,"10th":10,"15th":15,"20th":20,"25th":25,"Last":-1}

def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def classify_task(text: str) -> dict:
    prompt = f"""You are a personal task classifier for a second brain system.

Task: "{text}"

Return ONLY valid JSON, no markdown, no explanation:
{{
  "task_name": "clean concise task name",
  "horizon": "one of exactly: 🔴 Today | 🟠 This Week | 🟡 This Month | ⚪ Backburner",
  "context": "one of exactly: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab",
  "confidence": "high or low"
}}

Horizon rules:
- today/tonight/now/urgent/ASAP/by EOD → 🔴 Today
- this week/by Friday/in a few days → 🟠 This Week
- this month/next few weeks/soon-ish → 🟡 This Month
- someday/eventually/no urgency → ⚪ Backburner
- NO time signal at all → confidence "low"

Context rules:
- meetings/clients/projects/reports → 💼 Work
- gym/doctor/dentist/food/workout → 🏃 Health
- family/friends/home/errands → 🏠 Personal
- collaborations/shared tasks → 🤝 Collab"""

    resp = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOK,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


# ═══════════════════════════════════════════════════════════════════════════════
# NOTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def create_task(name: str, horizon: str, context: str,
                recurring: str = "None", repeat_day: str | None = None) -> str:
    props = {
        "Name":      {"title":  [{"text": {"content": name}}]},
        "Status":    {"select": {"name": "To Do"}},
        "Horizon":   {"select": {"name": horizon}},
        "Context":   {"select": {"name": context}},
        "Source":    {"select": {"name": "📱 Telegram"}},
        "Recurring": {"select": {"name": recurring}},
    }
    if repeat_day:
        props["Repeat Day"] = {"select": {"name": repeat_day}}
    page = notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
    return page["id"]


def mark_done(page_id: str) -> None:
    notion.pages.update(page_id=page_id, properties={"Status": {"select": {"name": "Done 🙌"}}})


def set_horizon(page_id: str, horizon: str) -> None:
    notion.pages.update(page_id=page_id, properties={"Horizon": {"select": {"name": horizon}}})


def set_last_generated(page_id: str, d: date) -> None:
    notion.pages.update(page_id=page_id, properties={"Last Generated": {"date": {"start": d.isoformat()}}})


def _get_prop(props: dict, key: str, kind: str) -> str | None:
    prop = props.get(key, {})
    if kind == "title":
        parts = prop.get("title", [])
        return parts[0]["text"]["content"] if parts else None
    if kind == "select":
        sel = prop.get("select")
        return sel["name"] if sel else None
    if kind == "date":
        d = prop.get("date")
        return d["start"] if d else None
    return None


def query_tasks(horizons: list[str]) -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={
            "and": [
                {"property": "Status", "select": {"does_not_equal": "Done 🙌"}},
                {"or": [{"property": "Horizon", "select": {"equals": h}} for h in horizons]},
            ]
        },
    )
    tasks = []
    for page in results.get("results", []):
        p = page["properties"]
        tasks.append({
            "page_id":  page["id"],
            "name":     _get_prop(p, "Name",     "title")  or "Untitled",
            "horizon":  _get_prop(p, "Horizon",  "select") or "",
            "context":  _get_prop(p, "Context",  "select") or "",
            "deadline": _get_prop(p, "Deadline", "date"),
        })
    return tasks


def get_all_active_tasks() -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={"property": "Status", "select": {"does_not_equal": "Done 🙌"}},
    )
    tasks = []
    for p in results.get("results", []):
        props = p["properties"]
        tasks.append({
            "page_id": p["id"],
            "name": _get_prop(props, "Name", "title") or "Untitled",
            "horizon": _get_prop(props, "Horizon", "select") or "",
            "context": _get_prop(props, "Context", "select") or "",
        })
    return tasks


def get_recurring_templates() -> list[dict]:
    results = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={
            "and": [
                {"property": "Recurring", "select": {"does_not_equal": "None"}},
                {"property": "Status",    "select": {"does_not_equal": "Done 🙌"}},
            ]
        },
    )
    templates = []
    for page in results.get("results", []):
        p = page["properties"]
        templates.append({
            "page_id":        page["id"],
            "name":           _get_prop(p, "Name",           "title")  or "Untitled",
            "horizon":        _get_prop(p, "Horizon",        "select") or "🔴 Today",
            "context":        _get_prop(p, "Context",        "select") or "🏠 Personal",
            "recurring":      _get_prop(p, "Recurring",      "select") or "None",
            "repeat_day":     _get_prop(p, "Repeat Day",     "select"),
            "last_generated": _get_prop(p, "Last Generated", "date"),
        })
    return templates




def normalize_task_name(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"\b(today|tonight|now|urgent|asap|by eod|this week|this month|someday|eventually)\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s.endswith("s") and len(s) > 3:
        s = s[:-1]
    return s


def find_duplicate_task(query: str, tasks: list[dict]) -> dict | None:
    nq = normalize_task_name(query)
    if not nq:
        return None
    for t in tasks:
        if normalize_task_name(t["name"]) == nq:
            return t
    return None


def fuzzy_match(query: str, tasks: list[dict]) -> dict | None:
    q = query.lower().strip()
    return next((t for t in tasks if q in t["name"].lower()), None)


# ═══════════════════════════════════════════════════════════════════════════════
# RECURRING LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

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
            "Name":    {"title":  [{"text": {"content": template["name"]}}]},
            "Status":  {"select": {"name": "To Do"}},
            "Horizon": {"select": {"name": template["horizon"]}},
            "Context": {"select": {"name": template["context"]}},
            "Source":  {"select": {"name": "✏️ Manual"}},
        },
    )
    set_last_generated(template["page_id"], today)
    log.info(f"Spawned recurring: {template['name']}")


def process_recurring_tasks() -> int:
    today     = date.today()
    templates = get_recurring_templates()
    spawned   = 0
    for t in templates:
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
        "name":           _get_prop(p, "Name",           "title")  or "Untitled",
        "horizon":        _get_prop(p, "Horizon",        "select") or "🔴 Today",
        "context":        _get_prop(p, "Context",        "select") or "🏠 Personal",
        "recurring":      recurring,
        "repeat_day":     _get_prop(p, "Repeat Day",     "select"),
        "last_generated": _get_prop(p, "Last Generated", "date"),
    })
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════

def format_daily_digest(tasks: list[dict]) -> tuple[str, list[dict]]:
    from datetime import datetime
    date_str  = datetime.now(TZ).strftime("%A, %B %-d")
    if not tasks:
        return f"☀️ *{date_str}*\n\nAll clear — no tasks due today! 🎉", []

    today_str          = date.today().isoformat()
    overdue            = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now          = [t for t in tasks if t not in overdue]
    lines, ordered, n  = [f"☀️ *{date_str}*\n"], [], 1

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

    lines.append("\n_Reply `done 1`, `done 1,3`, or `done: task name` to mark complete_")
    return "\n".join(lines), ordered


def format_sunday_intro(week_tasks: list[dict], month_tasks: list[dict]) -> tuple[str, list[dict]]:
    from datetime import datetime
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


# ═══════════════════════════════════════════════════════════════════════════════
# INLINE KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════

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




async def send_capture_confirmation(message, page_id: str, task_name: str, horizon: str, ctx: str) -> None:
    await message.edit_text(
        f"✅ Captured!\n\n📝 {task_name}\n🕐 {horizon}  {ctx}\n\n_Saved to Notion_",
        parse_mode="Markdown")
    capture_map[message.message_id] = {"page_id": page_id, "name": task_name}


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pending_counter, last_digest_msg_id
    if update.effective_chat.id != MY_CHAT_ID:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    lower_text = text.lower().strip()

    # reply "done" to a captured task message
    if update.message.reply_to_message and lower_text == "done":
        reply_id = update.message.reply_to_message.message_id
        captured = capture_map.get(reply_id)
        if captured:
            mark_done(captured["page_id"])
            suffix = "\n↻ Next instance created" if handle_done_recurring(captured["page_id"]) else ""
            await update.message.reply_text(f"✅ Done: {captured['name']}{suffix}")
            return

    # done 1,3
    match_nums = re.match(r"done\s+([\d,\s]+)$", text, re.IGNORECASE)
    if match_nums:
        numbers   = [int(n.strip()) for n in match_nums.group(1).split(",") if n.strip().isdigit()]
        source_id = (update.message.reply_to_message.message_id
                     if update.message.reply_to_message else last_digest_msg_id)
        if source_id and source_id in digest_map:
            items, done_names = digest_map[source_id], []
            for n in numbers:
                if 1 <= n <= len(items):
                    pid  = items[n - 1]["page_id"]
                    name = items[n - 1]["name"]
                    mark_done(pid)
                    suffix = " ↻ next queued" if handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")
            msg = ("Marked done:\n" + "\n".join(f"✅ {n}" for n in done_names)
                   if done_names else "Couldn't find those items.")
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    # done: task name
    match_name = re.match(r"done:\s*(.+)$", text, re.IGNORECASE)
    if match_name:
        query = match_name.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            mark_done(matched["page_id"])
            suffix = "\n↻ Next instance created" if handle_done_recurring(matched["page_id"]) else ""
            await update.message.reply_text(f"✅ Done: {matched['name']}{suffix}")
        else:
            await update.message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # mark task done
    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        query = match_mark_done.group(1).strip()
        matched = fuzzy_match(query, get_all_active_tasks())
        if matched:
            mark_done(matched["page_id"])
            suffix = "\n↻ Next instance created" if handle_done_recurring(matched["page_id"]) else ""
            await update.message.reply_text(f"✅ Done: {matched['name']}{suffix}")
        else:
            await update.message.reply_text(f"Couldn't find a task matching \"{query}\".")
        return

    # force add bypasses duplicate guard
    force_add = False
    force_match = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if force_match:
        force_add = True
        text = force_match.group(1).strip()

    # New task capture
    thinking = await update.message.reply_text("🧠 Classifying...")
    try:
        result     = classify_task(text)
        task_name  = result.get("task_name", text)
        horizon    = result.get("horizon",    "⚪ Backburner")
        ctx        = result.get("context",    "🏠 Personal")
        confidence = result.get("confidence", "low")
    except Exception as e:
        log.error(f"Claude error: {e}")
        await thinking.edit_text("⚠️ Couldn't classify that. Try rephrasing?")
        return

    active_tasks = get_all_active_tasks()
    duplicate = None if force_add else find_duplicate_task(task_name, active_tasks)
    if duplicate:
        dup_horizon = duplicate.get("horizon", "")
        dup_context = duplicate.get("context", "")
        await thinking.edit_text(
            f"⚠️ Already on your list:\n\n📝 {duplicate['name']}\n🕐 {dup_horizon}  {dup_context}\n\n"
            f'Send `force: {task_name}` if you want to add it anyway.',
            parse_mode="Markdown",
        )
        return

    if confidence == "high":
        try:
            page_id = create_task(task_name, horizon, ctx)
            await send_capture_confirmation(thinking, page_id, task_name, horizon, ctx)
        except Exception as e:
            log.error(f"Notion error: {e}")
            await thinking.edit_text("⚠️ Classified but couldn't write to Notion.")
    else:
        key = str(_pending_counter); _pending_counter += 1
        pending_map[key] = {"name": task_name, "context": ctx}
        await thinking.edit_text(
            f"📝 *{task_name}*  {ctx}\n\nWhen should this happen?",
            parse_mode="Markdown", reply_markup=new_task_keyboard(key))

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
        horizon = HORIZON_MAP.get(code, "⚪ Backburner")

        try:
            page_id = create_task(task["name"], horizon, task["context"])
            await q.edit_message_text(
                f"✅ Captured!\n\n📝 {task['name']}\n🕐 {horizon}  {task['context']}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            capture_map[q.message.message_id] = {
                "page_id": page_id,
                "name": task["name"],
            }
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
        horizon = HORIZON_MAP.get(code, "⚪ Backburner")
        try:
            set_horizon(page_id, horizon)
            await q.edit_message_text(f"Updated → {horizon} ✓")
        except Exception as e:
            log.error(f"Notion horizon error: {e}")
            await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULED JOBS
# ═══════════════════════════════════════════════════════════════════════════════

async def run_recurring_check(bot) -> None:
    spawned = process_recurring_tasks()
    log.info(f"Recurring check: {spawned} task(s) spawned")


async def send_daily_digest(bot) -> None:
    global last_digest_msg_id
    tasks            = query_tasks(["🔴 Today"])
    message, ordered = format_daily_digest(tasks)
    sent = await bot.send_message(chat_id=MY_CHAT_ID, text=message, parse_mode="Markdown")
    if ordered:
        digest_map[sent.message_id] = ordered
        last_digest_msg_id          = sent.message_id
    log.info(f"Daily digest sent — {len(ordered)} tasks")


async def send_sunday_review(bot) -> None:
    await send_daily_digest(bot)
    week_tasks  = query_tasks(["🟠 This Week"])
    month_tasks = query_tasks(["🟡 This Month"])
    header, ordered = format_sunday_intro(week_tasks, month_tasks)
    await bot.send_message(chat_id=MY_CHAT_ID, text=header, parse_mode="Markdown")
    for n, task in enumerate(ordered, 1):
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text=f"{num_emoji(n)} *{task['name']}*  {task['context']}\n_Currently: {task['horizon']}_",
            parse_mode="Markdown",
            reply_markup=review_keyboard(task["page_id"]),
        )
    log.info(f"Sunday review sent — {len(ordered)} items")


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    scheduler = AsyncIOScheduler(timezone=TZ)

    # Recurring check (before digests)
    scheduler.add_job(run_recurring_check, "cron",
                      hour=_rc_h, minute=_rc_m, args=[app.bot])
    # Mon–Fri digest
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="mon-fri", hour=_wk_h, minute=_wk_m, args=[app.bot])
    # Sat digest
    scheduler.add_job(send_daily_digest, "cron",
                      day_of_week="sat", hour=_we_h, minute=_we_m, args=[app.bot])
    # Sun full review
    scheduler.add_job(send_sunday_review, "cron",
                      day_of_week="sun", hour=_we_h, minute=_we_m, args=[app.bot])

    scheduler.start()
    log.info(
        f"Scheduler started ✓  TZ={TZ}  "
        f"weekday={_wk_h:02d}:{_wk_m:02d}  "
        f"weekend={_we_h:02d}:{_we_m:02d}  "
        f"recurring={_rc_h:02d}:{_rc_m:02d}"
    )


def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    log.info("🤖 Second Brain bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
