"""Task-related Notion helpers."""

import re
import calendar
from datetime import date, datetime, timedelta

from notion_client import Client as NotionClient

from second_brain.config import HORIZON_DEADLINE_OFFSETS
from second_brain.utils import local_today
from second_brain.notion.properties import (
    checkbox_filter,
    extract_date,
    extract_formula,
    extract_select,
    extract_title,
    query_all,
    select_prop,
    title_prop,
)


def _deadline_prop(days: int | None) -> dict:
    if days is None:
        return {"date": None}
    return {"date": {"start": (local_today() + timedelta(days=days)).isoformat()}}


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


def _task_sort_key(task: dict) -> tuple[int, str, str]:
    deadline = _parse_deadline(task.get("deadline"))
    deadline_ord = deadline.toordinal() if deadline else 99999999
    context = (task.get("context") or "").lower()
    name = (task.get("name") or "").lower()
    return (deadline_ord, context, name)


def _context_label(task: dict) -> str:
    return (task.get("context") or "").strip()


def _normalize_task_name(text: str) -> str:
    s = text.lower().strip()
    s = re.sub(r"\b(today|tonight|tomorrow|this week|this month|asap|urgent|by eod)\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s.endswith("s") and len(s) > 4:
        s = s[:-1]
    return s

def create_task(notion: NotionClient, notion_db_id: str, name: str, deadline_days: int | None, context: str,
                recurring: str = "None", repeat_day: str | None = None) -> str:
    props = {
        "Name":      title_prop(name),
        "Deadline":  _deadline_prop(deadline_days),
        "Context":   select_prop(context),
        "Source":    select_prop("📱 Telegram"),
        "Recurring": select_prop(recurring),
    }
    if repeat_day:
        props["Repeat Day"] = select_prop(repeat_day)
    page = notion.pages.create(parent={"database_id": notion_db_id}, properties=props)
    return page["id"]


def mark_done(notion: NotionClient, page_id: str) -> None:
    notion.pages.update(page_id=page_id, properties={"Done": {"checkbox": True}})


def set_deadline_from_horizon_code(notion: NotionClient, page_id: str, code: str) -> None:
    days = HORIZON_DEADLINE_OFFSETS.get(code)
    if days is None:
        notion.pages.update(page_id=page_id, properties={"Deadline": {"date": None}})
    else:
        target = local_today() + timedelta(days=days)
        notion.pages.update(page_id=page_id, properties={"Deadline": {"date": {"start": target.isoformat()}}})


def set_focus(notion: NotionClient, page_id: str, focused: bool) -> None:
    notion.pages.update(page_id=page_id, properties={"Focus": {"checkbox": focused}})


def set_last_generated(notion: NotionClient, page_id: str, d: date) -> None:
    notion.pages.update(page_id=page_id, properties={"Last Generated": {"date": {"start": d.isoformat()}}})



def get_all_active_tasks(notion: NotionClient, notion_db_id: str) -> list[dict]:
    results = query_all(notion, notion_db_id, filter=checkbox_filter("Done", False))

    return [
        {
            "page_id": p["id"],
            "name": extract_title(p["properties"].get("Name")) or "Untitled",
            "auto_horizon": extract_formula(p["properties"].get("Auto Horizon")) or "",
            "context": extract_select(p["properties"].get("Context")) or "",
            "deadline": extract_date(p["properties"].get("Deadline")),
        }
        for p in results
    ]


def _get_tasks_by_deadline_horizon(notion: NotionClient, notion_db_id: str) -> tuple[list, list, list, list]:
    tasks = get_all_active_tasks(notion, notion_db_id)
    today = local_today()
    overdue, today_tasks, this_week, backlog = [], [], [], []
    for task in tasks:
        deadline = _parse_deadline(task.get("deadline"))
        if deadline is None:
            backlog.append(task)
        elif deadline < today:
            overdue.append(task)
        elif deadline == today:
            today_tasks.append(task)
        elif 1 <= (deadline - today).days <= 7:
            this_week.append(task)
        else:
            backlog.append(task)
    return (sorted(overdue, key=_task_sort_key), sorted(today_tasks, key=_task_sort_key), sorted(this_week, key=_task_sort_key), sorted(backlog, key=_task_sort_key))


def get_today_and_overdue_tasks(notion: NotionClient, notion_db_id: str, limit: int | None = 10) -> list[dict]:
    tasks = get_all_active_tasks(notion, notion_db_id)
    today = local_today()
    selected = []

    def context_rank(task: dict) -> int:
        ctx = (task.get("context") or "").lower()
        if "personal" in ctx or "🏠" in ctx:
            return 0
        if "work" in ctx or "💼" in ctx:
            return 2
        return 1

    def urgency_sort_key(task: dict) -> tuple[int, int, int, str]:
        parsed_deadline = _parse_deadline(task.get("deadline"))
        if parsed_deadline is not None:
            deadline_days = (parsed_deadline - today).days
        else:
            deadline_days = 8

        horizon = task.get("auto_horizon") or ""
        horizon_rank = 0 if horizon == "🔴 Today" else 1 if horizon == "🟠 This Week" else 2
        return (deadline_days, horizon_rank, context_rank(task), task.get("name", "").lower())

    for t in tasks:
        parsed_deadline = _parse_deadline(t.get("deadline"))
        has_due_date = parsed_deadline is not None
        is_overdue = bool(parsed_deadline and parsed_deadline < today)
        due_within_7_days = bool(parsed_deadline and 0 <= (parsed_deadline - today).days <= 7)
        horizon = t.get("auto_horizon") or ""
        horizon_carry = (not has_due_date) and horizon in {"🔴 Today", "🟠 This Week"}
        if is_overdue or due_within_7_days or horizon_carry:
            selected.append(t)

    overdue = [t for t in selected if (d := _parse_deadline(t.get("deadline"))) is not None and d < today]
    non_overdue = [t for t in selected if t not in overdue]
    ordered = sorted(overdue, key=urgency_sort_key) + sorted(non_overdue, key=urgency_sort_key)
    return ordered[:limit] if isinstance(limit, int) else ordered


def get_quick_refresh_tasks(notion: NotionClient, notion_db_id: str, limit: int = 10) -> list[dict]:
    tasks = get_all_active_tasks(notion, notion_db_id)
    today = local_today()
    today_str = today.isoformat()
    cutoff_str = (today + timedelta(days=7)).isoformat()

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
    ordered = sorted(visible, key=lambda t: (context_rank(t), t.get("deadline") or "9999-12-31", t.get("name", "").lower()))
    return ordered[:limit]


def get_recurring_templates(notion: NotionClient, notion_db_id: str) -> list[dict]:
    results = notion.databases.query(
        database_id=notion_db_id,
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
            "name": extract_title(p.get("Name")) or "Untitled",
            "auto_horizon": extract_formula(p.get("Auto Horizon")) or "🔴 Today",
            "context": extract_select(p.get("Context")) or "🏠 Personal",
            "recurring": extract_select(p.get("Recurring")) or "None",
            "repeat_day": extract_select(p.get("Repeat Day")),
            "last_generated": extract_date(p.get("Last Generated")),
            "deadline": extract_date(p.get("Deadline")),
        })
    return templates


def fuzzy_match(query: str, tasks: list[dict]) -> dict | None:
    q = _normalize_task_name(query)
    if not q:
        return None
    exact = next((t for t in tasks if _normalize_task_name(t["name"]) == q), None)
    if exact:
        return exact
    return next(((t for t in tasks if q in _normalize_task_name(t["name"]) or _normalize_task_name(t["name"]) in q)), None)


def find_duplicate_active_task(notion: NotionClient, notion_db_id: str, name: str) -> dict | None:
    return fuzzy_match(name, get_all_active_tasks(notion, notion_db_id))


def recover_digest_items_from_text(notion: NotionClient, notion_db_id: str, text: str) -> dict[int, dict]:
    if not text:
        return {}
    number_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    emoji_to_num = {emoji: i + 1 for i, emoji in enumerate(number_emojis)}
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
        task_name = remainder.split("  ")[0].strip()
        if task_name:
            numbered_names[n] = task_name
    if not numbered_names:
        return {}
    active_tasks = get_all_active_tasks(notion, notion_db_id)
    recovered: dict[int, dict] = {}
    for n, name in numbered_names.items():
        matched = fuzzy_match(name, active_tasks)
        if matched:
            recovered[n] = matched
    return recovered

REPEAT_DAY_TO_WEEKDAY = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}
REPEAT_DAY_TO_MONTHDAY = {
    "1st": 1, "2nd": 2, "3rd": 3, "4th": 4, "5th": 5,
    "6th": 6, "7th": 7, "8th": 8, "9th": 9, "10th": 10,
    "11th": 11, "12th": 12, "13th": 13, "14th": 14, "15th": 15,
    "16th": 16, "17th": 17, "18th": 18, "19th": 19, "20th": 20,
    "21st": 21, "22nd": 22, "23rd": 23, "24th": 24, "25th": 25,
    "26th": 26, "27th": 27, "28th": 28, "29th": 29, "30th": 30,
    "31st": 31, "Last": -1,
}


def _resolve_monthly_target_day(repeat_day: str, today: date) -> int | None:
    if repeat_day not in REPEAT_DAY_TO_MONTHDAY:
        return None
    configured_day = REPEAT_DAY_TO_MONTHDAY[repeat_day]
    month_last_day = calendar.monthrange(today.year, today.month)[1]
    if configured_day == -1:
        return month_last_day
    return min(configured_day, month_last_day)


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
        if not repeat_day:
            return False
        target_day = _resolve_monthly_target_day(repeat_day, today)
        return target_day is not None and today.day == target_day
    if recurring == "📆 Quarterly":
        if not repeat_day:
            return False
        target_day = _resolve_monthly_target_day(repeat_day, today)
        if target_day is None or today.day != target_day:
            return False
        anchor_raw = template.get("deadline") or last_gen
        if not anchor_raw:
            return today.month % 3 == 0
        try:
            anchor = date.fromisoformat(anchor_raw)
        except ValueError:
            return today.month % 3 == 0
        months_since_anchor = (today.year - anchor.year) * 12 + (today.month - anchor.month)
        return months_since_anchor >= 0 and months_since_anchor % 3 == 0
    return False


def spawn_recurring_instance(notion: NotionClient, notion_db_id: str, template: dict) -> None:
    today = local_today()
    notion.pages.create(
        parent={"database_id": notion_db_id},
        properties={
            "Name": {"title": [{"text": {"content": template["name"]}}]},
            "Deadline": {"date": {"start": today.isoformat()}},
            "Context": {"select": {"name": template["context"]}},
            "Source": {"select": {"name": "✏️ Manual"}},
        },
    )
    set_last_generated(notion, template["page_id"], today)


def process_recurring_tasks(notion: NotionClient, notion_db_id: str) -> int:
    today = local_today()
    spawned = 0
    for t in get_recurring_templates(notion, notion_db_id):
        if should_spawn_today(t, today):
            spawn_recurring_instance(notion, notion_db_id, t)
            spawned += 1
    return spawned


def handle_done_recurring(notion: NotionClient, notion_db_id: str, page_id: str) -> bool:
    result = notion.pages.retrieve(page_id=page_id)
    p = result["properties"]
    recurring = extract_select(p.get("Recurring")) or "None"
    if recurring == "None":
        return False
    spawn_recurring_instance(notion, notion_db_id, {
        "page_id": page_id,
        "name": extract_title(p.get("Name")) or "Untitled",
        "auto_horizon": extract_formula(p.get("Auto Horizon")) or "🔴 Today",
        "context": extract_select(p.get("Context")) or "🏠 Personal",
        "recurring": recurring,
        "repeat_day": extract_select(p.get("Repeat Day")),
        "last_generated": extract_date(p.get("Last Generated")),
        "deadline": extract_date(p.get("Deadline")),
    })
    return True
