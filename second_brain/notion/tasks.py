"""Task-related Notion helpers."""

import re
from datetime import date, datetime, timedelta

from notion_client import Client as NotionClient

from second_brain.config import HORIZON_DEADLINE_OFFSETS, TZ


def local_today() -> date:
    return datetime.now(TZ).date()


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
        "Name":      {"title":  [{"text": {"content": name}}]},
        "Deadline":  _deadline_prop(deadline_days),
        "Context":   {"select": {"name": context}},
        "Source":    {"select": {"name": "📱 Telegram"}},
        "Recurring": {"select": {"name": recurring}},
    }
    if repeat_day:
        props["Repeat Day"] = {"select": {"name": repeat_day}}
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
    cursor = None
    results: list[dict] = []
    while True:
        kwargs = {"database_id": notion_db_id, "filter": {"property": "Done", "checkbox": {"equals": False}}}
        if cursor:
            kwargs["start_cursor"] = cursor
        response = notion.databases.query(**kwargs)
        results.extend(response.get("results", []))
        if not response.get("has_more"):
            break
        cursor = response.get("next_cursor")

    return [
        {
            "page_id": p["id"],
            "name": _get_prop(p["properties"], "Name", "title") or "Untitled",
            "auto_horizon": _get_prop(p["properties"], "Auto Horizon", "formula") or "",
            "context": _get_prop(p["properties"], "Context", "select") or "",
            "deadline": _get_prop(p["properties"], "Deadline", "date"),
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

    def context_rank(task: dict) -> tuple[int, str]:
        ctx = (task.get("context") or "").lower()
        if "personal" in ctx or "🏠" in ctx:
            return (0, task.get("name", "").lower())
        if "work" in ctx or "💼" in ctx:
            return (2, task.get("name", "").lower())
        return (1, task.get("name", "").lower())

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
    today_only = [t for t in selected if (d := _parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
    carryover = [t for t in selected if t not in overdue and t not in today_only]
    ordered = sorted(overdue, key=context_rank) + sorted(today_only, key=context_rank) + sorted(carryover, key=context_rank)
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
