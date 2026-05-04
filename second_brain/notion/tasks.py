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

