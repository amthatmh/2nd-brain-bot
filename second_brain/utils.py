from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from second_brain.config import NUMBER_EMOJIS


_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)


class ExpiringDict(dict):
    """Simple in-memory dict with TTL-based expiry on read/write."""

    def __init__(self, ttl_seconds: int = 3600) -> None:
        super().__init__()
        self._ttl = ttl_seconds
        self._expiries: dict[Any, datetime] = {}

    def _purge(self) -> None:
        now = datetime.now(timezone.utc)
        stale = [k for k, exp in self._expiries.items() if exp <= now]
        for key in stale:
            self._expiries.pop(key, None)
            super().pop(key, None)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._purge()
        self._expiries[key] = datetime.now(timezone.utc) + timedelta(seconds=self._ttl)
        super().__setitem__(key, value)

    def __getitem__(self, key: Any) -> Any:
        self._purge()
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        self._purge()
        return super().get(key, default)


def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    today = date.today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


def split_tasks(text: str) -> list[str]:
    chunks = [c.strip(" -•\t") for c in _BULLET_RE.split(text) if c.strip()]
    if len(chunks) <= 1:
        lines = [l.strip(" -•\t") for l in text.splitlines() if l.strip()]
        return lines if len(lines) > 1 else [text.strip()]
    return chunks


def _normalize_task_name(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", text.lower())).strip()


def _clean_pid(pid: str) -> str:
    return pid.replace("-", "")


def _restore_pid(pid: str) -> str:
    return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def fuzzy_match(query: str, tasks: list[dict]) -> dict | None:
    nq = _normalize_task_name(query)
    for t in tasks:
        if nq == _normalize_task_name(t.get("name", "")):
            return t
    for t in tasks:
        if nq in _normalize_task_name(t.get("name", "")):
            return t
    return None


async def reply_notion_error(message_or_query: Any, context: str) -> None:
    target = message_or_query.message if hasattr(message_or_query, "message") else message_or_query
    await target.reply_text(f"⚠️ Couldn't {context} in Notion right now. Please try again.")


def done_picker_keyboard(key: str, tasks: list[dict], page: int = 0, page_size: int = 5) -> InlineKeyboardMarkup:
    start = page * page_size
    slice_ = tasks[start:start + page_size]
    rows = [[InlineKeyboardButton(f"{num_emoji(start + i + 1)} {t['name']}", callback_data=f"donepick|{key}|{start+i}")] for i, t in enumerate(slice_)]
    return InlineKeyboardMarkup(rows)
