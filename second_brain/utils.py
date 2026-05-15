from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

log = logging.getLogger(__name__)

NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
from second_brain.services.task_parsing import split_tasks  # noqa: F401


def local_today(tz: ZoneInfo | None = None) -> date:
    """Return today's date in the app timezone (or tz if provided)."""
    if tz is None:
        from second_brain.config import TZ

        tz = TZ
    return datetime.now(tz).date()


def get_current_monday() -> date:
    from second_brain.config import TZ

    today = datetime.now(TZ).date()
    return today - timedelta(days=today.weekday())



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



def parse_time_to_minutes(time_str: str | None) -> int:
    """Parse HH:MM to minutes since midnight; return -1 on invalid."""
    if not time_str:
        return -1
    try:
        hour_str, minute_str = str(time_str).strip().split(":")
        hour = int(hour_str)
        minute = int(minute_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return -1
        return hour * 60 + minute
    except Exception:
        return -1

def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."


def next_weekday(weekday: int) -> date:
    today = local_today()
    days_ahead = (weekday - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return today + timedelta(days=days_ahead)


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
