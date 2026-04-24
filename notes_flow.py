#!/usr/bin/env python3
"""Notes flow helpers for Second Brain bot."""

from __future__ import annotations

import re
from datetime import date, datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

NOTE_TOPICS = [
    "🎵 Acoustics",
    "💼 Work",
    "🏠 Personal",
    "💪 Health",
    "🏢 LEED",
    "✅ WELL",
    "💡 Ideas",
    "📚 Research",
]

_URL_RE = re.compile(r"(https?://[^\s]+)")
_RICH_TEXT_LIMIT = 2000


def split_kind_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Tasks", callback_data=f"kind_task:{key}"),
        InlineKeyboardButton("📝 Note", callback_data=f"kind_note:{key}"),
        InlineKeyboardButton("🔄", callback_data=f"kind_refresh:{key}"),
    ]])


def ordered_topics(topic_recency_map: dict[str, datetime]) -> list[str]:
    return sorted(
        NOTE_TOPICS,
        key=lambda topic: topic_recency_map.get(topic, datetime.min),
        reverse=True,
    )


def note_topics_keyboard(key: str, ordered: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(ordered), 2):
        row_topics = ordered[i:i + 2]
        rows.append([InlineKeyboardButton(t, callback_data=f"note_topic:{key}:{j}") for j, t in enumerate(row_topics, start=i)])
    rows.append([InlineKeyboardButton("⏭️ No topic", callback_data=f"note_topic:{key}:none")])
    return InlineKeyboardMarkup(rows)


def create_note_payload(content: str, topic: str | None = None) -> dict:
    clean = content.strip()
    first_line = next((line.strip() for line in clean.splitlines() if line.strip()), "Untitled")
    title = first_line[:80]
    url_match = _URL_RE.search(clean)
    link = url_match.group(1).rstrip(".,);]}>\"'") if url_match else None
    note_type = "🔗 Link/Article" if link else "📝 Quick Note"
    content_value = clean if clean else first_line
    content_chunks = [
        {"text": {"content": content_value[i:i + _RICH_TEXT_LIMIT]}}
        for i in range(0, len(content_value), _RICH_TEXT_LIMIT)
    ] or [{"text": {"content": "Untitled"}}]

    props: dict = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Content": {"rich_text": content_chunks},
        "Date Created": {"date": {"start": date.today().isoformat()}},
        "Type": {"select": {"name": note_type}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Processed": {"checkbox": False},
    }
    if topic:
        props["Topic"] = {"multi_select": [{"name": topic}]}
    if link:
        props["Link"] = {"url": link}
    return props
