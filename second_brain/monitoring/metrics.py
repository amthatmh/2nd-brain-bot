"""Metrics helpers for operational summaries."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def count_entries_since(notion: Any, database_id: str, date_property: str, since: datetime) -> int:
    """Count Notion database entries whose date_property is on or after since."""
    if not database_id:
        return 0
    response = notion.databases.query(
        database_id=database_id,
        filter={"property": date_property, "date": {"on_or_after": since.date().isoformat()}},
        page_size=100,
    )
    total = len(response.get("results", []))
    while response.get("has_more"):
        response = notion.databases.query(
            database_id=database_id,
            filter={"property": date_property, "date": {"on_or_after": since.date().isoformat()}},
            start_cursor=response.get("next_cursor"),
            page_size=100,
        )
        total += len(response.get("results", []))
    return total


def generate_weekly_summary(notion: Any, *, task_db_id: str = "", log_db_id: str = "", notes_db_id: str = "") -> str:
    """Generate a simple weekly activity summary from implemented Notion counts."""
    since = datetime.now(timezone.utc) - timedelta(days=7)
    parts = [f"Since {since.date().isoformat()}:"]
    if task_db_id:
        parts.append(f"• Tasks touched: {count_entries_since(notion, task_db_id, 'Updated', since)}")
    if log_db_id:
        parts.append(f"• Habit/log entries: {count_entries_since(notion, log_db_id, 'Date', since)}")
    if notes_db_id:
        parts.append(f"• Notes captured: {count_entries_since(notion, notes_db_id, 'Created', since)}")
    if len(parts) == 1:
        parts.append("• No metrics databases configured.")
    return "\n".join(parts)
