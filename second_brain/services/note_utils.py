"""Pure note and formatting helpers extracted from main."""

from __future__ import annotations

import re


def extract_date_only(date_str: str | None) -> str | None:
    if not date_str:
        return None
    if len(date_str) >= 10 and date_str[4] == "-" and date_str[7] == "-":
        return date_str[:10]
    return date_str


def extract_url(text: str, url_re: re.Pattern[str]) -> str | None:
    m = url_re.search(text)
    return m.group(0) if m else None


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
