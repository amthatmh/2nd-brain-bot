"""Shared date parsing helpers with numeric ambiguity detection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
import calendar
import re


@dataclass
class DateParseResult:
    resolved: Optional[str]
    ambiguous: bool
    option_a: Optional[str]
    option_b: Optional[str]
    label_a: Optional[str]
    label_b: Optional[str]


_MONTHS = {
    name.lower(): idx
    for idx, name in enumerate(calendar.month_name)
    if name
} | {
    name.lower(): idx
    for idx, name in enumerate(calendar.month_abbr)
    if name
}
_WEEKDAYS = {name.lower(): idx for idx, name in enumerate(calendar.day_name)}
_WEEKDAYS.update({name[:3].lower(): idx for idx, name in enumerate(calendar.day_name)})


def _empty_result(resolved: str | None = None) -> DateParseResult:
    return DateParseResult(
        resolved=resolved,
        ambiguous=False,
        option_a=None,
        option_b=None,
        label_a=None,
        label_b=None,
    )


def _format_label(value: date) -> str:
    return f"{calendar.month_abbr[value.month]} {value.day}"


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _avoid_future(value: date, today: date, *, explicit_year: bool) -> date:
    if explicit_year or value <= today or value.month == today.month:
        return value
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        # Feb 29 -> Feb 28 when rolling back to a non-leap year.
        return value.replace(year=value.year - 1, day=28)


def _from_components(year: int, month: int, day: int, today: date, *, explicit_year: bool) -> DateParseResult | None:
    value = _safe_date(year, month, day)
    if not value:
        return None
    return _empty_result(_avoid_future(value, today, explicit_year=explicit_year).isoformat())


def _parse_relative(text: str, today: date) -> DateParseResult | None:
    normalized = re.sub(r"\s+", " ", text.lower()).strip()
    if normalized in {"today", "tod"}:
        return _empty_result(today.isoformat())
    if normalized == "yesterday":
        return _empty_result((today - timedelta(days=1)).isoformat())
    if normalized == "tomorrow":
        return _empty_result((today + timedelta(days=1)).isoformat())

    m = re.search(r"\blast\s+([a-z]+)\b", normalized)
    if m and m.group(1) in _WEEKDAYS:
        target = _WEEKDAYS[m.group(1)]
        days_back = (today.weekday() - target) % 7 or 7
        return _empty_result((today - timedelta(days=days_back)).isoformat())
    return None


def _parse_spelled_month(text: str, today: date) -> DateParseResult | None:
    month_names = "|".join(sorted((re.escape(k) for k in _MONTHS), key=len, reverse=True))
    month_first = re.search(
        rf"\b(?P<month>{month_names})\.?\s+(?P<day>\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(?P<year>\d{{4}}))?\b",
        text,
        flags=re.IGNORECASE,
    )
    if month_first:
        month = _MONTHS[month_first.group("month").lower().rstrip(".")]
        day = int(month_first.group("day"))
        explicit_year = bool(month_first.group("year"))
        year = int(month_first.group("year")) if explicit_year else today.year
        return _from_components(year, month, day, today, explicit_year=explicit_year)

    day_first = re.search(
        rf"\b(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+(?P<month>{month_names})\.?(?:,?\s+(?P<year>\d{{4}}))?\b",
        text,
        flags=re.IGNORECASE,
    )
    if day_first:
        month = _MONTHS[day_first.group("month").lower().rstrip(".")]
        day = int(day_first.group("day"))
        explicit_year = bool(day_first.group("year"))
        year = int(day_first.group("year")) if explicit_year else today.year
        return _from_components(year, month, day, today, explicit_year=explicit_year)
    return None


def _parse_numeric(text: str, today: date) -> DateParseResult | None:
    # Year-first ISO-ish forms are explicit and unambiguous.
    m = re.search(r"\b(?P<year>\d{4})[/-](?P<month>\d{1,2})[/-](?P<day>\d{1,2})\b", text)
    if m:
        return _from_components(int(m.group("year")), int(m.group("month")), int(m.group("day")), today, explicit_year=True)

    # Slash date with trailing year, e.g. 5/6/2026 or 13/5/2026.
    m = re.search(r"\b(?P<x>\d{1,2})[/-](?P<y>\d{1,2})[/-](?P<year>\d{2,4})\b", text)
    if m:
        x, y = int(m.group("x")), int(m.group("y"))
        year = int(m.group("year"))
        if year < 100:
            year += 2000
        if 1 <= x <= 12:
            return _from_components(year, x, y, today, explicit_year=True)
        if 1 <= y <= 12:
            return _from_components(year, y, x, today, explicit_year=True)
        return None

    m = re.search(r"\b(?P<x>\d{1,2})[/-](?P<y>\d{1,2})\b", text)
    if not m:
        return None
    x, y = int(m.group("x")), int(m.group("y"))
    x_is_month = 1 <= x <= 12
    y_is_month = 1 <= y <= 12

    if x_is_month and y_is_month:
        option_a = _safe_date(today.year, x, y)
        option_b = _safe_date(today.year, y, x)
        if option_a and option_b and option_a != option_b:
            return DateParseResult(
                resolved=None,
                ambiguous=True,
                option_a=option_a.isoformat(),
                option_b=option_b.isoformat(),
                label_a=_format_label(option_a),
                label_b=_format_label(option_b),
            )
        if option_a:
            return _empty_result(_avoid_future(option_a, today, explicit_year=False).isoformat())

    if x_is_month:
        return _from_components(today.year, x, y, today, explicit_year=False)
    if y_is_month:
        return _from_components(today.year, y, x, today, explicit_year=False)
    return None


def parse_date(text: str | None, today: Optional[date] = None) -> DateParseResult:
    """
    Parse a date string from natural language input.

    Rules:
    - No date mentioned → today (resolved, not ambiguous)
    - Spelled out month (May 6, 6 May, June 5) → unambiguous
    - Relative (yesterday, last Tuesday) → unambiguous
    - Numeric X/Y or X-Y where BOTH X<=12 AND Y<=12 → ambiguous
    - Numeric X/Y where only one is a valid month → unambiguous
    - Year included (5/6/2026, 2026-05-06) → unambiguous
    """
    today = today or date.today()
    raw = (text or "").strip()
    if not raw:
        return _empty_result(today.isoformat())

    for parser in (_parse_relative, _parse_spelled_month, _parse_numeric):
        parsed = parser(raw, today)
        if parsed:
            return parsed
    return _empty_result(today.isoformat())


# TEST: parse_date("5/6")   → ambiguous, label_a="May 6", label_b="Jun 5"
# TEST: parse_date("5/13")  → resolved="YYYY-05-13", not ambiguous
# TEST: parse_date("13/5")  → resolved="YYYY-05-13", not ambiguous
# TEST: parse_date("May 6") → resolved="YYYY-05-06", not ambiguous
# TEST: parse_date(None)    → resolved=today.isoformat(), not ambiguous
# TEST: parse_date("")      → resolved=today.isoformat(), not ambiguous
# TEST: parse_date("yesterday") → resolved=yesterday.isoformat(), not ambiguous
