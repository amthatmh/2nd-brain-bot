from datetime import date, timedelta

from utils.date_parser import parse_date


def test_parse_ambiguous_numeric_date_labels_both_options():
    parsed = parse_date("5/6", today=date(2026, 5, 8))

    assert parsed.ambiguous is True
    assert parsed.resolved is None
    assert parsed.option_a == "2026-05-06"
    assert parsed.option_b == "2026-06-05"
    assert parsed.label_a == "May 6"
    assert parsed.label_b == "Jun 5"


def test_parse_unambiguous_numeric_month_first():
    parsed = parse_date("5/13", today=date(2026, 5, 8))

    assert parsed.ambiguous is False
    assert parsed.resolved == "2026-05-13"


def test_parse_unambiguous_numeric_day_first():
    parsed = parse_date("13/5", today=date(2026, 5, 8))

    assert parsed.ambiguous is False
    assert parsed.resolved == "2026-05-13"


def test_parse_spelled_month():
    parsed = parse_date("May 6", today=date(2026, 5, 8))

    assert parsed.ambiguous is False
    assert parsed.resolved == "2026-05-06"


def test_parse_none_and_empty_default_to_today():
    today = date(2026, 5, 8)

    assert parse_date(None, today=today).resolved == today.isoformat()
    assert parse_date("", today=today).resolved == today.isoformat()


def test_parse_yesterday():
    today = date(2026, 5, 8)

    parsed = parse_date("yesterday", today=today)

    assert parsed.ambiguous is False
    assert parsed.resolved == (today - timedelta(days=1)).isoformat()


def test_parse_unambiguous_numeric_without_year_rolls_future_to_previous_year():
    parsed = parse_date("5/13", today=date(2026, 1, 8))

    assert parsed.ambiguous is False
    assert parsed.resolved == "2025-05-13"


def test_parse_explicit_year_allows_future():
    parsed = parse_date("5/13/2026", today=date(2026, 1, 8))

    assert parsed.ambiguous is False
    assert parsed.resolved == "2026-05-13"
