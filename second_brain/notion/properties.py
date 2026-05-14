"""Shared helpers for reading, writing, and querying Notion properties."""

from __future__ import annotations

from datetime import date
from typing import Any

from second_brain.notion import notion_call


def _plain_text(items: list[dict[str, Any]] | None) -> str:
    return "".join(
        item.get("plain_text") or (item.get("text") or {}).get("content") or ""
        for item in (items or [])
        if isinstance(item, dict)
    ).strip()


def extract_title(prop: dict[str, Any] | None) -> str:
    return _plain_text((prop or {}).get("title"))


def extract_rich_text(prop: dict[str, Any] | None) -> str:
    return _plain_text((prop or {}).get("rich_text"))


def extract_select(prop: dict[str, Any] | None) -> str:
    return (((prop or {}).get("select") or {}).get("name") or "").strip()


def extract_multi_select(prop: dict[str, Any] | None) -> list[str]:
    prop = prop or {}
    names = [
        item.get("name", "")
        for item in (prop.get("multi_select") or [])
        if item.get("name")
    ]
    if names:
        return names
    selected = extract_select(prop)
    if selected:
        return [selected]
    text = extract_rich_text(prop)
    if text:
        import re

        return [part.strip() for part in re.split(r"[,;/|]", text) if part.strip()]
    return []


def extract_date(prop: dict[str, Any] | None) -> str | None:
    date_value = (prop or {}).get("date")
    return date_value.get("start") if date_value else None


def extract_checkbox(prop: dict[str, Any] | None) -> bool:
    return bool((prop or {}).get("checkbox", False))


def extract_number(prop: dict[str, Any] | None) -> int | float | None:
    return (prop or {}).get("number")


def extract_formula(prop: dict[str, Any] | None) -> str | int | float | bool | None:
    formula = (prop or {}).get("formula") or {}
    return (
        formula.get("string")
        or formula.get("number")
        or formula.get("boolean")
        or (formula.get("date") or {}).get("start")
    )


def extract_plain_text(prop: dict[str, Any] | None) -> str:
    """Extract a readable string from common Notion property payloads."""
    if not prop:
        return ""
    prop_type = prop.get("type")
    if prop_type == "title" or prop.get("title") is not None:
        return extract_title(prop)
    if prop_type == "rich_text" or prop.get("rich_text") is not None:
        return extract_rich_text(prop)
    if prop_type == "select" or prop.get("select") is not None:
        return extract_select(prop)
    if prop_type == "multi_select" or prop.get("multi_select") is not None:
        return ", ".join(extract_multi_select(prop))
    if prop_type == "number" or prop.get("number") is not None:
        value = extract_number(prop)
        return "" if value is None else str(int(value) if isinstance(value, float) and value.is_integer() else value)
    if prop_type == "checkbox" or prop.get("checkbox") is not None:
        return str(extract_checkbox(prop))
    if prop_type == "date" or prop.get("date") is not None:
        return extract_date(prop) or ""
    if prop_type == "formula" or prop.get("formula") is not None:
        value = extract_formula(prop)
        return "" if value is None else str(value)
    if isinstance(prop.get("name"), str):
        return prop["name"].strip()
    return ""


def get_property_by_name(props: dict[str, Any], name: str) -> dict[str, Any] | None:
    """Return a property by exact name or whitespace/case-insensitive match."""
    prop = props.get(name)
    if prop is not None:
        return prop
    normalized_name = name.strip().casefold()
    for key, value in props.items():
        if key.strip().casefold() == normalized_name:
            return value
    return None


def title_prop(text: str) -> dict[str, Any]:
    return {"title": [dict(text={"content": text})]}


def rich_text_prop(text: str) -> dict[str, Any]:
    return {"rich_text": [dict(text={"content": text})]}


def select_prop(name: str) -> dict[str, Any]:
    return {"select": {"name": name}}


def multi_select_prop(names) -> dict[str, Any]:
    return {"multi_select": [{"name": name} for name in names]}


def date_prop(d: date | str) -> dict[str, Any]:
    return {"date": {"start": d.isoformat() if isinstance(d, date) else d}}


def checkbox_prop(val: bool) -> dict[str, Any]:
    return {"checkbox": bool(val)}


def number_prop(val) -> dict[str, Any]:
    return {"number": val}


def url_prop(val: str) -> dict[str, Any]:
    return {"url": val}


def _date_value(date_val: date | str) -> str:
    return date_val.isoformat() if isinstance(date_val, date) else date_val


def date_filter_equals(prop_name, date_val) -> dict[str, Any]:
    return {"property": prop_name, "date": {"equals": _date_value(date_val)}}


def date_filter_after(prop_name, date_val) -> dict[str, Any]:
    return {"property": prop_name, "date": {"after": _date_value(date_val)}}


def date_filter_before(prop_name, date_val) -> dict[str, Any]:
    return {"property": prop_name, "date": {"before": _date_value(date_val)}}


def date_filter_range(prop_name, start, end) -> dict[str, Any]:
    return {
        "and": [
            {"property": prop_name, "date": {"on_or_after": _date_value(start)}},
            {"property": prop_name, "date": {"on_or_before": _date_value(end)}},
        ]
    }


def checkbox_filter(prop_name, value: bool) -> dict[str, Any]:
    return {"property": prop_name, "checkbox": {"equals": bool(value)}}


def query_all(notion, database_id, filter=None, sorts=None, page_size=100) -> list[dict]:
    """Paginate through all results of a database query."""
    results: list[dict] = []
    cursor = None
    while True:
        kwargs: dict[str, Any] = {"database_id": database_id}
        if page_size is not None:
            kwargs["page_size"] = page_size
        if filter:
            kwargs["filter"] = filter
        if sorts:
            kwargs["sorts"] = sorts
        if cursor:
            kwargs["start_cursor"] = cursor
        response = notion_call(notion.databases.query, **kwargs)
        results.extend(response.get("results", []) or [])
        if response.get("has_more") is not True:
            break
        cursor = response.get("next_cursor")
        if not cursor:
            break
    return results
