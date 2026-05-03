"""Habit-related Notion helpers."""

from __future__ import annotations

import re
from typing import Any


def _plain_text_from_property(prop: dict[str, Any] | None) -> str:
    """Extract readable text from a Notion property payload."""
    if not prop:
        return ""

    prop_type = prop.get("type")
    if prop_type == "select":
        return ((prop.get("select") or {}).get("name") or "").strip()
    if prop_type == "rich_text":
        rich = prop.get("rich_text") or []
        return "".join(
            (item.get("plain_text") or (item.get("text") or {}).get("content") or "")
            for item in rich
            if isinstance(item, dict)
        ).strip()
    if prop_type == "number":
        value = prop.get("number")
        if isinstance(value, (int, float)):
            return str(int(value))
    if prop_type == "formula":
        formula = prop.get("formula") or {}
        if formula.get("type") == "number" and isinstance(formula.get("number"), (int, float)):
            return str(int(formula["number"]))
        if formula.get("type") == "string":
            return (formula.get("string") or "").strip()

    # Legacy / loose payloads in tests and integrations.
    if isinstance(prop.get("number"), (int, float)):
        return str(int(prop["number"]))
    if isinstance(prop.get("name"), str):
        return prop["name"].strip()
    return ""


def extract_habit_frequency(props: dict[str, Any]) -> int | None:
    """Return weekly frequency target from a Notion habit page properties dict."""
    for field in ("Frequency Per Week", "Frequency"):
        text = _plain_text_from_property(props.get(field))
        if not text:
            continue
        match = re.search(r"\d+", text)
        if match:
            value = int(match.group(0))
            if value > 0:
                return value

    label = _plain_text_from_property(props.get("Frequency Label"))
    if label:
        match = re.search(r"\d+", label)
        if match:
            value = int(match.group(0))
            if value > 0:
                return value

    return None
