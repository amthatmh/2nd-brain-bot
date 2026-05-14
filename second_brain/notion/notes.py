"""Notes-related Notion helpers."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from second_brain.notes_flow import create_note_payload
from second_brain.notion.properties import rich_text_prop, title_prop


def fetch_note_topics_from_notion(notion, notion_notes_db: str) -> list[str]:
    """Read Topic multi-select options directly from the Notion Notes DB schema."""
    if not notion_notes_db:
        return []

    db = notion.databases.retrieve(database_id=notion_notes_db)
    topic_prop = db.get("properties", {}).get("Topic", {})
    if topic_prop.get("type") != "multi_select":
        return []

    options = topic_prop.get("multi_select", {}).get("options", [])
    return [opt.get("name", "").strip() for opt in options if opt.get("name", "").strip()]


def save_note(notion, notion_notes_db: str, title: str, url: str | None, content: str, topics: list[str], note_type: str) -> str:
    """Write a note to the 📒 Notes Notion DB. Returns page_id."""
    today = date.today().isoformat()
    props: dict[str, Any] = {
        "Title": title_prop(title or "Untitled"),
        "Type": {"select": {"name": note_type}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Date Created": {"date": {"start": today}},
        "Processed": {"checkbox": False},
    }
    if url:
        props["Link"] = {"url": url}
    if content:
        props["Content"] = rich_text_prop(content[:2000])
    if topics:
        props["Topic"] = {"multi_select": [{"name": t} for t in topics]}
    page = notion.pages.create(
        parent={"database_id": notion_notes_db},
        properties=props,
    )
    return page["id"]


def create_note_entry(notion, notion_notes_db: str, content: str, topic: str | None = None) -> str:
    if not notion_notes_db:
        raise ValueError("NOTION_NOTES_DB is not configured")
    base_props = create_note_payload(content, topic=topic)
    db = notion.databases.retrieve(database_id=notion_notes_db)
    schema_props = db.get("properties", {})

    def schema_type(prop_name: str) -> str | None:
        return schema_props.get(prop_name, {}).get("type")

    props: dict[str, Any] = {}

    # Map title payload to whichever title property exists in the DB.
    title_payload = base_props.get("Title")
    title_prop_name = next((name for name, p in schema_props.items() if p.get("type") == "title"), None)
    if title_payload and title_prop_name:
        props[title_prop_name] = title_payload

    if "Content" in base_props and schema_type("Content") == "rich_text":
        props["Content"] = base_props["Content"]
    if "Date Created" in base_props and schema_type("Date Created") == "date":
        props["Date Created"] = base_props["Date Created"]
    if "Processed" in base_props and schema_type("Processed") == "checkbox":
        props["Processed"] = base_props["Processed"]
    if "Link" in base_props and schema_type("Link") == "url":
        props["Link"] = base_props["Link"]

    if "Type" in base_props and schema_type("Type") == "select":
        desired = base_props["Type"]["select"]["name"]
        options = schema_props["Type"].get("select", {}).get("options", [])
        names = {o.get("name") for o in options}
        if desired in names:
            props["Type"] = base_props["Type"]
        elif options:
            props["Type"] = {"select": {"name": options[0]["name"]}}

    if "Source" in base_props and schema_type("Source") == "select":
        desired = base_props["Source"]["select"]["name"]
        options = schema_props["Source"].get("select", {}).get("options", [])
        names = {o.get("name") for o in options}
        if desired in names:
            props["Source"] = base_props["Source"]
        elif options:
            props["Source"] = {"select": {"name": options[0]["name"]}}

    if "Topic" in base_props and schema_type("Topic") == "multi_select":
        desired_topics = [t.get("name") for t in base_props["Topic"].get("multi_select", []) if t.get("name")]
        options = schema_props["Topic"].get("multi_select", {}).get("options", [])
        names = {o.get("name") for o in options}
        selected = [{"name": t} for t in desired_topics if t in names]
        # Notion can create missing multi_select options on write, so include any
        # user-provided topics that are not in the current schema yet.
        props["Topic"] = {"multi_select": selected or [{"name": t} for t in desired_topics]}

    if not props:
        raise ValueError("Notes DB schema has no writable matching properties for note payload")

    page = notion.pages.create(parent={"database_id": notion_notes_db}, properties=props)
    return page["id"]
