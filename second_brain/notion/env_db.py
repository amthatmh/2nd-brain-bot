"""Helpers for reading and writing scalar values in the Notion ENV database."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Optional

from notion_client import Client as NotionClient

from second_brain.notion import notion_call
from second_brain.notion.properties import rich_text_prop, title_prop

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _notion_client() -> NotionClient:
    """Build the Notion client lazily so imports do not require env vars."""
    token = os.environ.get("NOTION_TOKEN", "").strip()
    if not token:
        raise RuntimeError("NOTION_TOKEN is not configured")
    return NotionClient(auth=token)


def _env_db_id() -> str:
    """Return configured ENV database id."""
    return os.environ.get("ENV_DB_ID", "").strip()


def _extract_text(prop: dict[str, Any]) -> str:
    """Extract plain text from common Notion text-like property payloads."""
    for prop_type in ("title", "rich_text"):
        chunks = prop.get(prop_type) or []
        text = "".join(chunk.get("plain_text") or chunk.get("text", {}).get("content", "") for chunk in chunks)
        if text:
            return text.strip()
    return ""


def _find_env_page(key: str) -> Optional[dict[str, Any]]:
    """Find a row in the ENV DB by Name, tolerating title or rich_text schemas."""
    database_id = _env_db_id()
    if not database_id:
        log.warning("ENV_DB_ID is not configured; cannot read/write %s", key)
        return None

    notion = _notion_client()
    for filter_type in ("title", "rich_text"):
        try:
            response = notion_call(
                notion.databases.query,
                database_id=database_id,
                filter={"property": "Name", filter_type: {"equals": key}},
                page_size=1,
            )
            rows = response.get("results", [])
            if rows:
                return rows[0]
        except Exception as exc:  # noqa: BLE001 - fallback supports alternate schemas
            log.debug("ENV DB lookup for %s with %s filter failed: %s", key, filter_type, exc)
    return None


def get_env_value(key: str) -> Optional[str]:
    """Get a scalar value from the Notion ENV DB by row Name."""
    try:
        page = _find_env_page(key)
        if not page:
            return None
        value = _extract_text(page.get("properties", {}).get("Value", {}))
        return value or None
    except Exception as exc:  # noqa: BLE001 - metrics tracking should not crash callers
        log.warning("Failed to read ENV DB value %s: %s", key, exc)
        return None


def set_env_value(key: str, value: str) -> bool:
    """Set a scalar value in the Notion ENV DB, creating the row when needed."""
    database_id = _env_db_id()
    if not database_id:
        log.warning("ENV_DB_ID is not configured; cannot set %s", key)
        return False

    try:
        notion = _notion_client()
        properties = {"Value": rich_text_prop(str(value))}
        page = _find_env_page(key)
        if page:
            notion_call(notion.pages.update, page_id=page["id"], properties=properties)
        else:
            notion_call(
                notion.pages.create,
                parent={"database_id": database_id},
                properties={
                    "Name": title_prop(key),
                    **properties,
                },
            )
        return True
    except Exception as exc:  # noqa: BLE001 - metrics tracking should not crash callers
        log.warning("Failed to set ENV DB value %s: %s", key, exc)
        return False
