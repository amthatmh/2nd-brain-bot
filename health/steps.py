import os
import logging
from datetime import date, datetime

import pytz
from notion_client import Client as NotionClient

log = logging.getLogger(__name__)

TZ = pytz.timezone(os.environ.get("TIMEZONE", "America/Chicago"))
NOTION_STEPS_DB = os.environ.get("NOTION_STEPS_DB") or os.environ.get("NOTION_LOG_DB", "")

_notion = NotionClient(auth=os.environ["NOTION_TOKEN"])


def parse_log_date(date_str: str) -> date:
    """Parse a Health Auto Export date string to a local date."""
    dt_utc = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
    return pytz.utc.localize(dt_utc).astimezone(TZ).date()


def upsert_steps(log_date: date, steps: int, log_db_id: str = NOTION_STEPS_DB) -> str:
    """
    Upsert a daily steps row in Notion.

    Returns 'created', 'updated_by_name', or 'updated_by_date'.
    """
    title = f"{log_date.isoformat()} Log"
    date_iso = log_date.isoformat()
    props = {"Steps": {"number": steps}}

    # Strategy 1: name match
    res = _notion.databases.query(
        database_id=log_db_id,
        filter={"property": "Name", "title": {"equals": title}},
    )
    pages = res.get("results", [])
    if pages:
        _notion.pages.update(page_id=pages[0]["id"], properties=props)
        log.info("Steps upsert: updated_by_name  date=%s  steps=%d", log_date, steps)
        return "updated_by_name"

    # Strategy 2: date property fallback
    res = _notion.databases.query(
        database_id=log_db_id,
        filter={"property": "Date", "date": {"equals": date_iso}},
    )
    pages = res.get("results", [])
    if pages:
        _notion.pages.update(page_id=pages[0]["id"], properties=props)
        log.info("Steps upsert: updated_by_date  date=%s  steps=%d", log_date, steps)
        return "updated_by_date"

    # Strategy 3: create new row
    _notion.pages.create(
        parent={"database_id": log_db_id},
        properties={
            "Name": {"title": [{"text": {"content": title}}]},
            "Date": {"date": {"start": date_iso}},
            **props,
        },
    )
    log.info("Steps upsert: created  date=%s  steps=%d", log_date, steps)
    return "created"
