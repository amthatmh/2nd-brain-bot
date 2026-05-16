"""Boot/startup logging helpers."""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime

from second_brain.notion.properties import rich_text_prop, title_prop

log = logging.getLogger(__name__)


def git_sha() -> str:
    """Best-effort short commit SHA for deploy receipts."""
    for env_key in (
        "RAILWAY_GIT_COMMIT_SHA",
        "GIT_SHA",
        "RENDER_GIT_COMMIT",
        "COMMIT_SHA",
        "SOURCE_VERSION",
    ):
        val = os.environ.get(env_key, "").strip()
        if val:
            return val[:12]
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


async def write_boot_log(
    bot,
    version: str,
    sha: str,
    asana_status: str,
    features: str,
    status: str = "ok",
    notes: str = "",
    *,
    notion,
    boot_log_db: str,
    tz,
) -> None:
    """
    Write a boot record to the 🖥️ Boot Log Notion DB.
    Silent — never raises, never sends Telegram.
    Falls back gracefully if NOTION_BOOT_LOG_DB is not configured.
    """
    if not boot_log_db:
        log.warning("write_boot_log: NOTION_BOOT_LOG_DB not configured, skipping")
        return
    try:
        props = {
            "Version": title_prop(version),
            "Boot Time": {
                "date": {"start": datetime.now(tz).isoformat()}
            },
            "Status": {
                "select": {"name": status}
            },
            "SHA": rich_text_prop(sha),
            "Asana": rich_text_prop(asana_status),
            "Features": rich_text_prop(features),
            "Timezone": rich_text_prop(str(tz)),
        }
        if notes:
            props["Notes"] = rich_text_prop(notes[:2000])
        notion.pages.create(
            parent={"database_id": boot_log_db},
            properties=props,
        )
        log.info("Boot log written to Notion: %s %s", version, sha)
    except Exception as e:
        log.error("write_boot_log: failed to write to Notion: %s", e)
