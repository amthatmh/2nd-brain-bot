from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path


def parse_hhmm_env(var_name: str, default: str, logger) -> tuple[int, int]:
    """Parse HH:MM env var with range checks and safe fallback."""
    raw = os.environ.get(var_name, default).strip()
    try:
        h_str, m_str = raw.split(":")
        hour, minute = int(h_str), int(m_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("out of range")
        return hour, minute
    except Exception:
        logger.warning(
            "Invalid %s=%r (expected HH:MM, 24h). Falling back to %s.",
            var_name,
            raw,
            default,
        )
        h_str, m_str = default.split(":")
        return int(h_str), int(m_str)


def parse_time_to_minutes(time_str: str | None) -> int:
    """Parse HH:MM to minutes since midnight; return -1 on invalid."""
    if not time_str:
        return -1
    try:
        hour_str, minute_str = str(time_str).strip().split(":")
        hour = int(hour_str)
        minute = int(minute_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return -1
        return hour * 60 + minute
    except Exception:
        return -1


def save_mute_state(mute_until, mute_state_file: Path, logger) -> None:
    try:
        payload = {"mute_until": mute_until.isoformat() if mute_until else None}
        mute_state_file.write_text(json.dumps(payload))
    except Exception as e:
        logger.error("Failed saving mute state: %s", e)


def load_mute_state(mute_state_file: Path, tz, logger):
    mute_until = None
    try:
        if not mute_state_file.exists():
            return None
        payload = json.loads(mute_state_file.read_text() or "{}")
        raw = payload.get("mute_until")
        if raw:
            mute_until = datetime.fromisoformat(raw)
        if mute_until and datetime.now(tz) >= mute_until:
            return None
        return mute_until
    except Exception as e:
        logger.error("Failed loading mute state: %s", e)
        return None


def is_muted(mute_until, tz) -> bool:
    if not mute_until:
        return False
    return datetime.now(tz) < mute_until


def format_reminder_snapshot(fmt_module, local_today_fn, notion, notion_db_id, tz, notion_tasks, *, mode: str = "priority", limit: int = 8) -> str:
    fmt_module.local_today = local_today_fn
    fmt_module.notion = notion
    fmt_module.NOTION_DB_ID = notion_db_id
    fmt_module.TZ = tz
    fmt_module.notion_tasks = notion_tasks
    return fmt_module.format_reminder_snapshot(mode=mode, limit=limit)
