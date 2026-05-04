"""Helpers for tracking and formatting sync telemetry."""

from __future__ import annotations

import json
from datetime import datetime, timezone


def init_sync_status() -> dict[str, dict]:
    return {
        "asana": {"last_run": None, "ok": None, "error": None, "stats": None},
        "cinema": {"last_run": None, "ok": None, "error": None, "stats": None},
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_sync_block(name: str, info: dict) -> str:
    ok = info.get("ok")
    if ok is True:
        state = "✅ OK"
    elif ok is False:
        state = "❌ Failed"
    else:
        state = "— Not yet run"
    last_run = info.get("last_run") or "n/a"
    error = info.get("error")
    stats = info.get("stats")
    lines = [f"*{name}*: {state}", f"last_run: `{last_run}`"]
    if stats:
        lines.append(f"stats: `{json.dumps(stats, separators=(',', ':'))}`")
    if error:
        lines.append(f"error: `{error}`")
    return "\n".join(lines)


def format_sync_status_message(sync_status: dict[str, dict]) -> str:
    msg = [
        "📊 *Sync Status*",
        "",
        format_sync_block("Asana", sync_status["asana"]),
        "",
        format_sync_block("Cinema", sync_status["cinema"]),
    ]
    return "\n".join(msg)
