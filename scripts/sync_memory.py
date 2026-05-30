#!/usr/bin/env python3
"""Sync weekly context from Notion into Claude Code memory files.

Run manually or via cron every Sunday after the weekly health insight job:
    python scripts/sync_memory.py

Reads:
  - HEALTH_PROFILE from Notion ENV DB (written by weekly_health_insight job)
  - Active/overdue tasks from NOTION_DB_ID
  - Habit completion for the past 7 days from NOTION_LOG_DB

Writes:
  - memory/health_profile.md
  - memory/weekly_progress.md
  - Updates memory/MEMORY.md index
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Resolve paths
REPO_ROOT = Path(__file__).parent.parent
MEMORY_DIR = Path.home() / ".claude" / "projects" / "-Users-atmh-Documents-Codex-2nd-brain-bot-local" / "memory"

CONTEXT_EMOJI = {
    "work": "💼",
    "personal": "🏠",
    "health": "🏃",
    "hk": "🤝",
}


def _notion_client():
    from notion_client import Client
    token = os.environ.get("NOTION_TOKEN", "").strip()
    if not token:
        raise RuntimeError("NOTION_TOKEN is not set")
    return Client(auth=token)


def fetch_health_profile() -> str | None:
    sys.path.insert(0, str(REPO_ROOT))
    from second_brain.notion.env_db import get_env_value
    return get_env_value("HEALTH_PROFILE")


def fetch_active_tasks(notion, db_id: str) -> list[dict]:
    from second_brain.notion.tasks import get_today_and_overdue_tasks, get_all_active_tasks
    # Get overdue + today tasks first for priority, then remaining active
    priority = get_today_and_overdue_tasks(notion, db_id, limit=None)
    all_active = get_all_active_tasks(notion, db_id)
    # Merge, dedup by name, priority tasks first
    seen = {t["name"] for t in priority}
    rest = [t for t in all_active if t["name"] not in seen]
    return priority + rest[:10]  # cap at ~20 total


def fetch_habit_completion(notion, log_db_id: str) -> dict[str, tuple[int, int]]:
    """Return {habit_name: (completed_count, total_count)} for the past 7 days."""
    from second_brain.notion.properties import date_filter_range, query_all

    today = date.today()
    start = today - timedelta(days=6)
    rows = query_all(
        notion,
        log_db_id,
        filter=date_filter_range("Date", start, today),
    )

    totals: dict[str, list] = defaultdict(lambda: [0, 0])  # [completed, total]
    for row in rows:
        props = row.get("properties", {})
        # Entry title gives habit name
        entry_chunks = (props.get("Entry") or {}).get("title") or []
        name = "".join(c.get("plain_text", "") for c in entry_chunks).strip()
        if not name:
            continue
        completed = (props.get("Completed") or {}).get("checkbox", False)
        totals[name][1] += 1
        if completed:
            totals[name][0] += 1

    return {k: (v[0], v[1]) for k, v in totals.items()}


def write_health_profile(profile_text: str, today: str) -> None:
    path = MEMORY_DIR / "health_profile.md"
    content = f"""---
name: health-profile
description: Current health status snapshot — HRV, RHR, sleep, VO2 Max, training frequency. Updated every Sunday by the weekly insight bot.
metadata:
  type: user
---

{profile_text}
Updated: {today}
"""
    path.write_text(content)
    print(f"  wrote {path}")


def write_weekly_progress(tasks: list[dict], habits: dict[str, tuple[int, int]], today: str) -> None:
    path = MEMORY_DIR / "weekly_progress.md"

    # Format tasks
    if tasks:
        task_lines = []
        for t in tasks:
            emoji = CONTEXT_EMOJI.get((t.get("context") or "").lower(), "📝")
            deadline = t.get("deadline")
            suffix = f" (due {deadline})" if deadline else ""
            task_lines.append(f"- {emoji} {t['name']}{suffix}")
        tasks_block = "\n".join(task_lines)
    else:
        tasks_block = "- None outstanding"

    # Format habits
    if habits:
        habit_lines = [f"- {name}: {done}/{total}" for name, (done, total) in sorted(habits.items())]
        habits_block = "\n".join(habit_lines)
    else:
        habits_block = "- No data"

    content = f"""---
name: weekly-progress
description: Current active tasks and habit completion rates this week. Updated every Sunday.
metadata:
  type: project
---

As of {today}:

**Active/overdue tasks:**
{tasks_block}

**Habit completion this week (past 7 days):**
{habits_block}
"""
    path.write_text(content)
    print(f"  wrote {path}")


def ensure_memory_index() -> None:
    """Add health_profile and weekly_progress entries to MEMORY.md if missing."""
    index_path = MEMORY_DIR / "MEMORY.md"
    if not index_path.exists():
        return

    text = index_path.read_text()
    entries = {
        "health_profile.md": "- [Health profile](health_profile.md) — current HRV, sleep, VO2 Max, training snapshot (updated Sundays)",
        "weekly_progress.md": "- [Weekly progress](weekly_progress.md) — active tasks and habit completion this week (updated Sundays)",
    }
    changed = False
    for filename, line in entries.items():
        if filename not in text:
            text = text.rstrip() + f"\n{line}\n"
            changed = True

    if changed:
        index_path.write_text(text)
        print(f"  updated {index_path}")


def main() -> None:
    today = date.today().isoformat()
    print(f"sync_memory — {today}")

    MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    # Add repo to path for second_brain imports
    sys.path.insert(0, str(REPO_ROOT))

    notion = _notion_client()
    log_db_id = os.environ.get("NOTION_LOG_DB", "").strip()
    tasks_db_id = os.environ.get("NOTION_DB_ID", "").strip()

    # Health profile
    print("fetching HEALTH_PROFILE...")
    profile = fetch_health_profile()
    if profile:
        write_health_profile(profile, today)
    else:
        print("  HEALTH_PROFILE not found in Notion ENV DB — skipping health_profile.md")

    # Tasks
    if tasks_db_id:
        print("fetching active tasks...")
        tasks = fetch_active_tasks(notion, tasks_db_id)
        print(f"  {len(tasks)} tasks found")
    else:
        print("  NOTION_DB_ID not set — skipping tasks")
        tasks = []

    # Habits
    if log_db_id:
        print("fetching habit completion...")
        habits = fetch_habit_completion(notion, log_db_id)
        print(f"  {len(habits)} habit(s): {', '.join(f'{k} {d}/{t}' for k, (d, t) in habits.items())}")
    else:
        print("  NOTION_LOG_DB not set — skipping habits")
        habits = {}

    write_weekly_progress(tasks, habits, today)
    ensure_memory_index()
    print("done.")


if __name__ == "__main__":
    main()
