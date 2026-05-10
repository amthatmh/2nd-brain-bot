from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def get_today_tasks_for_palette(*, notion_tasks, notion, notion_db_id, local_today_fn) -> list[dict]:
    tasks = notion_tasks.get_today_and_overdue_tasks(notion, notion_db_id)
    today_str = local_today_fn().isoformat()
    return [t for t in tasks if t.get("deadline") == today_str]


def format_digest_view(*, notion_tasks, notion, notion_db_id, local_today_fn, back_to_palette_keyboard, weather_card: str = "") -> tuple[str, InlineKeyboardMarkup]:
    today = local_today_fn()
    cutoff = today + timedelta(days=7)
    tasks = notion_tasks.get_all_active_tasks(notion, notion_db_id)
    groups: dict[str, list[dict]] = defaultdict(list)
    beyond_count = 0

    for task in tasks:
        parsed_deadline = notion_tasks._parse_deadline(task.get("deadline"))
        if not parsed_deadline or parsed_deadline < today:
            continue
        if parsed_deadline <= cutoff:
            groups[parsed_deadline.isoformat()].append(task)
        else:
            beyond_count += 1

    lines = ["📖 Digest — Today + 7 Days"]
    if weather_card:
        lines.extend([weather_card, ""])
    else:
        lines.append("")
    if not groups:
        lines.append("✅ Clear for next 7 days!")
    else:
        for d in sorted(groups.keys()):
            day_tasks = sorted(groups[d], key=notion_tasks._task_sort_key)
            date_label = date.fromisoformat(d).strftime("%A, %B %-d")
            lines.append(f"📌 {date_label} ({len(day_tasks)})")
            for task in day_tasks:
                lines.append(f"  • {task.get('name', 'Untitled')}  {notion_tasks._context_label(task)}")
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    if beyond_count:
        lines.extend(["", f"...and {beyond_count} more beyond 7 days (view in Notion)"])

    return "\n".join(lines).strip(), back_to_palette_keyboard()


def format_todo_view(*, notion_tasks, notion, notion_db_id, local_today_fn, num_emoji, marked_done_indices: set | None = None) -> tuple[str, InlineKeyboardMarkup]:
    marked_done_indices = marked_done_indices or set()
    tasks = get_today_tasks_for_palette(notion_tasks=notion_tasks, notion=notion, notion_db_id=notion_db_id, local_today_fn=local_today_fn)
    lines = ["✅ Today's Tasks — Mark Complete", ""]
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    if not tasks:
        lines.append("✅ No tasks due today!")
    else:
        for idx, task in enumerate(tasks):
            label = task.get("name", "Untitled")
            if idx in marked_done_indices:
                lines.append(f"✅ {label}")
                continue
            keyboard_rows.append([InlineKeyboardButton(f"{num_emoji(idx + 1)} {label}", callback_data=f"qp:done:{idx}")])
        if len(marked_done_indices) >= len(tasks):
            lines.append("✅ All today's tasks marked done! 🎉")

    keyboard_rows.append([InlineKeyboardButton("📖 Back to Palette", callback_data="qp:back")])
    return "\n".join(lines).strip(), InlineKeyboardMarkup(keyboard_rows)


def quick_access_keyboard(*, notion_tasks, notion, notion_db_id) -> InlineKeyboardMarkup:
    _, _, this_week, backlog = notion_tasks._get_tasks_by_deadline_horizon(notion, notion_db_id)
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"🟠 This Week ({len(this_week)})", callback_data="qv:week"), InlineKeyboardButton(f"⚪ Backlog ({len(backlog)})", callback_data="qv:backlog")]])


def _normalize_keycap_digits(normalized: str) -> str:
    keycap_map = {"0️⃣": "0", "1️⃣": "1", "2️⃣": "2", "3️⃣": "3", "4️⃣": "4", "5️⃣": "5", "6️⃣": "6", "7️⃣": "7", "8️⃣": "8", "9️⃣": "9", "🔟": "10"}
    for keycap, digit in keycap_map.items():
        normalized = normalized.replace(keycap, digit)
    return normalized


def parse_done_numbers_command(text: str) -> list[int] | None:
    normalized = _normalize_keycap_digits(text.strip().lower())
    m = re.match(r"^(?:done|complete|finish|check(?:\s+off)?)\s+((?:\d+\s*(?:,|\band\b)?\s*)+)$", normalized, re.IGNORECASE)
    if not m:
        m = re.match(r"^mark\s+(?:done\s+)?((?:\d+\s*(?:,|\band\b)?\s*)+)\s+done$", normalized, re.IGNORECASE)
    if not m:
        return None
    nums = [int(n) for n in re.findall(r"\d+", m.group(1))]
    return nums or None


def parse_review_numbers_command(text: str) -> list[int] | None:
    normalized = _normalize_keycap_digits(text.strip().lower())
    m = re.match(r"^(?:review|reassign|horizon|check(?:\s+off)?)\s+((?:\d+\s*(?:,|\band\b)?\s*)+)$", normalized, re.IGNORECASE)
    if not m:
        m = re.match(r"^mark\s+(?:review\s+)?((?:\d+\s*(?:,|\band\b)?\s*)+)\s+review$", normalized, re.IGNORECASE)
    if not m:
        return None
    nums = [int(n) for n in re.findall(r"\d+", m.group(1))]
    return nums or None
