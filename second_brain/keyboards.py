from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from second_brain.config import HORIZON_LABELS, HORIZON_DEADLINE_OFFSETS


def _clean_pid(pid: str) -> str: return pid.replace("-", "")

def _restore_pid(pid: str) -> str: return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"

def review_keyboard(page_id: str) -> InlineKeyboardMarkup:
    pid = _clean_pid(page_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Today", callback_data=f"h:{pid}:t"),
            InlineKeyboardButton("🟠 This Week", callback_data=f"h:{pid}:w"),
        ],
        [
            InlineKeyboardButton("🟡 This Month", callback_data=f"h:{pid}:m"),
            InlineKeyboardButton("⚪ Backburner", callback_data=f"h:{pid}:b"),
        ],
    ])

def habit_buttons(
    habits: list[dict],
    check_type: str,
    page: int = 0,
    page_size: int = 8,
    selected: set | None = None,
) -> InlineKeyboardMarkup:
    """Render habit buttons with multi-select checkmarks."""
    selected = selected or set()
    start = max(0, page) * page_size
    end = start + page_size
    page_habits = habits[start:end]

    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for habit in page_habits:
        p = _clean_pid(habit["page_id"])
        marker = "✅ " if habit["page_id"] in selected or p in selected else ""
        label = f"{marker}{habit['name']}"
        row.append(InlineKeyboardButton(label, callback_data=f"h:toggle:{p}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if len(habits) > page_size:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"hpag:{check_type}:{page-1}"))
        if end < len(habits):
            nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"hpag:{check_type}:{page+1}"))
        if nav:
            rows.append(nav)

    if selected:
        rows.append([InlineKeyboardButton(f"✅ Done ({len(selected)})", callback_data="h:done")])
    rows.append([InlineKeyboardButton("✖️ Cancel", callback_data="h:check:cancel")])

    return InlineKeyboardMarkup(rows)

def done_picker_keyboard(key: str, done_picker_map: dict[str, list[dict]], page: int = 0, page_size: int = 5) -> InlineKeyboardMarkup:
    tasks  = done_picker_map.get(key, [])
    start  = page * page_size
    end    = start + page_size
    total_pages = max(1, (len(tasks) + page_size - 1) // page_size)
    rows   = []
    for idx, task in enumerate(tasks[start:end], start=start):
        label = task["name"]
        if len(label) > 28:
            label = label[:25] + "..."
        rows.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"dp:{key}:{idx}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"dpp:{key}:{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data=f"noop:{key}"))
    if end < len(tasks):
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"dpp:{key}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("✖️ Cancel", callback_data=f"dpc:{key}")])
    return InlineKeyboardMarkup(rows)

def todo_picker_keyboard(key: str, todo_picker_map: dict[str, list[dict]], context_emoji_fn) -> InlineKeyboardMarkup:
    tasks = todo_picker_map.get(key, [])
    rows: list[list[InlineKeyboardButton]] = []
    for idx, task in enumerate(tasks):
        if task.get("_done"):
            continue
        label = f"{context_emoji_fn(task.get('context'))} {task.get('name', 'Untitled')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"td:{key}:{idx}")])
    rows.append([InlineKeyboardButton("✖️ Cancel", callback_data=f"tdc:{key}")])
    return InlineKeyboardMarkup(rows)

def notes_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📝 Quick Note", callback_data="nq:quick")],
            [InlineKeyboardButton("💡 Save Idea", callback_data="nq:idea")],
            [InlineKeyboardButton("💻 Save Code", callback_data="nq:code")],
            [InlineKeyboardButton("🔗 Save Link", callback_data="nq:link")],
            [InlineKeyboardButton("❌ Cancel", callback_data="nq:cancel")],
        ]
    )

def mute_options_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1 day", callback_data="mq:1"),
                InlineKeyboardButton("3 days", callback_data="mq:3"),
                InlineKeyboardButton("7 days", callback_data="mq:7"),
            ],
            [
                InlineKeyboardButton("Status", callback_data="mq:status"),
                InlineKeyboardButton("Unmute", callback_data="mq:unmute"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="mq:cancel")],
        ]
    )

def entertainment_confirm_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Yes", callback_data=f"el:{key}:yes")],
            [InlineKeyboardButton("❌ No", callback_data=f"el:{key}:no")],
        ]
    )

def wantslist_confirm_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Save to Wantslist", callback_data=f"wl_save:{key}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"wl_cancel:{key}"),
    ]])

def tmdb_candidates_keyboard(key: str, candidates: list[dict], notion_type_from_tmdb_fn) -> InlineKeyboardMarkup:
    rows = []
    for i, c in enumerate(candidates[:5]):
        label = f"{c['title']} ({c['year']}) · {notion_type_from_tmdb_fn(c['media_type'])}"
        if len(label) > 38:
            label = label[:35] + "..."
        rows.append([InlineKeyboardButton(label, callback_data=f"tmdb_pick:{key}:{i}")])
    rows.append([InlineKeyboardButton("➕ Save title only", callback_data=f"tmdb_skip:{key}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"tmdb_cancel:{key}")])
    return InlineKeyboardMarkup(rows)

def field_work_keyboard(key: str, trip_map: dict[str, dict]) -> InlineKeyboardMarkup:
    selected = trip_map[key].get("field_work_types", [])
    types = [
        ("sw", "Site Walk"),
        ("nm", "Noise Measurements"),
        ("vm", "Vibration Measurements"),
        ("rt", "RT Measurements"),
        ("it", "Isolation Testing"),
        ("hm", "24hr Monitoring"),
        ("nn", "None"),
    ]
    rows, row = [], []
    for slug, label in types:
        prefix = "✅ " if label in selected else ""
        row.append(InlineKeyboardButton(f"{prefix}{label}", callback_data=f"tw:{key}:{slug}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("✅ Done", callback_data=f"twd:{key}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"tcancel:{key}")])
    return InlineKeyboardMarkup(rows)

def format_command_palette() -> InlineKeyboardMarkup:
    """Returns the 6-button command palette."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Digest", callback_data="qp:digest"),
            InlineKeyboardButton("✅ To Do", callback_data="qp:todo"),
            InlineKeyboardButton("🎯 Habits", callback_data="qp:habits"),
        ],
        [
            InlineKeyboardButton("📝 Notes", callback_data="qp:notes"),
            InlineKeyboardButton("🌤️ Weather", callback_data="qp:weather"),
            InlineKeyboardButton("🔇 Mute", callback_data="qp:mute"),
        ],
    ])

def back_to_palette_keyboard() -> InlineKeyboardMarkup:
    """Single button to return to command palette."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Back to Palette", callback_data="qp:back")],
    ])

def quick_actions_keyboard(btn_refresh: str, btn_all_open: str, btn_habits: str, btn_crossfit: str, btn_notes: str, btn_weather: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[btn_refresh, btn_all_open, btn_habits], [btn_crossfit, btn_notes, btn_weather]],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Type a task, tap a quick action, or log a workout…",
    )

def horizon_view_back_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for expanded horizon views."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("← Back to Today", callback_data="digest:today")],
        ]
    )
