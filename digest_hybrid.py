"""Hybrid digest formatting and horizon helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

_get_all_active_tasks: Callable[[], list[dict]] | None = None
_num_emoji: Callable[[int], str] | None = None
_tz = None


def configure_digest_hybrid(*, get_all_active_tasks: Callable[[], list[dict]], num_emoji: Callable[[int], str], tz) -> None:
    """Bind runtime dependencies from the main bot module."""
    global _get_all_active_tasks, _num_emoji, _tz
    _get_all_active_tasks = get_all_active_tasks
    _num_emoji = num_emoji
    _tz = tz


def _parse_deadline(raw_deadline: str | None) -> date | None:
    if not raw_deadline:
        return None
    try:
        return datetime.fromisoformat(raw_deadline).date()
    except Exception:
        try:
            return date.fromisoformat(raw_deadline[:10])
        except Exception:
            return None


def _context_label(task: dict) -> str:
    return (task.get("context") or "").strip()


def _task_sort_key(task: dict) -> tuple[int, str, str]:
    deadline = _parse_deadline(task.get("deadline"))
    deadline_ord = deadline.toordinal() if deadline else 99999999
    context = (task.get("context") or "").lower()
    name = (task.get("name") or "").lower()
    return (deadline_ord, context, name)


def _require_runtime() -> tuple[Callable[[], list[dict]], Callable[[int], str]]:
    if _get_all_active_tasks is None or _num_emoji is None:
        raise RuntimeError("digest_hybrid runtime not configured")
    return _get_all_active_tasks, _num_emoji


def _get_tasks_by_deadline_horizon() -> tuple[list, list, list, list]:
    """
    Returns: (overdue, today, this_week, backlog)

    Overdue: deadline < date.today()
    Today: deadline == date.today()
    This Week: 1 <= days_until_deadline <= 7
    Backlog: days_until_deadline > 7 OR no deadline set
    """
    get_all_active_tasks, _ = _require_runtime()
    tasks = get_all_active_tasks()
    today = date.today()

    overdue: list[dict] = []
    today_tasks: list[dict] = []
    this_week: list[dict] = []
    backlog: list[dict] = []

    for task in tasks:
        deadline = _parse_deadline(task.get("deadline"))
        if deadline is None:
            backlog.append(task)
            continue

        if deadline < today:
            overdue.append(task)
            continue

        if deadline == today:
            today_tasks.append(task)
            continue

        days_until = (deadline - today).days
        if 1 <= days_until <= 7:
            this_week.append(task)
        else:
            backlog.append(task)

    return (
        sorted(overdue, key=_task_sort_key),
        sorted(today_tasks, key=_task_sort_key),
        sorted(this_week, key=_task_sort_key),
        sorted(backlog, key=_task_sort_key),
    )


def format_hybrid_digest(tasks: list[dict]) -> tuple[str, list[dict]]:
    """Main digest message with status peek and critical sections."""
    del tasks  # counts and sections are always computed fresh
    _, num_emoji = _require_runtime()
    overdue, today_tasks, this_week, backlog = _get_tasks_by_deadline_horizon()

    now_dt = datetime.now(_tz) if _tz is not None else datetime.now()
    date_str = now_dt.strftime("%A, %B %-d")

    summary_parts = []
    if overdue:
        summary_parts.append(f"{len(overdue)} overdue")
    if today_tasks:
        summary_parts.append(f"{len(today_tasks)} due today")
    if this_week:
        summary_parts.append(f"{len(this_week)} this week")
    if backlog:
        summary_parts.append(f"{len(backlog)} backlog")
    if not summary_parts:
        summary_parts = ["0 due today"]

    lines = [
        f"☀️ *{date_str}*",
        "",
        f"📊 {', '.join(summary_parts)}",
        "",
    ]

    ordered: list[dict] = []
    n = 1

    lines.append("🚨 *Overdue*")
    if overdue:
        for task in overdue:
            lines.append(f"{num_emoji(n)} {task['name']}  {_context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")
    lines.append("")

    lines.append("📌 *Due Today*")
    if today_tasks:
        for task in today_tasks:
            lines.append(f"{num_emoji(n)} {task['name']}  {_context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")

    lines.append("")
    if ordered:
        lines.append("_Reply `done 1`, `done 1,3`, or `done: task name` to complete_")

    return "\n".join(lines).strip(), ordered


def format_week_view(view_type: str) -> tuple[str, list[dict]]:
    """Return the This Week or Backlog expanded view."""
    _, num_emoji = _require_runtime()
    _, _, this_week, backlog = _get_tasks_by_deadline_horizon()

    if view_type == "week":
        title = "🟠 *This Week (2–7 days)*"
        tasks = this_week
        max_display = None
    elif view_type == "backlog":
        title = "⚪ *Backlog (7+ days)*"
        tasks = backlog
        max_display = 20
    else:
        raise ValueError("view_type must be 'week' or 'backlog'")

    lines = [title]
    if not tasks:
        lines.append("✅ Nothing — all clear!")
        lines.append("")
        lines.append("_Tap items below to adjust urgency 👇_")
        return "\n".join(lines), []

    shown = tasks
    hidden_count = 0
    if max_display is not None and len(tasks) > max_display:
        shown = tasks[:max_display]
        hidden_count = len(tasks) - max_display

    for i, task in enumerate(shown, 1):
        lines.append(f"{num_emoji(i)} {task['name']}  {_context_label(task)}")

    if hidden_count:
        lines.append("")
        lines.append(f"... and {hidden_count} more (view in Notion)")

    lines.append("")
    lines.append("_Tap items below to adjust urgency 👇_")

    return "\n".join(lines), shown


def quick_access_keyboard() -> InlineKeyboardMarkup:
    """Keyboard with live This Week and Backlog counts."""
    _, _, this_week, backlog = _get_tasks_by_deadline_horizon()
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(f"🟠 This Week ({len(this_week)})", callback_data="qv:week"),
            InlineKeyboardButton(f"⚪ Backlog ({len(backlog)})", callback_data="qv:backlog"),
        ]]
    )


def horizon_view_back_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for expanded horizon views."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("← Back to Today", callback_data="digest:today")],
            [InlineKeyboardButton("📅 Full Sunday Review", callback_data="digest:sunday")],
        ]
    )
