from __future__ import annotations

import re


def load_digest_slots(*, rows: list[dict], logger) -> list[dict]:
    """Build normalized digest slots from Digest Selector rows."""
    context_map = {
        "🏠 Personal": "🏠 Personal",
        "💼 Work": "💼 Work",
        "🏃 Health": "🏃 Health",
        "🤝 HK": "🤝 HK",
    }

    def first_text(prop: dict) -> str:
        rich_text = prop.get("rich_text", [])
        if rich_text:
            return (rich_text[0].get("plain_text") or "").strip()
        title = prop.get("title", [])
        if title:
            return (title[0].get("plain_text") or "").strip()
        select = prop.get("select")
        if select and select.get("name"):
            return (select.get("name") or "").strip()
        date_value = prop.get("date") or {}
        if isinstance(date_value, dict) and date_value.get("start"):
            return str(date_value.get("start")).strip()
        return ""

    def normalize_slot_time(raw: str) -> str | None:
        value = (raw or "").strip()
        if not value:
            return None

        iso_match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", value)
        if iso_match:
            hh = int(iso_match.group(1))
            mm = int(iso_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
            return None

        ampm_match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])", value)
        if ampm_match:
            hh = int(ampm_match.group(1))
            mm = int(ampm_match.group(2))
            ampm = ampm_match.group(3).lower()
            if not (1 <= hh <= 12 and 0 <= mm <= 59):
                return None
            hh = (0 if hh == 12 else hh) if ampm == "am" else (12 if hh == 12 else hh + 12)
            return f"{hh:02d}:{mm:02d}"

        dt_match = re.search(r"T(\d{2}):(\d{2})", value)
        if dt_match:
            hh = int(dt_match.group(1))
            mm = int(dt_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"

        internal_match = re.search(r"\b(\d{1,2}):(\d{2})\b", value)
        if internal_match:
            hh = int(internal_match.group(1))
            mm = int(internal_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
        return None

    slots: list[dict] = []
    seen_slot_keys: set[tuple[str, bool]] = set()

    for row in rows:
        props = row.get("properties", {})
        slot_time_raw = first_text(props.get("Time", {}))
        slot_time = normalize_slot_time(slot_time_raw)
        if not slot_time:
            logger.warning("Skipping digest selector row with invalid Time=%r", slot_time_raw)
            continue

        ww = props.get("Weekday/Weekend", {}).get("select")
        ww_name = (ww.get("name") if ww else "").strip()
        ww_norm = ww_name.lower()
        is_all = ww_norm in {"all", "every day", "daily", "always"}
        is_weekday_val = ww_norm in {"weekday", "weekdays", "mon-fri"}
        is_weekend_val = ww_norm in {"weekend", "weekends", "sat,sun", "sat/sun"}
        if not (is_all or is_weekday_val or is_weekend_val):
            logger.warning("Skipping digest selector row with invalid Weekday/Weekend=%r", ww_name)
            continue

        weekday_variants = [True, False] if is_all else ([True] if is_weekday_val else [False])
        include_habits = bool(props.get("Habits", {}).get("checkbox", False))
        max_items_raw = props.get("Max Items", {}).get("number")
        max_items = int(max_items_raw) if isinstance(max_items_raw, (int, float)) else None
        contexts = [context_label for prop_name, context_label in context_map.items() if bool(props.get(prop_name, {}).get("checkbox", False))]
        include_weather = bool(props.get("Weather", {}).get("checkbox", False))
        include_uvi = bool(props.get("UVI", {}).get("checkbox", False))
        include_feel = bool(props.get("Feel", {}).get("checkbox", False))

        for is_weekday in weekday_variants:
            slot_key = (slot_time, is_weekday)
            if slot_key in seen_slot_keys:
                logger.warning("Skipping duplicate digest selector slot %s (%s)", slot_time, "weekday" if is_weekday else "weekend")
                continue
            seen_slot_keys.add(slot_key)
            slots.append({"time": slot_time, "is_weekday": is_weekday, "include_habits": include_habits, "max_items": max_items, "contexts": contexts, "include_weather": include_weather, "include_uvi": include_uvi, "include_feel": include_feel})

    logger.info("Loaded %d digest selector slot(s) from Notion", len(slots))
    return slots


def pending_habits_for_digest(*, habit_cache: dict[str, dict], time_str: str | None, already_logged_today, is_on_pace) -> list[dict]:
    """Return pending habits, applying Show After gates only for timed digests.

    ``time_str`` is the current digest time in HH:MM format. When it is
    ``None``, callers such as /habits receive the full pending habit list
    without time-of-day filtering.
    """

    def _to_minutes(t: str) -> int:
        """Convert HH:MM to minutes since midnight."""
        try:
            hours, minutes = t.split(":")
            return int(hours) * 60 + int(minutes)
        except (AttributeError, ValueError):
            return 0

    habits = list(habit_cache.values())

    if time_str is not None:
        current_minutes = _to_minutes(time_str)
        habits = [
            habit
            for habit in habits
            if not habit.get("show_after") or current_minutes >= _to_minutes(habit["show_after"])
        ]

    pending: list[dict] = []
    for habit in sorted(habits, key=lambda h: h["sort"]):
        pid = habit["page_id"]
        if already_logged_today(pid):
            continue
        if is_on_pace(habit):
            continue
        pending.append(habit)
    return pending

# Digest scheduling helpers migrated from main.py. A few functions use transition
# imports to share runtime state until the entrypoint is further decomposed.

import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from second_brain.config import (
    MY_CHAT_ID,
    NOTION_DIGEST_SELECTOR_DB,
    NOTION_DB_ID,
    NOTION_HABIT_DB,
    NOTION_LOG_DB,
    NOTION_DAILY_LOG_DB,
    TZ,
)
from second_brain.notion.properties import query_all
from second_brain import formatters as fmt
from second_brain import keyboards as kb
from second_brain import weather as wx  # noqa: F401 - retained for transition parity


log = logging.getLogger(__name__)

# Digest runtime state — owned here, set by main.py post_init where needed.
_digest_jobs: list = []
_scheduler = None
_digest_slots_last_load_succeeded: bool = False
_digest_catchup_sent: set = set()
_digest_slot_sent_today: set = set()
_last_daily_log_url: str = ""


async def get_digest_config(notion_or_slot_time, digest_selector_db_id=None, slot_time=None, weekday=None) -> dict:
    if slot_time is None:
        notion = None
        slot_time = notion_or_slot_time
        weekday = digest_selector_db_id
        digest_selector_db_id = NOTION_DIGEST_SELECTOR_DB
    else:
        notion = notion_or_slot_time
        digest_selector_db_id = digest_selector_db_id or NOTION_DIGEST_SELECTOR_DB
    try:
        if notion is None:
            import second_brain.main as _main  # transition import
            rows = query_all(_main.notion, digest_selector_db_id)
        else:
            rows = query_all(notion, digest_selector_db_id)
        slots = load_digest_slots(rows=rows, logger=log)
    except Exception as e:
        log.error("Failed to read digest config for %s (%s): %s", slot_time, "weekday" if weekday else "weekend", e)
        return {"contexts": None, "max_items": None, "include_habits": False, "include_weather": False, "include_uvi": False, "include_feel": False}
    for slot in slots:
        if slot.get("time") == slot_time and bool(slot.get("is_weekday")) == bool(weekday):
            return {
                "contexts": slot.get("contexts"),
                "max_items": slot.get("max_items"),
                "include_habits": bool(slot.get("include_habits")),
                "include_weather": bool(slot.get("include_weather")),
                "include_uvi": bool(slot.get("include_uvi")),
                "include_feel": bool(slot.get("include_feel")),
            }
    return {"contexts": None, "max_items": None, "include_habits": False, "include_weather": False, "include_uvi": False, "include_feel": False}


def _filter_digest_tasks(tasks: list[dict], config: dict | None = None) -> list[dict]:
    if not config:
        return tasks
    filtered = tasks
    contexts = config.get("contexts")

    def normalize_context_label(value: str | None) -> str:
        v = (value or "").strip().lower()
        if "personal" in v or "🏠" in v:
            return "personal"
        if "work" in v or "💼" in v:
            return "work"
        if "health" in v or "🏃" in v:
            return "health"
        if "hk" in v or "collab" in v or "🤝" in v:
            return "hk"
        return v

    if contexts is not None and isinstance(contexts, list):
        allowed = {normalize_context_label(c) for c in contexts}
        filtered = [t for t in filtered if normalize_context_label(t.get("context")) in allowed]
    return filtered


async def send_digest_for_slot(bot, slot: dict, *, notion=None, digest_selector_db_id: str = NOTION_DIGEST_SELECTOR_DB) -> None:
    global _digest_slot_sent_today
    import second_brain.main as _main  # transition import — for alert_digest_sent

    now = datetime.now(TZ)
    day_key = now.date().isoformat()
    for key in list(_digest_slot_sent_today):
        if not key.startswith(day_key):
            _digest_slot_sent_today.discard(key)
    weekday = now.weekday() < 5
    slot_key = f"{day_key}|{'wd' if weekday else 'we'}|{slot.get('time')}"
    if slot_key in _digest_slot_sent_today:
        log.info("Skipping duplicate digest send for slot %s (%s)", slot.get("time"), "weekday" if weekday else "weekend")
        return
    if notion is None:
        config = await get_digest_config(slot["time"], slot["is_weekday"])
    else:
        config = await get_digest_config(notion, digest_selector_db_id, slot["time"], slot["is_weekday"])
    log.info(
        "Digest slot trigger fired at %s (%s) — include_habits=%s include_feel=%s contexts=%s max_items=%s",
        slot.get("time"),
        "weekday" if slot.get("is_weekday") else "weekend",
        bool(slot.get("include_habits")),
        bool(config.get("include_feel")),
        config.get("contexts"),
        config.get("max_items"),
    )
    if not config.get("contexts") and not config.get("include_habits") and not config.get("include_weather") and not config.get("include_feel"):
        log.info(
            "Skipping slot %s — nothing selected (no contexts, habits, weather, or feel)",
            slot.get("time"),
        )
        return
    await send_daily_digest(
        bot,
        include_habits=bool(config.get("include_habits")),
        config={**config, "slot_name": f"{slot.get('time')} ({'weekday' if slot.get('is_weekday') else 'weekend'})"},
    )
    _main.alert_digest_sent(f"{slot.get('time')} ({'weekday' if slot.get('is_weekday') else 'weekend'})")
    _digest_slot_sent_today.add(slot_key)


def _queue_missed_slots_for_today(scheduler, bot, slots: list[dict]) -> None:
    now = datetime.now(TZ)
    weekday = now.weekday() < 5
    grace_minutes = 20

    today_prefix = now.date().isoformat()
    for key in list(_digest_catchup_sent):
        if not key.startswith(today_prefix):
            _digest_catchup_sent.discard(key)

    for slot in slots:
        if bool(slot.get("is_weekday")) != weekday:
            continue
        try:
            slot_hour, slot_minute = map(int, str(slot["time"]).split(":"))
        except Exception:
            continue

        slot_dt = now.replace(hour=slot_hour, minute=slot_minute, second=0, microsecond=0)
        age_minutes = (now - slot_dt).total_seconds() / 60.0
        if age_minutes < 0 or age_minutes > grace_minutes:
            continue

        catchup_key = f"{today_prefix}|{'wd' if weekday else 'we'}|{slot['time']}"
        if catchup_key in _digest_catchup_sent:
            continue

        try:
            job = scheduler.add_job(
                send_digest_for_slot,
                "date",
                run_date=now + timedelta(seconds=2),
                args=[bot, slot],
                id=f"digest_catchup_{today_prefix}_{'wd' if weekday else 'we'}_{slot_hour:02d}{slot_minute:02d}",
                replace_existing=True,
                max_instances=1,
            )
            _digest_jobs.append(job)
            _digest_catchup_sent.add(catchup_key)
            log.info("Queued digest catch-up for slot %s (%s)", slot["time"], "weekday" if weekday else "weekend")
        except Exception as e:
            log.warning("Failed to queue digest catch-up for slot %s: %s", slot.get("time"), e)


def build_digest_schedule(scheduler, bot, queue_catchup: bool = False) -> int:
    global _digest_slots_last_load_succeeded
    import second_brain.main as _main  # transition import — for cleanup_old_habit_selections and notion

    _main.cleanup_old_habit_selections()
    for job in _digest_jobs:
        try:
            job.remove()
        except Exception:
            log.debug("Could not remove digest job during schedule rebuild", exc_info=True)
    _digest_jobs.clear()

    try:
        rows = query_all(_main.notion, NOTION_DIGEST_SELECTOR_DB)
        slots = load_digest_slots(rows=rows, logger=log)
    except Exception as e:
        _digest_slots_last_load_succeeded = False
        log.error("Failed to load digest slots: %s", e)
        return 0

    dedupe_keys: set[tuple[str, bool]] = set()
    for slot in slots:
        slot_key = (slot.get("time", ""), bool(slot.get("is_weekday")))
        if slot_key in dedupe_keys:
            log.warning("Skipping duplicate digest slot %s (%s)", slot.get("time"), "weekday" if slot.get("is_weekday") else "weekend")
            continue
        dedupe_keys.add(slot_key)
        try:
            hour_str, minute_str = slot["time"].split(":")
            hour, minute = int(hour_str), int(minute_str)
        except Exception:
            log.warning("Skipping invalid digest slot time: %r", slot.get("time"))
            continue
        day_of_week = "mon-fri" if slot.get("is_weekday") else "sat,sun"
        job = scheduler.add_job(
            send_digest_for_slot,
            "cron",
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            args=[bot, slot],
            max_instances=1,
        )
        _digest_jobs.append(job)

    if queue_catchup:
        _queue_missed_slots_for_today(scheduler, bot, slots)
    _digest_slots_last_load_succeeded = True
    log.info("Digest schedule built: %d slots registered", len(_digest_jobs))
    return len(_digest_jobs)


async def rebuild_digest_schedule_job(bot, scheduler) -> dict:
    was_last_success = _digest_slots_last_load_succeeded
    result = build_digest_schedule(scheduler, bot)
    if result == 0 and was_last_success:
        await bot.send_message(
            chat_id=MY_CHAT_ID,
            text="⚠️ Digest schedule rebuild returned 0 slots. Check Digest Selector.",
        )
    return {"action": "rebuilt", "slots_registered": result}


async def refresh_digest_schedule_job(bot, scheduler) -> dict:
    import second_brain.main as _main  # transition import

    slots_registered = build_digest_schedule(scheduler, bot)
    _main.notion_habits.load_habit_cache(notion=_main.notion, notion_habit_db=NOTION_HABIT_DB)
    _main._refresh_habit_cache_refs()
    return {"action": "refreshed", "slots_registered": slots_registered}


async def generate_daily_log(bot) -> dict:
    global _last_daily_log_url
    import second_brain.main as _main  # transition import

    _last_daily_log_url = await _main.notion_daily_log.generate_daily_log(
        notion=_main.notion,
        notion_daily_log_db=NOTION_DAILY_LOG_DB,
        notion_db_id=NOTION_DB_ID,
        notion_log_db=NOTION_LOG_DB,
        notion_notes_db=_main.NOTION_NOTES_DB,
        claude=_main.claude,
        claude_model=_main.CLAUDE_MODEL,
        tz=TZ,
        signoff_notes=_main.get_and_clear_project_signoff_notes(),
        claude_activity=_main.get_and_clear_claude_activity(),
    )
    return {"action": "generated", "has_url": bool(_last_daily_log_url)}


async def send_daily_digest(bot, include_habits: bool = True, config: dict | None = None) -> None:
    global _last_daily_log_url
    import second_brain.main as _main  # transition import

    if _main._is_muted():
        log.info("Daily digest skipped (muted)")
        return
    tasks = _filter_digest_tasks(_main.notion_tasks.get_today_and_overdue_tasks(_main.notion, NOTION_DB_ID, limit=None), config=config)
    today = _main.local_today()
    overdue = [t for t in tasks if (d := _main.notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
    today_tasks = [t for t in tasks if (d := _main.notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
    this_week_tasks = [t for t in tasks if t not in overdue and t not in today_tasks]
    ordered = overdue + today_tasks + this_week_tasks
    max_items = config.get("max_items") if config else None
    if isinstance(max_items, int):
        ordered = ordered[:max_items]
        overdue = [t for t in ordered if (d := _main.notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
        today_tasks = [t for t in ordered if (d := _main.notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
        this_week_tasks = [t for t in ordered if t not in overdue and t not in today_tasks]

    date_str = _main.datetime.now(TZ).strftime("%A, %B %-d")
    lines = [f"☀️ *{date_str}*", ""]

    if _last_daily_log_url:
        log_date_label = (today - timedelta(days=1)).isoformat()
        lines.append(f"📓 [{log_date_label} Log]({_last_daily_log_url})")
        lines.append("")
    include_weather = True if config is None else bool(config.get("include_weather"))
    if include_weather:
        weather_block = fmt.format_digest_weather_card()
        if weather_block:
            lines.append(weather_block)
        else:
            lines.append(fmt.weather_unavailable_digest_line())
        lines.append("")
    n = 1

    habits: list[dict] = []
    habits_enabled = include_habits
    if config and config.get("include_habits") is not None:
        habits_enabled = bool(config.get("include_habits"))
    log.info(
        "Digest habits check: habits_enabled=%s include_habits_param=%s config_include_habits=%s",
        habits_enabled, include_habits, config.get("include_habits") if config else None
    )
    if habits_enabled:
        now_str = _main.datetime.now(TZ).strftime("%H:%M")
        habits = [
            h
            for h in _main.pending_habits_for_digest(time_str=now_str)
            if (h.get("name") or "").strip().lower()
            != _main.health_config.STEPS_HABIT_NAME.strip().lower()
        ]
        log.info("Digest habits final: count=%d habit_names=%s", len(habits), [h.get("name") for h in habits[:5]])

    if overdue:
        lines.append("🚨 *Overdue*")
        for task in overdue:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if today_tasks:
        lines.append("📌 *Today*")
        for task in today_tasks:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if this_week_tasks:
        lines.append("📅 *This Week*")
        for task in this_week_tasks:
            lines.append(f"{fmt.num_emoji(n)}{fmt.context_emoji(task.get('context'))} {task['name']}")
            n += 1
        lines.append("")

    if habits:
        lines.append("*Habits:* tap to log:")
        lines.append("")

    message = "\n".join(lines).strip()
    message = _main.append_trip_reminders_to_text(message, within_days=2)

    include_feel = bool(config.get("include_feel", False)) if config else False
    digest_keyboard_rows: list[list[InlineKeyboardButton]] = []
    if habits:
        digest_keyboard_rows.extend([list(row) for row in _main.kb.habit_buttons(habits, "morning", selected=set()).inline_keyboard])
    if include_feel:
        digest_keyboard_rows.extend([list(row) for row in kb.feel_prompt_keyboard().inline_keyboard])
    reply_markup = InlineKeyboardMarkup(digest_keyboard_rows) if digest_keyboard_rows else None

    sent_digest = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )

    if habits:
        _main._store_habit_selection_session(sent_digest.message_id, habits)
    if ordered:
        _main.digest_map[sent_digest.message_id] = ordered
    _main.last_digest_msg_id = sent_digest.message_id
    log.info("Consolidated daily digest sent — %d tasks, %d habits", len(ordered), len(habits))

    if _last_daily_log_url:
        _last_daily_log_url = ""
