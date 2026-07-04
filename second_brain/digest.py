from __future__ import annotations

import re
from datetime import datetime


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
        contexts_raw = [context_label for prop_name, context_label in context_map.items() if bool(props.get(prop_name, {}).get("checkbox", False))]
        contexts = contexts_raw if contexts_raw else None
        include_weather = bool(props.get("Weather", {}).get("checkbox", False))
        include_uvi = bool(props.get("UVI", {}).get("checkbox", False))
        include_feel = bool(props.get("Feel", {}).get("checkbox", False))
        include_log = bool(props.get("Log", {}).get("checkbox", False))
        include_weight = bool(props.get("Weight", {}).get("checkbox", False))

        for is_weekday in weekday_variants:
            slot_key = (slot_time, is_weekday)
            if slot_key in seen_slot_keys:
                logger.warning("Skipping duplicate digest selector slot %s (%s)", slot_time, "weekday" if is_weekday else "weekend")
                continue
            seen_slot_keys.add(slot_key)
            slots.append({"time": slot_time, "is_weekday": is_weekday, "include_habits": include_habits, "max_items": max_items, "contexts": contexts, "include_weather": include_weather, "include_uvi": include_uvi, "include_feel": include_feel, "include_log": include_log, "include_weight": include_weight})

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

    habits = [habit for habit in habit_cache.values() if not habit.get("auto_only")]

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


def pending_habits_for_date(*, habit_cache: dict[str, dict], already_logged, is_on_pace) -> list[dict]:
    """Return active habits with no completion for a specific past date.

    Mirrors ``pending_habits_for_digest`` but without Show After time-of-day
    gating — the target day has fully elapsed, so every non-auto habit is
    eligible. ``already_logged`` should check completion for the target date;
    ``is_on_pace`` still suppresses habits whose weekly target is already met so
    the catch-up does not nag about days that were legitimately skipped.
    """

    habits = [habit for habit in habit_cache.values() if not habit.get("auto_only")]

    pending: list[dict] = []
    for habit in sorted(habits, key=lambda h: h["sort"]):
        pid = habit["page_id"]
        if already_logged(pid):
            continue
        if is_on_pace(habit):
            continue
        pending.append(habit)
    return pending

# Digest scheduling helpers — runtime state is owned here, set by post_init in main.py.

import logging
import re
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from second_brain.config import (
    MY_CHAT_ID,
    NOTION_DIGEST_SELECTOR_DB,
    NOTION_DB_ID,
    NOTION_HABIT_DB,
    NOTION_LOG_DB,
    NOTION_DAILY_LOG_DB,
    NOTION_NOTES_DB,
    NOTION_TRIPS_DB,
    CLAUDE_MODEL,
    TZ,
    NOTION_DAILY_READINESS_DB,
    NOTION_WORKOUT_DAYS_DB,
    NOTION_WORKOUT_LOG_DB,
)
from second_brain.notion.properties import query_all
from second_brain import formatters as fmt
from second_brain import keyboards as kb
from second_brain import mute as mute_helpers
from second_brain import trips as trips_mod
from second_brain.notion import habits as notion_habits
from second_brain.notion import tasks as notion_tasks
from second_brain.notion import daily_log as notion_daily_log
from second_brain.crossfit.readiness import check_readiness_logged_today
from second_brain.crossfit.notion import get_today_weight_prs, get_today_workout_link
from second_brain.error_reporting import send_system_log
from second_brain.state import STATE
from second_brain.utils import local_today
from second_brain.ai.client import VOICE_INSTRUCTION, get_claude_client
from second_brain.healthtrack import config as health_config
from utils.alert_handlers import alert_digest_sent

log = logging.getLogger(__name__)

# Digest runtime state — owned here, set by main.py post_init where needed.
_digest_jobs: list = []
_scheduler = None
_notion = None
_on_rebuild_fn = None          # cleanup_old_habit_selections from main.py
_store_habit_session_fn = None  # _store_habit_selection_session from main.py
_refresh_cache_fn = None        # _refresh_habit_cache_refs from main.py
_signoff_notes_fn = None        # get_and_clear_project_signoff_notes from main.py
_claude_activity_fn = None      # get_and_clear_claude_activity from main.py
_digest_slots_last_load_succeeded: bool = False
_digest_catchup_sent: set = set()
_digest_slot_sent_today: set = set()
_last_daily_log_url: str = ""


async def get_digest_config(slot_time: str, weekday: bool, digest_selector_db_id: str = NOTION_DIGEST_SELECTOR_DB) -> dict:
    try:
        rows = query_all(_notion, digest_selector_db_id)
        slots = load_digest_slots(rows=rows, logger=log)
    except Exception as e:
        log.error("Failed to read digest config for %s (%s): %s", slot_time, "weekday" if weekday else "weekend", e)
        return {"contexts": None, "max_items": None, "include_habits": False, "include_weather": False, "include_uvi": False, "include_feel": False, "include_log": False}
    for slot in slots:
        if slot.get("time") == slot_time and bool(slot.get("is_weekday")) == bool(weekday):
            return {
                "contexts": slot.get("contexts"),
                "max_items": slot.get("max_items"),
                "include_habits": bool(slot.get("include_habits")),
                "include_weather": bool(slot.get("include_weather")),
                "include_uvi": bool(slot.get("include_uvi")),
                "include_feel": bool(slot.get("include_feel")),
                "include_log": bool(slot.get("include_log")),
                "include_weight": bool(slot.get("include_weight")),
            }
    return {"contexts": None, "max_items": None, "include_habits": False, "include_weather": False, "include_uvi": False, "include_feel": False, "include_log": False, "include_weight": False}


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


async def send_digest_for_slot(bot, slot: dict) -> None:
    global _last_daily_log_url
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
    day_prefix = f"{day_key}|{'wd' if weekday else 'we'}"
    is_first_digest = not any(k.startswith(day_prefix) for k in _digest_slot_sent_today)
    config = await get_digest_config(slot["time"], slot["is_weekday"])
    config = {**config, "is_first_digest": is_first_digest}
    log.info(
        "Digest slot trigger fired at %s (%s) — include_habits=%s include_feel=%s contexts=%s max_items=%s",
        slot.get("time"),
        "weekday" if slot.get("is_weekday") else "weekend",
        bool(slot.get("include_habits")),
        bool(config.get("include_feel")),
        config.get("contexts"),
        config.get("max_items"),
    )
    if not config.get("contexts") and not config.get("include_habits") and not config.get("include_weather") and not config.get("include_feel") and not config.get("include_log"):
        log.info(
            "Skipping slot %s — nothing selected (no contexts, habits, weather, feel, or log)",
            slot.get("time"),
        )
        return
    if config.get("include_log") and not _last_daily_log_url:
        yesterday_label = (datetime.now(TZ) - timedelta(days=1)).strftime("%A, %B %-d, %Y")
        page_id = notion_daily_log.get_existing_daily_log(_notion, NOTION_DAILY_LOG_DB, yesterday_label)
        if page_id:
            _last_daily_log_url = f"https://www.notion.so/{page_id.replace('-', '')}"
        else:
            await generate_daily_log(bot)
    await send_daily_digest(
        bot,
        include_habits=bool(config.get("include_habits")),
        config={**config, "slot_name": f"{slot.get('time')} ({'weekday' if slot.get('is_weekday') else 'weekend'})"},
    )
    if is_first_digest and config.get("include_habits"):
        try:
            await send_yesterday_habit_catchup(bot)
        except Exception as e:
            log.warning("Yesterday habit catch-up failed for slot %s: %s", slot.get("time"), e)
    alert_digest_sent(f"{slot.get('time')} ({'weekday' if slot.get('is_weekday') else 'weekend'})")
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
    for job in _digest_jobs:
        try:
            job.remove()
        except Exception:
            log.debug("Could not remove digest job during schedule rebuild", exc_info=True)
    _digest_jobs.clear()

    try:
        rows = query_all(_notion, NOTION_DIGEST_SELECTOR_DB)
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
        await send_system_log(bot, "Digest schedule rebuild returned 0 slots. Check Digest Selector.")
    return {"action": "rebuilt", "slots_registered": result}


async def refresh_digest_schedule_job(bot, scheduler) -> dict:
    slots_registered = build_digest_schedule(scheduler, bot)
    notion_habits.load_habit_cache(notion=_notion, notion_habit_db=NOTION_HABIT_DB)
    if _refresh_cache_fn:
        _refresh_cache_fn()
    return {"action": "refreshed", "slots_registered": slots_registered}


async def generate_daily_log(bot) -> dict:
    global _last_daily_log_url
    claude_activity = _claude_activity_fn() if _claude_activity_fn else None
    _last_daily_log_url = await notion_daily_log.generate_daily_log(
        notion=_notion,
        notion_daily_log_db=NOTION_DAILY_LOG_DB,
        notion_db_id=NOTION_DB_ID,
        notion_log_db=NOTION_LOG_DB,
        notion_notes_db=NOTION_NOTES_DB,
        claude=get_claude_client(),
        claude_model=CLAUDE_MODEL,
        tz=TZ,
        signoff_notes=_signoff_notes_fn() if _signoff_notes_fn else None,
        claude_activity=claude_activity,
    )
    return {"action": "generated", "has_url": bool(_last_daily_log_url)}


def _generate_digest_brief(weather_block, overdue_count, today_count, habit_count, day_str, *, is_first_digest=True) -> str:
    try:
        from second_brain.weather import load_yesterday_weather, fetch_remaining_day_range, fetch_weather

        yesterday_line = "Yesterday: unavailable"
        if is_first_digest:
            yesterday = load_yesterday_weather()
            yesterday_line = (
                f"Yesterday: {yesterday['high_c']}°C / {yesterday['low_c']}°C ({yesterday['condition']})"
                if yesterday else "Yesterday: unavailable"
            )
            weather_guidance = (
                "Write one warm direct sentence weaving weather (and how it compares to yesterday) "
                "into the day — help the user know what to wear and what to prioritise. "
            )
        else:
            rain_match = re.search(r"Rain chance:\s*(\d+)%", weather_block or "")
            rain_pct = rain_match.group(1) if rain_match else "unavailable"
            yesterday_line = f"Upcoming rain probability: {rain_pct}%"
            weather_guidance = (
                "Write one warm direct sentence weaving weather and upcoming rain risk into the day — "
                "help the user know what to wear and what to prioritise. "
            )
        remaining = fetch_remaining_day_range()
        current = fetch_weather("current")
        if remaining and current and current.get("temp") is not None:
            now_temp = round(float(current["temp"]))
            remaining_line = f"Temperature now to midnight: {now_temp}°C → {remaining['low']}°C (low)"
        else:
            remaining_line = ""
        claude = get_claude_client()
        resp = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=80,
            messages=[{"role": "user", "content": (
                f"{VOICE_INSTRUCTION}\n\n"
                f"You are a personal secretary giving a morning brief in one sentence (max 25 words).\n\n"
                f"Today: {day_str}\n"
                f"Weather: {weather_block or 'unavailable'}\n"
                + (f"{remaining_line}\n" if remaining_line else "")
                + f"{yesterday_line}\n"
                f"Tasks: {overdue_count} overdue, {today_count} due today\n"
                f"Habits pending: {habit_count}\n\n"
                f"{weather_guidance}"
                "Always include the temperature range from now to end of day. "
                "Use Celsius for any temperatures mentioned. No greeting. No padding."
            )}],
        )
        return resp.content[0].text.strip().strip('"')
    except Exception as e:
        log.debug("Digest AI brief skipped: %s", e)
        return ""


async def send_daily_digest(bot, include_habits: bool | None = None, config: dict | None = None) -> None:
    global _last_daily_log_url
    if mute_helpers.is_muted(STATE.mute_until, TZ):
        log.info("Daily digest skipped (muted)")
        return
    if _notion is None:
        log.warning("Daily digest skipped: Notion client not initialised")
        return
    tasks = _filter_digest_tasks(notion_tasks.get_today_and_overdue_tasks(_notion, NOTION_DB_ID, limit=None), config=config)
    today = local_today()
    overdue = [t for t in tasks if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
    today_tasks = [t for t in tasks if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
    this_week_tasks = [t for t in tasks if t not in overdue and t not in today_tasks]
    ordered = overdue + today_tasks + this_week_tasks
    max_items = config.get("max_items") if config else None
    if isinstance(max_items, int):
        ordered = ordered[:max_items]
        overdue = [t for t in ordered if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d < today]
        today_tasks = [t for t in ordered if (d := notion_tasks._parse_deadline(t.get("deadline"))) is not None and d == today and t not in overdue]
        this_week_tasks = [t for t in ordered if t not in overdue and t not in today_tasks]

    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    lines = [f"☀️ *{date_str}*", ""]
    weather_block = ""

    include_log = config.get("include_log", False) if config is not None else False
    if _last_daily_log_url and include_log:
        now_local = datetime.now(TZ)
        label_date = (now_local - timedelta(days=1)).date() if now_local.hour >= 2 else (now_local - timedelta(days=2)).date()
        log_date_label = label_date.isoformat()
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
    if include_habits is None:
        habits_enabled = True if config is None else bool(config.get("include_habits", True))
    else:
        habits_enabled = bool(include_habits)
    log.info(
        "Digest habits check: habits_enabled=%s include_habits_param=%s "
        "config_include_habits=%s habit_count=%d",
        habits_enabled, include_habits,
        config.get("include_habits") if config else None,
        len(habits) if habits_enabled else -1,
    )
    if habits_enabled:
        now_str = datetime.now(TZ).strftime("%H:%M")
        habits = [
            h
            for h in pending_habits_for_digest(
                habit_cache=notion_habits.habit_cache,
                time_str=now_str,
                already_logged_today=lambda pid: notion_habits.already_logged_today(_notion, NOTION_LOG_DB, pid, TZ),
                is_on_pace=lambda habit: notion_habits.is_on_pace(_notion, NOTION_LOG_DB, habit, TZ),
            )
            if (h.get("name") or "").strip().lower()
            != health_config.STEPS_HABIT_NAME.strip().lower()
        ]
        log.info("Digest habits final: count=%d habit_names=%s", len(habits), [h.get("name") for h in habits[:5]])

    include_ai_brief = True if config is None else bool(config.get("include_ai_brief", True))
    ai_brief = ""
    if include_ai_brief:
        is_first = bool(config.get("is_first_digest", True)) if config else True
        ai_brief = _generate_digest_brief(
            weather_block=weather_block if include_weather else "",
            overdue_count=len(overdue),
            today_count=len(today_tasks),
            habit_count=len(habits),
            day_str=date_str,
            is_first_digest=is_first,
        )
    if ai_brief:
        lines[2:2] = [f"_{ai_brief}_", ""]

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

    include_weight = bool(config.get("include_weight", False)) if config else False
    if include_weight and _notion and NOTION_WORKOUT_DAYS_DB and NOTION_WORKOUT_LOG_DB:
        import asyncio as _asyncio
        _loop = _asyncio.get_running_loop()
        workout_link = await _loop.run_in_executor(
            None,
            lambda: get_today_workout_link(_notion, NOTION_WORKOUT_DAYS_DB),
        )
        weight_prs = await _loop.run_in_executor(
            None,
            lambda: get_today_weight_prs(_notion, NOTION_WORKOUT_DAYS_DB, NOTION_WORKOUT_LOG_DB),
        )
        if workout_link or weight_prs:
            if workout_link and workout_link.get("url"):
                lines.append(f"🏋️ [Today's workout: {workout_link['track']}]({workout_link['url']})")
            else:
                lines.append("🏋️ *Today's Strength PRs*")
            for entry in weight_prs:
                load = f"{entry['load_lbs']} lbs" if entry.get("load_lbs") else "BW"
                reps = entry.get("reps") or "?"
                sets = entry.get("sets")
                effort = f"{sets}×{reps}" if sets and sets > 1 else f"{reps} reps"
                date_label = f"  _({entry['date']})_" if entry.get("date") else ""
                lines.append(f"• {entry['name']} — {load} × {effort}{date_label}")
            lines.append("")

    if habits:
        lines.append("*Habits:* tap to log:")
        lines.append("")

    message = "\n".join(lines).strip()
    message = trips_mod.append_trip_reminders_to_text(message, within_days=2, notion=_notion, notion_trips_db=NOTION_TRIPS_DB)

    include_feel = bool(config.get("include_feel", False)) if config else False
    if include_feel and _notion:
        include_feel = not await check_readiness_logged_today(_notion, NOTION_DAILY_READINESS_DB)
    digest_keyboard_rows: list[list[InlineKeyboardButton]] = []
    if habits:
        digest_keyboard_rows.extend([list(row) for row in kb.habit_buttons(habits, "morning", selected=set()).inline_keyboard])
    if include_feel:
        digest_keyboard_rows.extend([list(row) for row in kb.feel_prompt_keyboard().inline_keyboard])
    reply_markup = InlineKeyboardMarkup(digest_keyboard_rows) if digest_keyboard_rows else None

    sent_digest = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=message,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )

    if habits and _store_habit_session_fn:
        _store_habit_session_fn(sent_digest.message_id, habits)
    if ordered:
        STATE.digest_map[sent_digest.message_id] = ordered
    STATE.last_digest_msg_id = sent_digest.message_id
    log.info("Consolidated daily digest sent — %d tasks, %d habits", len(ordered), len(habits))

    if _last_daily_log_url:
        _last_daily_log_url = ""


async def send_yesterday_habit_catchup(bot) -> None:
    """Ask about habits left unchecked yesterday and let the user log them late.

    Sent as a follow-up to the first morning digest. Buttons reuse the standard
    multi-select habit keyboard, but the selection session carries yesterday's
    date so tapping *Done* logs the completions against yesterday rather than
    today (e.g. you took magnesium before bed and forgot to check it off).
    """
    if mute_helpers.is_muted(STATE.mute_until, TZ):
        log.info("Yesterday habit catch-up skipped (muted)")
        return
    if _notion is None:
        log.warning("Yesterday habit catch-up skipped: Notion client not initialised")
        return

    yesterday = (datetime.now(TZ) - timedelta(days=1)).date().isoformat()
    pending = [
        h
        for h in pending_habits_for_date(
            habit_cache=notion_habits.habit_cache,
            already_logged=lambda pid: notion_habits.already_logged_today(
                _notion, NOTION_LOG_DB, pid, TZ, log_date=yesterday
            ),
            is_on_pace=lambda habit: notion_habits.is_on_pace(_notion, NOTION_LOG_DB, habit, TZ),
        )
        if (h.get("name") or "").strip().lower()
        != health_config.STEPS_HABIT_NAME.strip().lower()
    ]
    if not pending:
        log.info("Yesterday habit catch-up: nothing pending for %s", yesterday)
        return

    day_label = (datetime.now(TZ) - timedelta(days=1)).strftime("%A")
    text = (
        f"🌙 *Yesterday's habits* — did you do any of these {day_label} "
        "and forget to check? Tap to log:"
    )
    sent = await bot.send_message(
        chat_id=MY_CHAT_ID,
        text=text,
        parse_mode="Markdown",
        reply_markup=kb.habit_buttons(pending, "yesterday", selected=set()),
    )
    if _store_habit_session_fn:
        _store_habit_session_fn(sent.message_id, pending, log_date=yesterday)
    log.info("Yesterday habit catch-up sent — %d habits (date=%s)", len(pending), yesterday)


def manual_digest_config_now(slots: list[dict], now_dt: datetime, is_weekday: bool) -> dict | None:
    """Pick the most recent digest slot for the provided day type and time."""
    candidates: list[tuple[int, dict]] = []
    for slot in slots:
        if bool(slot.get("is_weekday")) != is_weekday:
            continue
        try:
            hh, mm = map(int, str(slot.get("time", "")).split(":"))
        except Exception:
            continue
        candidates.append((hh * 60 + mm, slot))

    if not candidates:
        return None

    now_minutes = now_dt.hour * 60 + now_dt.minute
    earlier_or_equal = [item for item in candidates if item[0] <= now_minutes]
    chosen = max(earlier_or_equal, key=lambda x: x[0])[1] if earlier_or_equal else min(candidates, key=lambda x: x[0])[1]
    return {
        "include_habits": bool(chosen.get("include_habits")),
        "include_weather": True,
        "include_uvi": bool(chosen.get("include_uvi")),
        "include_feel": bool(chosen.get("include_feel")),
        "include_log": bool(chosen.get("include_log")),
        "contexts": chosen.get("contexts"),
        "max_items": chosen.get("max_items"),
    }
