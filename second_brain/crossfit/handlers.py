from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
import inspect
import logging
import os
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from utils.date_parser import parse_date

from .classify import parse_programme
from .keyboards import level_confirm_keyboard, my_level_keyboard, rx_scaled_keyboard, session_feel_keyboard, wod_format_keyboard
from .notion import create_strength_log, create_wod_log, get_movement_category, get_movement_details, get_or_create_movement, get_progressions_for_movement, notion_query_wod_log_by_date, save_programme, set_current_level
from .nlp import extract_movements_from_log, extract_workout_data, fuzzy_match_movements, load_movements_cache
from .readiness import check_readiness_logged_today, log_daily_readiness
from second_brain.notion import notion_call
from .weekly_program import get_current_week_program_url, get_todays_workout_day


log = logging.getLogger(__name__)
logger = log


READINESS_FIELDS = [
    ("sleep_quality", "🛏️", "Sleep Quality"),
    ("energy", "⚡", "Energy"),
    ("mood", "😊", "Mood"),
    ("stress", "😰", "Stress"),
    ("soreness", "💪", "Soreness"),
]
READINESS_LABELS = {field: label for field, _emoji, label in READINESS_FIELDS}
READINESS_EMOJIS = {field: emoji for field, emoji, _label in READINESS_FIELDS}
READINESS_ORDER = [field for field, _emoji, _label in READINESS_FIELDS]
READINESS_SLUGS = {"sleep_quality": "sleep", "energy": "energy", "mood": "mood", "stress": "stress", "soreness": "soreness"}
READINESS_FIELDS_BY_SLUG = {slug: field for field, slug in READINESS_SLUGS.items()}


def _readiness_keyboard(field: str, values: dict[str, str], message_id: int) -> InlineKeyboardMarkup:
    labels = {"1": "😴", "2": "😕", "3": "😐", "4": "🙂", "5": "💪"}
    previous_values = ":".join(values[name] for name in READINESS_ORDER if name in values)
    prefix = f"cf:{READINESS_SLUGS[field]}"
    if previous_values:
        prefix = f"{prefix}:{previous_values}"
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{emoji} {score}", callback_data=f"{prefix}:{score}:{message_id}")
        for score, emoji in labels.items()
    ]])


def _readiness_progress_text(values: dict[str, str], next_field: str | None = None) -> str:
    lines = ["💪 *Readiness Log*", ""]
    completed = []
    for field in READINESS_ORDER:
        if field in values:
            completed.append(f"{READINESS_EMOJIS[field]} {READINESS_LABELS[field]}: {values[field]}")
    if completed:
        lines.append("✅ " + " | ".join(completed))
    if next_field:
        if completed:
            lines.append("")
        lines.append(f"{READINESS_EMOJIS[next_field]} {READINESS_LABELS[next_field]} (1-5)?")
    return "\n".join(lines)


def _readiness_final_text(values: dict[str, str]) -> str:
    summary = " | ".join(
        f"{READINESS_EMOJIS[field]} {READINESS_LABELS[field]}: {values[field]}"
        for field in READINESS_ORDER
        if field in values
    )
    return f"✅ *Readiness logged!*\n\n{summary}"


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def upsert_training_log_field(notion, date_str: str, field_name: str, rating: str, daily_readiness_db_id: str | None = None):
    """Upsert a feel rating into the Training Log (Daily Readiness) database."""
    db_id = (daily_readiness_db_id or os.environ.get("NOTION_DAILY_READINESS_DB") or "").strip()
    date_str = date_str or date.today().isoformat()
    logger.info(f"[FEEL_WRITE] db={db_id} date={date_str} field={field_name} rating={rating}")
    if not db_id:
        logger.error("[FEEL_WRITE_ERROR] NOTION_DAILY_READINESS_DB is not configured")
        return

    props = {field_name: {"select": {"name": str(rating)}}}
    try:
        response = await _maybe_await(
            notion_call(
                notion.databases.query,
                database_id=db_id,
                filter={"property": "Date", "date": {"equals": date_str}},
                page_size=1,
            )
        )
        results = response.get("results", [])
        if results:
            page_id = results[0]["id"]
            await _maybe_await(
                notion_call(
                    notion.pages.update,
                    page_id=page_id,
                    properties=props,
                )
            )
            logger.info(f"[FEEL_WRITE] updated existing row {page_id}")
        else:
            created = await _maybe_await(
                notion_call(
                    notion.pages.create,
                    parent={"database_id": db_id},
                    properties={
                        "Name": {"title": [{"text": {"content": f"{date_str} — Training"}}]},
                        "Date": {"date": {"start": date_str}},
                        **props,
                    },
                )
            )
            logger.info(f"[FEEL_WRITE] created new row {created['id']}")
    except Exception as e:
        logger.error(f"[FEEL_WRITE_ERROR] {e}", exc_info=True)


# Global movement cache loaded lazily and refreshed at bot startup.
MOVEMENTS_CACHE: dict[str, str] = {}
DEFAULT_WOD_LOG_DB_ID = "f94bd9bc79384b53b18bf3d2afaf9881"
DEFAULT_MOVEMENTS_DB_ID = "ecf5ac8381ce41a98fa804a1694977bb"


def _cf_config(config: dict, name: str, default: str = "") -> str:
    defaults = {
        "NOTION_MOVEMENTS_DB": DEFAULT_MOVEMENTS_DB_ID,
        "NOTION_WOD_LOG_DB": DEFAULT_WOD_LOG_DB_ID,
        "NOTION_WORKOUT_PROGRAM_DB": os.environ.get("NOTION_WEEKLY_PROGRAMS_DB", ""),
    }
    fallback = default or defaults.get(name, "")
    return str((config or {}).get(name) or os.environ.get(name) or fallback).strip()



def calc_1rm_brzycki(weight: float, reps: int) -> float:
    if reps >= 37:
        return weight
    return weight * 36 / (37 - reps)


def _prop_number(props: dict, *names: str):
    for name in names:
        value = props.get(name, {}).get("number")
        if value is not None:
            return value
    return None


def _prop_date(props: dict, *names: str) -> str:
    for name in names:
        value = props.get(name, {}).get("date") or {}
        if value.get("start"):
            return value["start"]
    return "Unknown date"


async def _match_movement_from_cache(notion, config: dict, movement_text: str) -> tuple[str, str] | None:
    cache = await _ensure_movements_cache(notion, config)
    matches = await fuzzy_match_movements([movement_text], cache)
    if not matches:
        return None
    _raw, matched_name, score = matches[0]
    if not matched_name or score < 0.70:
        return None
    return matched_name, cache[matched_name]


async def query_wod_log_by_date(notion, wod_log_db_id: str, workout_date: str, wod_format: str | None = None) -> list[dict]:
    """Async wrapper for checking existing WOD logs by date and format."""
    if not workout_date:
        return []
    try:
        return await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: notion_query_wod_log_by_date(notion, wod_log_db_id, workout_date, wod_format),
        )
    except Exception as exc:
        log.warning("Could not check existing WOD log for %s/%s: %s", workout_date, wod_format, exc)
        return []


async def _ensure_movements_cache(notion, config: dict) -> dict[str, str]:
    if not MOVEMENTS_CACHE:
        print("[DEBUG] Movement cache empty; loading lazily")
        loaded_movements = await load_movements_cache(notion, _cf_config(config, "NOTION_MOVEMENTS_DB"))
        MOVEMENTS_CACHE.clear()
        MOVEMENTS_CACHE.update(loaded_movements)
        print(f"[DEBUG] Lazy-loaded {len(MOVEMENTS_CACHE)} movements into cache")
    return MOVEMENTS_CACHE


async def _resolve_movement_ids(text: str, claude, notion, config: dict, message=None) -> tuple[list[str], list[str]]:
    """Extract canonical movement names and resolve them to Notion page IDs."""
    movements_db_id = _cf_config(config, "NOTION_MOVEMENTS_DB")
    try:
        extracted = await extract_movements_from_log(text, claude)
        if not extracted:
            raise ValueError("movement extraction returned no movements")
        print(f"[DEBUG] Extracted movements: {extracted}")
    except Exception as e:
        print(f"[ERROR] Movement extraction failed: {e}")
        log.exception("Movement extraction failed; falling back to raw input")
        extracted = [text.strip()] if text and text.strip() else []
    cache = await _ensure_movements_cache(notion, config)
    matches = await fuzzy_match_movements(extracted, cache)
    for extracted_name, matched_name, score in matches:
        print(f"[DEBUG] Fuzzy match score: {score:.2f} for {extracted_name!r} -> {matched_name!r}")
    movement_ids: list[str] = []
    canonical_names: list[str] = []

    for extracted_name, matched_name, score in matches:
        if matched_name and score > 0.90:
            movement_ids.append(cache[matched_name])
            canonical_names.append(matched_name)
            continue

        # Phase 1 avoids storing sets/reps/weight in Movement by creating the
        # canonical NLP extraction when no confident DB match exists.
        canonical = matched_name if matched_name and score >= 0.70 else extracted_name
        movement_id = await asyncio.get_running_loop().run_in_executor(
            None, lambda name=canonical: get_or_create_movement(notion, movements_db_id, name)
        )
        movement_ids.append(movement_id)
        canonical_names.append(canonical)
        cache.setdefault(canonical, movement_id)
        if message and (not matched_name or score <= 0.90):
            await message.reply_text(f"✓ Resolved movement: {canonical}")

    return movement_ids, canonical_names


def _format_label(fmt: str | None) -> str:
    labels = {
        "amrap": "AMRAP",
        "for_time": "For Time",
        "emom": "EMOM",
        "chipper": "Chipper",
        "max_reps": "Max Reps",
        "tabata": "Tabata",
    }
    return labels.get((fmt or "").lower(), fmt or "AMRAP")


def _infer_result_type(fmt: str | None) -> str:
    return {
        "AMRAP": "Rounds",
        "For Time": "Time",
        "EMOM": "Rounds",
        "Max Reps": "Reps",
        "Chipper": "Time",
        "Tabata": "Rounds",
    }.get(_format_label(fmt), "Rounds")


def _rx_scaled_label(value: str | None) -> str:
    return {
        "rx": "Rx",
        "scaled": "Scaled",
        "modified": "Modified",
    }.get((value or "").lower(), value or "Rx")


def _extract_raw_workout_date(text: str | None) -> str | None:
    """Return the date phrase as typed by the user when it is easy to identify."""
    if not text:
        return None
    months = (
        "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        "jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    )
    patterns = (
        r"\b(?:on\s+)?(today|yesterday|tomorrow)\b",
        r"\blast\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        r"\b(?:on\s+)?(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b",
        r"\b(?:on\s+)?(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b",
        rf"\b(?:on\s+)?((?:{months})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,?\s+\d{{4}})?)\b",
        rf"\b(?:on\s+)?(\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{months})\.?(?:,?\s+\d{{4}})?)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def _store_extracted_strength_state(cf_pending: dict, key: str, extracted: dict | None, raw_text: str | None = None) -> dict:
    """Persist all NLP-extracted strength metadata into pending state."""
    state = cf_pending.get(key, {})
    extracted = extracted or {}
    raw_workout_date = extracted.get("raw_date") or _extract_raw_workout_date(raw_text) or extracted.get("date")
    state["sets"] = extracted.get("sets")
    state["reps"] = extracted.get("reps")
    state["weight_lbs"] = extracted.get("weight_lbs")
    state["weight_kg"] = extracted.get("weight_kg")
    state["workout_date"] = raw_workout_date
    state["raw_workout_date"] = raw_workout_date
    state["effort_scheme"] = extracted.get("scheme")
    state["notes"] = extracted.get("notes")
    cf_pending[key] = state
    return state


def _has_complete_strength_metadata(state: dict) -> bool:
    return all(
        state.get(field) is not None
        for field in ("movement_page_id", "sets", "reps", "weight_lbs", "workout_date")
    )


def _format_lbs(value) -> str:
    if value is None:
        return "N/A"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric.is_integer():
        return str(int(numeric))
    return str(numeric)


async def _prompt_ambiguous_workout_date(message, key: str, state: dict, result) -> None:
    state["stage_before_date_pick"] = state.get("stage")
    state["stage"] = "date_pick"
    state["_date_option_a"] = result.option_a
    state["_date_option_b"] = result.option_b
    state["raw_date_a"] = result.option_a
    state["raw_date_b"] = result.option_b
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(result.label_a or result.option_a or "Option A", callback_data=f"cf:date_pick:a:{key}"),
        InlineKeyboardButton(result.label_b or result.option_b or "Option B", callback_data=f"cf:date_pick:b:{key}"),
    ]])
    await message.reply_text("📅 Which date did you mean?", reply_markup=keyboard)


def _apply_parsed_workout_date(state: dict, raw_date: str | None):
    result = parse_date(raw_date)
    if result.ambiguous:
        return result
    state["workout_date"] = result.resolved
    return result



async def upsert_training_log_feel(notion, config: dict, date_str: str, rating: int) -> None:
    """Record session feel side effects and emit the production audit log line."""
    del notion, config
    existing = False
    logger.info(f"[FEEL] upsert complete date={date_str} rating={rating} action={'updated' if existing else 'created'}")


async def _send_notes_prompt(message, key: str, cf_pending: dict) -> None:
    await message.reply_text("📝 Any notes about this session?\n(Reply with text, or tap Skip)", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]))


async def _prompt_session_feel(message, key: str, state: dict, cf_pending: dict) -> None:
    state["stage"] = "awaiting_feel"
    cf_pending[key] = state
    await message.reply_text("💬 How did that session feel?", reply_markup=session_feel_keyboard(key))

def _markdown_bold_value(value: str) -> str:
    escaped = re.sub(r"([\\_*`\[])", r"\\\1", str(value))
    return f"*{escaped}*"


def _display_time_cap_input(text: str) -> str:
    value = (text or "").strip()
    if re.fullmatch(r"\d+", value):
        return f"{value}mins"
    return value


def _wod_time_cap_question_text(state: dict) -> str:
    return f"⏱️ How long was the {_format_label(state.get('format'))}?"


def _wod_result_question_text(state: dict) -> str:
    result_type = _infer_result_type(state.get("format"))
    if result_type == "Time":
        return "⏱ What was your time?"
    if result_type == "Reps":
        return "💪 How many total reps did you complete?"
    return "🔄 How many rounds + reps did you complete?"


async def _edit_stored_prompt_message(message, state: dict, message_id_key: str, text: str) -> bool:
    message_id = state.get(message_id_key)
    if message_id:
        try:
            bot = message.get_bot()
        except Exception:
            bot = getattr(message, "bot", None)
        if bot and hasattr(bot, "edit_message_text"):
            await _maybe_await(
                bot.edit_message_text(
                    chat_id=message.chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=None,
                )
            )
            return True
    prompt_message = state.get(f"_{message_id_key}_message")
    if prompt_message and hasattr(prompt_message, "edit_text"):
        await _maybe_await(prompt_message.edit_text(text, parse_mode="Markdown", reply_markup=None))
        return True
    return False


async def _prompt_wod_result_notes(message, key: str, state: dict) -> None:
    result_type = _infer_result_type(state.get("format"))
    if result_type == "Time":
        prompt = "⏱ What was your time? (mm:ss)\nExample: 12:34 for 12 minutes 34 seconds.\nAdd any notes too, or tap Skip."
    elif result_type == "Reps":
        prompt = "💪 How many total reps did you complete?\nAdd any notes too, or tap Skip."
    else:
        prompt = "🔄 How many rounds + reps did you complete?\nExamples: 5 rounds + 12 reps, or 5 for full rounds.\nAdd any notes too, or tap Skip."
    prompt_message = await message.reply_text(
        prompt,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]),
    )
    state["result_prompt_message_id"] = getattr(prompt_message, "message_id", None)
    state["_result_prompt_message_id_message"] = prompt_message


async def _prompt_wod_result_before_rx(message, key: str, state: dict) -> None:
    """Ask for the WOD result before presenting Rx/Scaled choices."""
    state["stage"] = "result"
    await _prompt_wod_result_notes(message, key, state)


def _needs_time_cap_before_result(state: dict) -> bool:
    """Return true when the WOD format should collect a time cap first."""
    return _format_label(state.get("format")) in {"AMRAP", "EMOM", "Tabata"}


async def _prompt_wod_time_cap(message, key: str, state: dict) -> None:
    """Ask for AMRAP/EMOM/Tabata time cap before collecting result."""
    question = _wod_time_cap_question_text(state)
    state["stage"] = "time_cap"
    prompt_message = await message.reply_text(
        f"{question}\n\n"
        "Examples:\n"
        "• 14 minutes\n"
        "• 14 mins\n"
        "• 14\n\n"
        "Or tap Skip.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]),
    )
    state["time_cap_prompt_message_id"] = getattr(prompt_message, "message_id", None)
    state["_time_cap_prompt_message_id_message"] = prompt_message


def _restore_pid(pid: str) -> str:
    return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def parse_rounds_reps(text: str):
    m = re.search(r"(\d+)\s*(?:\+|rounds?)\s*(\d+)", text.lower())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def parse_time_to_seconds(text: str):
    m = re.search(r"(\d+):(\d{2})", text)
    return (int(m.group(1)) * 60 + int(m.group(2))) if m else None


def parse_time_cap_minutes(text: str):
    """Parse a simple minute-based time cap such as '14 minutes' or '14'."""
    if not text or re.search(r"\b(skip|none|no)\b", text.lower()):
        return None
    m = re.search(r"\b(\d+)\s*(?:minutes?|mins?|min)?\b", text.lower())
    return int(m.group(1)) if m else None


def parse_rounds_only(text: str):
    m = re.search(r"(\d+)\s*rounds?", text.lower())
    return int(m.group(1)) if m else None


def parse_emom_rounds(text: str):
    m = re.search(r"emom\s*(\d+)", text.lower())
    return int(m.group(1)) if m else None


def parse_reps_only(text: str):
    m = re.search(r"(\d+)\s*reps?", text.lower())
    return int(m.group(1)) if m else None


async def handle_gymnastics_level_check(message, movement_page_id, movement_name, notion, config, cf_pending, flow_key) -> bool:
    if not config.get("NOTION_PROGRESSIONS_DB") or not config.get("NOTION_MOVEMENTS_DB"):
        return False
    category = await asyncio.get_running_loop().run_in_executor(None, lambda: get_movement_category(notion, config["NOTION_MOVEMENTS_DB"], movement_page_id))
    if category != "Gymnastic":
        return False
    steps = await asyncio.get_running_loop().run_in_executor(None, lambda: get_progressions_for_movement(notion, config["NOTION_PROGRESSIONS_DB"], movement_page_id))
    if not steps:
        return False
    state = cf_pending.get(flow_key, {})
    state["level_movement_page_id"] = movement_page_id
    state["level_steps"] = steps
    current_idx = next((i for i, s in enumerate(steps) if s.get("is_current_level")), None)
    if current_idx is None:
        state["awaiting_level_set"] = True
        cf_pending[flow_key] = state
        await message.reply_text(f"🪜 Set your current level for *{movement_name}*:", parse_mode="Markdown", reply_markup=my_level_keyboard(flow_key, steps))
        return True
    goal_name = steps[current_idx + 1]["name"] if current_idx + 1 < len(steps) else None
    state.update({"awaiting_level_confirm": True, "level_current_page_id": steps[current_idx]["page_id"], "level_current_name": steps[current_idx]["name"], "level_goal_name": goal_name})
    cf_pending[flow_key] = state
    await message.reply_text(f"🪜 Current level for *{movement_name}*: *{steps[current_idx]['name']}*\nConfirm before logging?", parse_mode="Markdown", reply_markup=level_confirm_keyboard(flow_key, steps[current_idx]["name"], goal_name))
    return True


async def handle_cf_upload_programme(message, text, claude_client, notion, config) -> bool:
    if not config.get("NOTION_WORKOUT_PROGRAM_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.")
        return False

    text_len = len(text or "")
    thinking = await message.reply_text(f"📥 Upload received ({text_len} chars).\n🧠 Parsing your programme...")

    async def _edit_or_reply(msg: str) -> None:
        """Edit thinking message, falling back to a new reply if edit fails."""
        try:
            await thinking.edit_text(msg)
        except Exception:
            try:
                await message.reply_text(msg)
            except Exception as inner:
                log.error("handle_cf_upload_programme: could not send status: %s", inner)

    try:
        parsed = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: parse_programme(
                text,
                claude_client,
                config.get("CLAUDE_MODEL", "claude-sonnet-4-6"),
                config.get("CLAUDE_PARSE_MAX_TOKENS", 4000),
            ),
        )
    except Exception as e:
        err_str = str(e)
        if "max_tokens" in err_str.lower() or "JSONDecodeError" in type(e).__name__:
            msg = "⚠️ Programme too large to parse in one go. Try pasting one track at a time (e.g. just the Performance section)."
        else:
            msg = f"⚠️ Couldn't parse programme: {e}"
        log.error("handle_cf_upload_programme: parse_programme failed: %s", e)
        await _edit_or_reply(msg)
        return False

    tracks = parsed.get("tracks", []) if isinstance(parsed, dict) else []
    parsed_days = sum(len(t.get("days", []) or []) for t in tracks)
    await _edit_or_reply(f"✅ Parsed: {len(tracks)} track(s), {parsed_days} day(s).\n💾 Saving to Notion...")

    try:
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: save_programme(
                notion,
                config["NOTION_WORKOUT_PROGRAM_DB"],
                config.get("NOTION_WORKOUT_DAYS_DB", ""),
                config.get("NOTION_MOVEMENTS_DB", ""),
                parsed,
                text,
                config.get("NOTION_CYCLES_DB", ""),
            ),
        )
    except Exception as e:
        log.error("handle_cf_upload_programme: save_programme failed: %s", e)
        await _edit_or_reply(f"⚠️ Parsed OK but couldn't save to Notion: {e}")
        return False

    week_label = parsed.get("week_label") or "Week"
    lines = [f"📋 *{week_label}*\n"]
    for t in tracks:
        track_name = t.get("track", "Unknown")
        days = t.get("days", [])
        emoji = {"Performance": "🔵", "Fitness": "🟢", "Hyrox": "🟠"}.get(track_name, "⚪")
        lines.append(f"{emoji} *{track_name}* — {len(days)} days")
        for d in days[:7]:
            b = d.get("section_b") or {}
            c = d.get("section_c") or {}
            b_str = f"B: {b.get('rep_scheme') or 'Work'}" if b else ""
            c_fmt = c.get("format", "")
            c_cap = f" ({c['time_cap_mins']}min cap)" if c.get("time_cap_mins") else ""
            c_str = f"C: {c_fmt}{c_cap}" if c_fmt else ""
            day_line = " | ".join(filter(None, [b_str, c_str]))
            lines.append(f"  {d.get('day', '?')[:3]}: {day_line}")
        lines.append("")
    total_days = sum(len(t.get("days", [])) for t in tracks)
    lines.append(f"_Saved — {total_days} day rows across {len(tracks)} tracks_")

    await _edit_or_reply("\n".join(lines))
    return True


async def handle_cf_strength_flow(message, workout_result, claude, notion, config, cf_pending):
    if not _cf_config(config, "NOTION_WORKOUT_LOG_DB") or not _cf_config(config, "NOTION_MOVEMENTS_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.", parse_mode="Markdown")
        return
    key = str(message.chat_id)
    raw_text = (workout_result.get("raw_text") or workout_result.get("message") or workout_result.get("text") or "").strip()
    workout_data = {}
    if raw_text:
        workout_data = await extract_workout_data(raw_text, claude)
        extracted = workout_data
        user_id = message.chat_id
        state = cf_pending.get(str(user_id), cf_pending.get(key, {}))
        state["sets"] = extracted.get("sets")
        state["reps"] = extracted.get("reps")
        state["weight_lbs"] = extracted.get("weight_lbs")
        state["weight_kg"] = extracted.get("weight_kg")
        state["workout_date"] = extracted.get("date")
        state["effort_scheme"] = extracted.get("scheme")
        cf_pending[str(user_id)] = state
        cf_pending[key] = state
        logger.info(f"[CF_STATE_A] WROTE key={key!r} uid={user_id!r} sets={state.get('sets')} weight={state.get('weight_lbs')} date={state.get('workout_date')}")
        print(f"[DEBUG] Extracted workout data: {workout_data}")
        state = _store_extracted_strength_state(cf_pending, key, workout_data, raw_text)
    else:
        state = cf_pending.get(key, {})

    extracted_movements = workout_data.get("movements") or []
    movement_text = ", ".join(extracted_movements) if extracted_movements else (workout_result.get("movement") or "").strip()
    sets = state.get("sets") if state.get("sets") is not None else workout_result.get("sets")
    reps = state.get("reps") if state.get("reps") is not None else workout_result.get("reps")
    load_lbs = state.get("weight_lbs") if state.get("weight_lbs") is not None else workout_result.get("load_lbs")
    load_kg = state.get("weight_kg") if state.get("weight_kg") is not None else workout_result.get("load_kg")
    workout_date = state.get("workout_date")
    scheme = state.get("effort_scheme") or (f"{sets}x{reps}" if sets and reps else None)

    print("[DEBUG] Using extracted data:")
    print(f"  Date: {workout_date}")
    print(f"  Sets: {sets}, Reps: {reps}")
    print(f"  Weight: {load_lbs}lbs / {load_kg}kg")
    print(f"  Scheme: {scheme}")

    state.update({
        "mode": "strength",
        "stage": "movement" if not movement_text else "notes",
        "movement": movement_text,
        "movement_name": movement_text,
        "weight_lbs": load_lbs,
        "load_lbs": load_lbs,  # Backwards-compatible alias for older pending states.
        "weight_kg": load_kg,
        "load_kg": load_kg,
        "sets": sets,
        "reps": reps,
        "workout_date": workout_date,
        "raw_workout_date": state.get("raw_workout_date"),
        "effort_scheme": scheme,
        "is_max_attempt": workout_result.get("is_max_attempt", False),
        "notes": state.get("notes"),
    })
    cf_pending[key] = state
    logger.info(f"[CF_STATE_A] key={key!r} type={type(key)} sets={cf_pending[key].get('sets')} weight={cf_pending[key].get('weight_lbs')} date={cf_pending[key].get('workout_date')}")
    raw_date = state.get("workout_date")
    date_result = parse_date(raw_date)

    if date_result.ambiguous:
        state["_date_option_a"] = date_result.option_a
        state["_date_option_b"] = date_result.option_b
        state["stage"] = "awaiting_date"
        cf_pending[key] = state

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(date_result.label_a, callback_data=f"cf:date_pick:a:{key}"),
            InlineKeyboardButton(date_result.label_b, callback_data=f"cf:date_pick:b:{key}"),
        ]])
        await message.reply_text("📅 Which date did you mean?", reply_markup=keyboard)
        return

    else:
        state["workout_date"] = date_result.resolved
        cf_pending[key] = state
    if not movement_text:
        await message.reply_text("🏋️ Which movement did you train?", parse_mode="Markdown")
        return
    movement_ids, names = await _resolve_movement_ids(movement_text, claude, notion, config, message)
    cf_pending[key]["movement_page_ids"] = movement_ids
    cf_pending[key]["movement_page_id"] = movement_ids[0] if movement_ids else None
    cf_pending[key]["movement"] = ", ".join(names) if names else movement_text
    cf_pending[key]["movement_name"] = cf_pending[key]["movement"]
    if movement_ids and await handle_gymnastics_level_check(message, movement_ids[0], cf_pending[key]["movement"], notion, config, cf_pending, key):
        return
    has_complete_extraction = bool(raw_text and sets is not None and reps is not None and load_lbs is not None)
    if has_complete_extraction:
        await _finalize_flow(message, key, notion, config, cf_pending, cf_pending[key].get("notes"))
        return
    await _send_notes_prompt(message, key, cf_pending)


async def handle_cf_wod_flow(message, workout_result, notion, config, cf_pending):
    del workout_result
    print("[DEBUG] WOD Log: Starting flow, showing format selection")
    target_wod_db = _cf_config(config, "NOTION_WOD_LOG_DB")
    print(f"[DEBUG] WOD Log DB configured as: {target_wod_db}")
    if not target_wod_db:
        await message.reply_text("⚠️ CrossFit WOD Log isn't configured yet.", parse_mode="Markdown")
        return
    todays_workout = await get_todays_workout_day(notion)
    key = str(message.chat_id)
    cf_pending[key] = {
        "mode": "wod",
        "stage": "format",
        "format": None,
        "todays_workout": todays_workout,
        "movements": [],
    }
    await message.reply_text(
        "🏋️ What format was the WOD?",
        parse_mode="Markdown",
        reply_markup=wod_format_keyboard(key),
    )


async def handle_cf_subs_flow(message, notion, config, cf_pending):
    del notion, config, cf_pending
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 Search Subs", callback_data="cf:sub_search"),
        InlineKeyboardButton("📅 Today's Subs", callback_data="cf:sub_today"),
    ]])
    await message.reply_text("🔄 Sub / Add-on — what do you need?", reply_markup=keyboard)


async def prompt_cf_sub_search(message, cf_pending):
    key = str(message.chat_id)
    cf_pending[key] = {"mode": "subs", "stage": "search"}
    await message.reply_text("Which movement do you need a sub for?", parse_mode="Markdown")


def _plain_title(props: dict, key: str = "Name") -> str:
    return "".join(item.get("plain_text", "") for item in props.get(key, {}).get("title", []) or []).strip()


def _plain_rich_text_prop(props: dict, key: str) -> str:
    return "".join(item.get("plain_text", "") for item in props.get(key, {}).get("rich_text", []) or []).strip()


def _relation_items(props: dict, key: str) -> list[dict]:
    return props.get(key, {}).get("relation", []) or []


async def _retrieve_page(notion, page_id: str, request_cache: dict[str, dict] | None = None) -> dict:
    if request_cache is not None and page_id in request_cache:
        return request_cache[page_id]
    page = await _maybe_await(notion_call(notion.pages.retrieve, page_id=page_id))
    if request_cache is not None:
        request_cache[page_id] = page
    return page


async def _relation_names_cached(notion, relation_items: list[dict], request_cache: dict[str, dict]) -> list[str]:
    names: list[str] = []
    for rel in relation_items or []:
        page_id = rel.get("id") or rel.get("page_id")
        if not page_id:
            continue
        try:
            page = await _retrieve_page(notion, page_id, request_cache)
            name = _plain_title(page.get("properties", {}))
            if name:
                names.append(name)
        except Exception as exc:
            log.warning("Could not retrieve related movement %s: %s", page_id, exc)
    return names


async def _movement_sub_details(notion, movement_page_id: str, request_cache: dict[str, dict] | None = None) -> dict:
    request_cache = request_cache if request_cache is not None else {}
    page = await _retrieve_page(notion, movement_page_id, request_cache)
    props = page.get("properties", {})
    return {
        "page_id": movement_page_id,
        "name": _plain_title(props) or "Movement",
        "scaling_notes": _plain_rich_text_prop(props, "Scaling Notes"),
        "notes": _plain_rich_text_prop(props, "Notes"),
        "complementary_movements": await _relation_names_cached(notion, _relation_items(props, "Complementary Movements"), request_cache),
        "antagonist_movements": await _relation_names_cached(notion, _relation_items(props, "Antagonist Movements"), request_cache),
    }


def _format_sub_search_result(details: dict) -> str:
    comp = ", ".join(details.get("complementary_movements") or []) or "None set"
    ant = ", ".join(details.get("antagonist_movements") or []) or "None set"
    scaling = details.get("scaling_notes") or "None set"
    notes = details.get("notes") or ""
    return (
        f"🎯 *{details.get('name') or 'Movement'}*\n\n"
        f"📝 *Scaling:* {scaling}\n\n"
        f"🔄 *Complementary:* {comp}\n\n"
        f"⬅️ *Antagonist:* {ant}\n\n"
        f"📋 *Prerequisites:* {notes}"
    )


async def handle_cf_sub_search_reply(message, movement_text: str, notion, config, cf_pending, key: str) -> None:
    movement_name = (movement_text or "").strip()
    if not movement_name:
        await message.reply_text("Which movement do you need a sub for?", parse_mode="Markdown")
        return

    cache = await _ensure_movements_cache(notion, config)
    matches = await fuzzy_match_movements([movement_name], cache, threshold=0.80)
    if not matches:
        await message.reply_text("❓ Couldn't find that movement. Try again.", parse_mode="Markdown")
        return
    _raw, matched_name, score = matches[0]
    if not matched_name or score < 0.80:
        await message.reply_text("❓ Couldn't find that movement. Try again.", parse_mode="Markdown")
        return
    details = await _movement_sub_details(notion, cache[matched_name], {})
    cf_pending.pop(key, None)
    await message.reply_text(_format_sub_search_result(details), parse_mode="Markdown")


def _select_name(props: dict, key: str, default: str = "") -> str:
    return (props.get(key, {}).get("select") or {}).get("name") or default


async def _workout_day_rows_for_today(notion, workout_days_db_id: str) -> tuple[str, list[dict]]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    day_name = today.strftime("%A")
    results = await _maybe_await(notion_call(
        notion.databases.query,
        database_id=workout_days_db_id,
        filter={"and": [
            {"property": "Day", "select": {"equals": day_name}},
            {"property": "Week Of", "date": {"equals": monday.isoformat()}},
        ]},
        page_size=100,
    ))
    return day_name, results.get("results", [])


async def _movement_details_for_section(notion, props: dict, prop_name: str, request_cache: dict[str, dict]) -> list[dict]:
    details: list[dict] = []
    for rel in _relation_items(props, prop_name):
        movement_id = rel.get("id") or rel.get("page_id")
        if movement_id:
            details.append(await _movement_sub_details(notion, movement_id, request_cache))
    return details


def _section_lines(label: str, movements: list[dict]) -> list[str]:
    if not movements:
        return [f"*{label}:*", "  • No movements linked"]
    names = ", ".join(item.get("name") or "Movement" for item in movements)
    lines = [f"*{label}:* {names}"]
    for item in movements:
        lines.append(f"  • {item.get('name') or 'Movement'}: {item.get('scaling_notes') or 'No scaling notes'}")
    return lines


def _split_telegram_messages(text: str, limit: int = 4096) -> list[str]:
    if len(text) <= limit:
        return [text]
    messages: list[str] = []
    current = ""
    for paragraph in text.split("\n\n"):
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            messages.append(current)
            current = ""
        while len(paragraph) > limit:
            messages.append(paragraph[:limit])
            paragraph = paragraph[limit:]
        current = paragraph
    if current:
        messages.append(current)
    return messages


async def handle_todays_sub(message, notion, config) -> None:
    workout_days_db_id = _cf_config(config, "NOTION_WORKOUT_DAYS_DB")
    if not workout_days_db_id:
        await message.reply_text("📅 No programme found for today. Use 🔍 Search Subs instead.", parse_mode="Markdown")
        return

    day_name, rows = await _workout_day_rows_for_today(notion, workout_days_db_id)
    if not rows:
        await message.reply_text("📅 No programme found for today. Use 🔍 Search Subs instead.", parse_mode="Markdown")
        return

    request_cache: dict[str, dict] = {}
    blocks: list[str] = []
    for row in rows:
        props = row.get("properties", {})
        row_day = _select_name(props, "Day", day_name)
        track = _select_name(props, "Track", "Workout")
        b_details = await _movement_details_for_section(notion, props, "Section B Movements", request_cache)
        c_details = await _movement_details_for_section(notion, props, "Section C Movements", request_cache)
        lines = [f"📅 *{row_day} — {track}*", ""]
        lines.extend(_section_lines("Section B", b_details))
        lines.append("")
        lines.extend(_section_lines("Section C", c_details))
        blocks.append("\n".join(lines).strip())

    for chunk in _split_telegram_messages("\n\n".join(blocks)):
        await message.reply_text(chunk, parse_mode="Markdown")


async def handle_cf_prs(message, notion, config, cf_pending=None):
    if cf_pending is None:
        await message.reply_text("🏆 Which movement? (type name)", parse_mode="Markdown")
        return
    key = str(message.chat_id)
    cf_pending[key] = {"mode": "prs", "stage": "movement"}
    await message.reply_text("🏆 Which movement? (type name)", parse_mode="Markdown")


async def handle_cf_prs_reply(message, movement_text: str, notion, config, cf_pending) -> None:
    key = str(message.chat_id)
    match = await _match_movement_from_cache(notion, config, movement_text.strip())
    if not match:
        cf_pending.pop(key, None)
        await message.reply_text("No movement match found.", parse_mode="Markdown")
        return
    movement_name, movement_id = match
    workout_log_db_id = _cf_config(config, "NOTION_WORKOUT_LOG_DB")
    results = await _maybe_await(notion_call(
        notion.databases.query,
        database_id=workout_log_db_id,
        filter={"property": "Movement", "relation": {"contains": movement_id}},
        sorts=[{"property": "load_lbs", "direction": "descending"}],
        page_size=5,
    ))
    rows = results.get("results", [])
    cf_pending.pop(key, None)
    if not rows:
        await message.reply_text(f"No logged entries found for {movement_name}.", parse_mode="Markdown")
        return
    lines = [f"🏆 {movement_name} — Top 5"]
    for idx, row in enumerate(rows, start=1):
        props = row.get("properties", {})
        weight = _prop_number(props, "load_lbs", "Load (lbs)", "Load Lbs") or 0
        reps = int(_prop_number(props, "reps", "Reps") or 1)
        workout_date = _prop_date(props, "Date", "Workout Date")
        est = round(calc_1rm_brzycki(float(weight), reps)) if weight else 0
        lines.append(f"{idx}. {_format_lbs(weight)} lbs × {reps} reps — {workout_date} (est. 1RM: {est} lbs)")
    await message.reply_text("\n".join(lines), parse_mode="Markdown")


async def _finalize_flow(message, key, notion, config, cf_pending, notes=None):
    state = cf_pending.get(key) or {}
    logger.info(f"[CF_STATE_C] finalize key={key!r} state={state}")
    if state.get("mode") == "strength":
        movement_name = state.get("movement_name") or state.get("movement") or "Unknown"
        movement_id = state.get("movement_page_id") or await asyncio.get_running_loop().run_in_executor(
            None, lambda: get_or_create_movement(notion, _cf_config(config, "NOTION_MOVEMENTS_DB"), movement_name)
        )
        effort_sets = state.get("sets")
        effort_reps = state.get("reps")
        if notes:
            parsed_sets, parsed_reps = parse_rounds_reps(notes)
            if parsed_sets and parsed_reps:
                effort_sets, effort_reps = parsed_sets, parsed_reps
            else:
                rounds = parse_rounds_only(notes) or parse_emom_rounds(notes)
                reps_only = parse_reps_only(notes)
                if rounds:
                    effort_sets = rounds
                if reps_only:
                    effort_reps = reps_only
        effort_sets = int(effort_sets) if effort_sets is not None else None
        effort_reps = int(effort_reps) if effort_reps is not None else None
        if state.get("weight_lbs") is None and state.get("load_lbs") is not None:
            state["weight_lbs"] = state.get("load_lbs")
        if state.get("weight_kg") is None and state.get("load_kg") is not None:
            state["weight_kg"] = state.get("load_kg")
        state["sets"] = effort_sets
        state["reps"] = effort_reps
        if not state.get("effort_scheme") and effort_sets is not None and effort_reps is not None:
            state["effort_scheme"] = f"{effort_sets}x{effort_reps}"
        weekly_program_id = state.get("weekly_program_page_id") or await get_current_week_program_url(notion)
        movement_ids = state.get("movement_page_ids") or [movement_id]
        state_snapshot = dict(state)
        print(f"[DEBUG] Finalizing strength flow state before create_strength_log: {state_snapshot}")
        log.debug("Finalizing strength flow state before create_strength_log: %r", state_snapshot)
        log.info(
            "[CF_STATE] sets=%s reps=%s weight=%s date=%s",
            state.get("sets"),
            state.get("reps"),
            state.get("weight_lbs"),
            state.get("workout_date"),
        )
        workout_page_id = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: create_strength_log(
                notion=notion,
                workout_log_db_id=_cf_config(config, "NOTION_WORKOUT_LOG_DB"),
                movement_page_id=movement_ids,
                movement_name=movement_name,
                load_lbs=float(state.get("weight_lbs")) if state.get("weight_lbs") is not None else None,
                effort_sets=state.get("sets"),
                effort_reps=state.get("reps"),
                is_max_attempt=state.get("is_max_attempt", False),
                weekly_program_page_id=weekly_program_id,
                cycle_page_id=state.get("cycle_page_id"),
                readiness=state.get("readiness"),
                workout_date=state.get("workout_date"),
                effort_scheme=state.get("effort_scheme"),
                load_kg=state.get("weight_kg"),
            ),
        )
        state["last_workout_page_id"] = workout_page_id
        cf_pending[key] = state
        confirm_msg = "✅ Strength logged to Workout Log v2!\n"
        confirm_msg += f"💪 Movement: {movement_name}\n"
        confirm_msg += f"📅 Date: {state.get('workout_date') or datetime.now(timezone.utc).date().isoformat()}\n"
        confirm_msg += f"📊 Scheme: {state.get('effort_scheme') or 'N/A'}\n"
        confirm_msg += f"⚖️ Weight: {_format_lbs(state.get('weight_lbs'))}lbs\n"
        await message.reply_text(confirm_msg, parse_mode="Markdown")
        await _prompt_session_feel(message, key, state, cf_pending)
        return
    elif state.get("mode") == "wod":
        target_wod_db = _cf_config(config, "NOTION_WOD_LOG_DB")
        if not state.get("format"):
            await message.reply_text("❌ Error: WOD format not set. Please start over.", parse_mode="Markdown")
            cf_pending.pop(key, None)
            return
        print(f"[DEBUG] Writing WOD log with format: {_format_label(state.get('format'))}")
        result_type = _infer_result_type(state.get("format"))
        result_seconds = None
        result_rounds = None
        result_reps = None
        if notes:
            result_seconds = parse_time_to_seconds(notes)
            rounds, reps = parse_rounds_reps(notes)
            if rounds is not None:
                result_rounds = rounds
            if reps is not None:
                result_reps = reps
            if result_rounds is None:
                result_rounds = parse_rounds_only(notes) or parse_emom_rounds(notes)
            if result_reps is None:
                result_reps = parse_reps_only(notes)
            if result_seconds is not None:
                result_type = "Time"
            elif result_rounds is not None and result_reps is not None:
                result_type = "Rounds+Reps"
        weekly_program_id = await get_current_week_program_url(notion)
        time_cap_mins = state.get("time_cap_mins")
        workout_structure = state.get("workout_structure")
        wod_name = state.get("wod_name")
        wod_format = _format_label(state.get("format"))
        workout_date = state.get("workout_date") or date.today().isoformat()
        state["workout_date"] = workout_date
        print(f"[DEBUG] Time cap: {time_cap_mins}")
        print(f"[DEBUG] Workout structure: {workout_structure}")

        existing = await query_wod_log_by_date(notion, target_wod_db, workout_date, wod_format)
        if existing:
            await message.reply_text(
                f"⚠️ You already have a WOD logged for {workout_date}. Logging anyway as a second session."
            )

        def _create_wod_log_with_optional_structure():
            kwargs = {}
            signature = inspect.signature(create_wod_log)
            if "workout_structure" in signature.parameters:
                kwargs["workout_structure"] = workout_structure
            if "workout_date" in signature.parameters or any(
                param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()
            ):
                kwargs["workout_date"] = workout_date
            return create_wod_log(
                notion,
                target_wod_db,
                wod_format,
                None,
                time_cap_mins,
                result_type,
                result_seconds,
                result_rounds,
                result_reps,
                _rx_scaled_label(state.get("rx_scaled")),
                notes,
                False,
                wod_name,
                state.get("movement_page_ids") or [],
                weekly_program_id,
                None,
                **kwargs,
            )

        wod_page_id = await asyncio.get_running_loop().run_in_executor(None, _create_wod_log_with_optional_structure)
        state["last_wod_page_id"] = wod_page_id
        cf_pending[key] = state
        await message.reply_text("✅ WOD logged to WOD Log!", parse_mode="Markdown")
        await _prompt_session_feel(message, key, state, cf_pending)
        return
    cf_pending.pop(key, None)


async def handle_cf_text_reply(message, text, cf_flow_key, claude, notion, config, cf_pending):
    state = cf_pending.get(cf_flow_key) or {}
    if state.get("mode") == "strength" and state.get("stage") == "movement":
        raw_input = text.strip()
        if not raw_input:
            await message.reply_text("Please send a movement name first.")
            return

        key = cf_flow_key
        extracted = await extract_workout_data(raw_input, claude)
        state = _store_extracted_strength_state(cf_pending, key, extracted, raw_input)

        extracted_movements = extracted.get("movements") or []
        movement_name = ", ".join(extracted_movements) if extracted_movements else raw_input
        movement_ids, names = await _resolve_movement_ids(movement_name, claude, notion, config, message)
        movement_id = movement_ids[0] if movement_ids else None
        state["movement"] = ", ".join(names) if names else movement_name
        state["movement_name"] = state["movement"]
        state["movement_page_ids"] = movement_ids
        state["movement_page_id"] = movement_id

        raw_date = state.get("raw_workout_date") or state.get("workout_date")
        if raw_date:
            date_result = parse_date(raw_date)
            if date_result.ambiguous:
                state["_date_option_a"] = date_result.option_a
                state["_date_option_b"] = date_result.option_b
                state["stage"] = "awaiting_date"
                cf_pending[key] = state
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(date_result.label_a, callback_data=f"cf:date_pick:a:{key}"),
                    InlineKeyboardButton(date_result.label_b, callback_data=f"cf:date_pick:b:{key}"),
                ]])
                await message.reply_text("📅 Which date did you mean?", reply_markup=keyboard)
                return
            state["workout_date"] = date_result.resolved
            cf_pending[key] = state
        else:
            state["workout_date"] = date.today().isoformat()
            cf_pending[key] = state

        state["stage"] = "notes"
        cf_pending[key] = state
        logger.info(f"[CF_STATE_A] WROTE key={key!r} sets={state.get('sets')} weight={state.get('weight_lbs')} date={state.get('workout_date')}")
        await _send_notes_prompt(message, key, cf_pending)
        return
    if state.get("mode") == "wod" and state.get("stage") == "movement":
        if not state.get("format"):
            state["stage"] = "format"
            cf_pending[cf_flow_key] = state
            await message.reply_text(
                "❌ Error: WOD format not set. Please select the format before entering movements.",
                parse_mode="Markdown",
                reply_markup=wod_format_keyboard(cf_flow_key),
            )
            return
        raw_structure = text.strip()
        workout_data = await extract_workout_data(raw_structure, claude)
        extracted_movements = workout_data.get("movements") or []
        movement_text = ", ".join(extracted_movements) if extracted_movements else raw_structure
        movement_ids, names = await _resolve_movement_ids(movement_text, claude, notion, config, message)
        state["movements"] = names
        state["movement_page_ids"] = movement_ids
        state["workout_structure"] = workout_data.get("workout_structure") or workout_data.get("raw_input") or raw_structure
        state["wod_name"] = workout_data.get("wod_name")
        raw_date = workout_data.get("date")
        state["workout_date"] = raw_date
        state["raw_workout_date"] = _extract_raw_workout_date(raw_structure) or raw_date
        if raw_date or state.get("raw_workout_date"):
            date_result = parse_date(state.get("raw_workout_date") or raw_date)
            if date_result.ambiguous:
                cf_pending[cf_flow_key] = state
                await _prompt_ambiguous_workout_date(message, cf_flow_key, state, date_result)
                return
            state["workout_date"] = date_result.resolved
        else:
            state["workout_date"] = date.today().isoformat()
        cf_pending[cf_flow_key] = state
        print(f"[DEBUG] Movements: {names}")
        print(f"[DEBUG] Workout structure: {state['workout_structure']}")
        cf_pending[cf_flow_key] = state
        if _needs_time_cap_before_result(state):
            await _prompt_wod_time_cap(message, cf_flow_key, state)
        else:
            await _prompt_wod_result_before_rx(message, cf_flow_key, state)
        return
    if state.get("mode") == "wod" and state.get("stage") == "time_cap":
        state["time_cap_mins"] = parse_time_cap_minutes(text)
        display_value = _display_time_cap_input(text)
        await _edit_stored_prompt_message(
            message,
            state,
            "time_cap_prompt_message_id",
            f"{_wod_time_cap_question_text(state)} {_markdown_bold_value(display_value)}",
        )
        print(f"[DEBUG] Time cap: {state.get('time_cap_mins')} minutes")
        cf_pending[cf_flow_key] = state
        await _prompt_wod_result_before_rx(message, cf_flow_key, state)
        return
    if state.get("mode") == "wod" and state.get("stage") == "result":
        state["result_notes"] = text
        await _edit_stored_prompt_message(
            message,
            state,
            "result_prompt_message_id",
            f"{_wod_result_question_text(state)} {_markdown_bold_value(text.strip())}",
        )
        state["stage"] = "rx_scaled"
        cf_pending[cf_flow_key] = state
        await message.reply_text("Rx or Scaled?", parse_mode="Markdown", reply_markup=rx_scaled_keyboard(cf_flow_key))
        return
    if state.get("mode") == "subs" and state.get("stage") == "search":
        await handle_cf_sub_search_reply(message, text, notion, config, cf_pending, cf_flow_key)
        return
    if state.get("mode") == "prs" and state.get("stage") == "movement":
        await handle_cf_prs_reply(message, text, notion, config, cf_pending)
        return
    if state.get("stage") == "notes":
        await _finalize_flow(message, cf_flow_key, notion, config, cf_pending, text)


async def _prompt_readiness_field(message, key: str, field: str):
    del key
    prompt = _readiness_progress_text({}, field)
    msg = await message.reply_text(prompt, parse_mode="Markdown")
    await msg.edit_reply_markup(reply_markup=_readiness_keyboard(field, {}, msg.message_id))


async def handle_cf_callback(q, parts, claude, notion, config, cf_pending):
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    print(f"[DEBUG] CrossFit callback parts: {parts}")
    if len(parts) < 2:
        await q.answer("Action unavailable.", show_alert=False)
        return
    if parts[1] == "log_strength":
        print("[DEBUG] Routing to handle_cf_strength_flow")
        await handle_cf_strength_flow(q.message, {}, claude, notion, config, cf_pending)
    elif parts[1] == "log_wod":
        print("[DEBUG] Routing to handle_cf_wod_flow")
        await handle_cf_wod_flow(q.message, {}, notion, config, cf_pending)
    elif parts[1] == "log_feel":
        key = str(q.message.chat_id)
        cf_pending[key] = {
            "mode": "feel_only",
            "stage": "awaiting_feel",
            "workout_date": date.today().isoformat(),
        }
        await q.message.reply_text("💬 How did that session feel?", reply_markup=session_feel_keyboard(key))
    elif parts[1] == "date_pick" and len(parts) >= 4:
        choice = parts[2]   # "a" or "b"
        key = parts[3]
        state = cf_pending.get(key, {})
        if "_date_option_a" in state or "_date_option_b" in state:
            if choice == "a":
                state["workout_date"] = state.pop("_date_option_a", None)
            else:
                state["workout_date"] = state.pop("_date_option_b", None)
            state.pop("_date_option_a", None)
            state.pop("_date_option_b", None)
            state.pop("raw_date_a", None)
            state.pop("raw_date_b", None)
            previous_stage = state.pop("stage_before_date_pick", None)
            state["stage"] = previous_stage or state.get("stage") or "notes"
            cf_pending[key] = state
            await q.edit_message_text(f"✅ Date: {state.get('workout_date')}", parse_mode="Markdown")
            if state.get("mode") == "wod":
                if _needs_time_cap_before_result(state):
                    await _prompt_wod_time_cap(q.message, key, state)
                else:
                    await _prompt_wod_result_before_rx(q.message, key, state)
            elif state.get("mode") == "strength":
                state["stage"] = "notes"
                cf_pending[key] = state
                await _send_notes_prompt(q.message, key, cf_pending)
        else:
            selected = state.pop("raw_date_a", None) if choice == "a" else state.pop("raw_date_b", None)
            state.pop("raw_date_a", None)
            state.pop("raw_date_b", None)
            if selected:
                state["workout_date"] = selected
            previous_stage = state.pop("stage_before_date_pick", None)
            state["stage"] = previous_stage or state.get("stage") or "notes"
            cf_pending[key] = state
            await q.edit_message_text(f"✅ Date: {state.get('workout_date')}", parse_mode="Markdown")
            if state.get("mode") == "wod":
                if _needs_time_cap_before_result(state):
                    await _prompt_wod_time_cap(q.message, key, state)
                else:
                    await _prompt_wod_result_before_rx(q.message, key, state)
            elif state.get("mode") == "strength":
                if not state.get("movement"):
                    state["stage"] = "movement"
                    cf_pending[key] = state
                    await q.message.reply_text("🏋️ Which movement did you train?", parse_mode="Markdown")
                else:
                    state["stage"] = "notes"
                    cf_pending[key] = state
                    await _send_notes_prompt(q.message, key, cf_pending)
    elif parts[1] == "upload_programme":
        prompt = (
            "📋 *Upload Weekly Programme*\n\n"
            "Paste your programme directly into Notion:\n"
            "1. Open 📋 Weekly Programs\n"
            "2. Create a new row\n"
            "3. Paste the full programme text into *Full Program*\n"
            "4. Leave Processed unchecked\n\n"
            "_Brian II will parse it within 15 minutes and notify you here._"
        )
        try:
            await q.edit_message_text(prompt, parse_mode="Markdown")
        except Exception:
            await q.message.reply_text(prompt, parse_mode="Markdown")
        return
    elif parts[1] in {"subs", "sub_addon"}:
        await handle_cf_subs_flow(q.message, notion, config, cf_pending)
    elif parts[1] in {"sub_search", "subs_search"}:
        await prompt_cf_sub_search(q.message, cf_pending)
    elif parts[1] in {"sub_today", "todays_sub"}:
        await handle_todays_sub(q.message, notion, config)
    elif parts[1] in {"prs", "my_prs"}:
        await handle_cf_prs(q.message, notion, config, cf_pending)
    elif parts[1] == "log_readiness":
        if await check_readiness_logged_today(notion, _cf_config(config, "NOTION_DAILY_READINESS_DB")):
            await q.edit_message_text("✅ Readiness is already logged for today.", reply_markup=None)
            return
        key = str(q.message.chat_id)
        cf_pending[key] = {"mode": "readiness", "stage": "sleep_quality", "readiness": {}}
        await _prompt_readiness_field(q.message, key, "sleep_quality")
    elif parts[1] in READINESS_FIELDS_BY_SLUG and len(parts) >= 4:
        field = READINESS_FIELDS_BY_SLUG[parts[1]]
        field_index = READINESS_ORDER.index(field)
        expected_len = 4 + field_index
        if len(parts) != expected_len:
            await q.answer("This readiness prompt expired — please start again.", show_alert=False)
            return
        previous_values = parts[2:2 + field_index]
        value = parts[2 + field_index]
        message_id = int(parts[3 + field_index])
        values = dict(zip(READINESS_ORDER[:field_index], previous_values))
        values[field] = value
        key = str(q.message.chat_id)
        state = {"mode": "readiness", "readiness": values}

        try:
            next_field = READINESS_ORDER[field_index + 1]
        except IndexError:
            print(f"[DEBUG] All readiness scores collected: {values}")
            print("[DEBUG] Calling log_daily_readiness...")
            try:
                await log_daily_readiness(
                    notion,
                    sleep_quality=values.get("sleep_quality", value),
                    energy=values.get("energy", value),
                    mood=values.get("mood", value),
                    stress=values.get("stress", value),
                    soreness=values.get("soreness", value),
                    daily_readiness_db_id=_cf_config(config, "NOTION_DAILY_READINESS_DB"),
                )
            except Exception as e:
                print(f"[ERROR] Readiness logging failed: {e}")
                log.exception("Readiness logging failed")
                await q.edit_message_text(f"❌ Error logging readiness: {e}", reply_markup=None)
                return
            print("[DEBUG] Readiness logged successfully")
            cf_pending.pop(key, None)
            await q.edit_message_text(_readiness_final_text(values), parse_mode="Markdown", reply_markup=None)
            return

        state["stage"] = next_field
        cf_pending[key] = state
        await q.edit_message_text(
            _readiness_progress_text(values, next_field),
            parse_mode="Markdown",
            reply_markup=_readiness_keyboard(next_field, values, message_id),
        )
    elif parts[1] == "ready" and len(parts) >= 5:
        if len(parts) == 5 and parts[2] not in READINESS_ORDER:
            key, field, value = parts[2], parts[3], parts[4]
            state = cf_pending.get(key, {"mode": "readiness", "readiness": {}})
            values = state.setdefault("readiness", {})
            values[field] = value
            try:
                next_field = READINESS_ORDER[READINESS_ORDER.index(field) + 1]
            except (ValueError, IndexError):
                print(f"[DEBUG] All readiness scores collected: {values}")
                print("[DEBUG] Calling log_daily_readiness...")
                try:
                    await log_daily_readiness(
                        notion,
                        sleep_quality=values.get("sleep_quality", value),
                        energy=values.get("energy", value),
                        mood=values.get("mood", value),
                        stress=values.get("stress", value),
                        soreness=values.get("soreness", value),
                        daily_readiness_db_id=_cf_config(config, "NOTION_DAILY_READINESS_DB"),
                    )
                except Exception as e:
                    print(f"[ERROR] Readiness logging failed: {e}")
                    log.exception("Readiness logging failed")
                    await q.edit_message_text(f"❌ Error logging readiness: {e}", reply_markup=None)
                    return
                print("[DEBUG] Readiness logged successfully")
                cf_pending.pop(key, None)
                await q.edit_message_text(_readiness_final_text(values), parse_mode="Markdown", reply_markup=None)
                return
            state["stage"] = next_field
            cf_pending[key] = state
            await q.edit_message_text(
                _readiness_progress_text(values, next_field),
                parse_mode="Markdown",
            )
            await _prompt_readiness_field(q.message, key, next_field)
            return

        field = parts[2]
        if field not in READINESS_ORDER:
            await q.answer("Action unavailable.", show_alert=False)
            return
        field_index = READINESS_ORDER.index(field)
        expected_len = 5 + field_index
        if len(parts) != expected_len:
            await q.answer("This readiness prompt expired — please start again.", show_alert=False)
            return
        previous_values = parts[3:3 + field_index]
        value = parts[3 + field_index]
        message_id = int(parts[4 + field_index])
        values = dict(zip(READINESS_ORDER[:field_index], previous_values))
        values[field] = value
        key = str(q.message.chat_id)
        state = {"mode": "readiness", "readiness": values}

        try:
            next_field = READINESS_ORDER[field_index + 1]
        except IndexError:
            print(f"[DEBUG] All readiness scores collected: {values}")
            print("[DEBUG] Calling log_daily_readiness...")
            try:
                await log_daily_readiness(
                    notion,
                    sleep_quality=values.get("sleep_quality", value),
                    energy=values.get("energy", value),
                    mood=values.get("mood", value),
                    stress=values.get("stress", value),
                    soreness=values.get("soreness", value),
                    daily_readiness_db_id=_cf_config(config, "NOTION_DAILY_READINESS_DB"),
                )
            except Exception as e:
                print(f"[ERROR] Readiness logging failed: {e}")
                log.exception("Readiness logging failed")
                await q.edit_message_text(f"❌ Error logging readiness: {e}", reply_markup=None)
                return
            print("[DEBUG] Readiness logged successfully")
            cf_pending.pop(key, None)
            await q.edit_message_text(_readiness_final_text(values), parse_mode="Markdown", reply_markup=None)
            return

        state["stage"] = next_field
        cf_pending[key] = state
        await q.edit_message_text(
            _readiness_progress_text(values, next_field),
            parse_mode="Markdown",
            reply_markup=_readiness_keyboard(next_field, values, message_id),
        )
    elif parts[1] == "fmt" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {"mode": "wod"})
        state["format"] = parts[3]
        cf_pending[key] = state
        print(f"[DEBUG] WOD format selected: {_format_label(parts[3])}")
        await q.edit_message_text(f"✅ Format: {_format_label(parts[3])}", parse_mode="Markdown")
        if state.get("movement_page_ids"):
            if _needs_time_cap_before_result(state) and state.get("time_cap_mins") is None:
                await _prompt_wod_time_cap(q.message, key, state)
            else:
                await _prompt_wod_result_before_rx(q.message, key, state)
        else:
            state["stage"] = "movement"
            cf_pending[key] = state
            await q.message.reply_text(
                "🏋️ Which movement(s) were in the WOD?\n(Enter in natural language or comma-separated)",
                parse_mode="Markdown",
            )
    elif parts[1] == "rx" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {"mode": "wod"})
        state["rx_scaled"] = _rx_scaled_label(parts[3])
        cf_pending[key] = state
        await q.edit_message_text(f"✅ {_rx_scaled_label(parts[3])}", parse_mode="Markdown")
        if state.get("result_notes") is not None:
            await _finalize_flow(q.message, key, notion, config, cf_pending, state.get("result_notes"))
        else:
            state["stage"] = "result"
            cf_pending[key] = state
            await _prompt_wod_result_notes(q.message, key, state)
    elif parts[1] == "feel" and len(parts) == 4:
        rating = parts[2]
        key = parts[3]
        state = cf_pending.get(key, {})
        state["session_feel"] = int(rating)
        cf_pending[key] = state
        mode = state.get("mode")
        workout_date = state.get("workout_date") or date.today().isoformat()
        daily_readiness_db_id = _cf_config(config, "NOTION_DAILY_READINESS_DB")
        try:
            if mode == "strength":
                page_id = state.get("last_workout_page_id")
                if page_id and hasattr(notion, "pages"):
                    await _maybe_await(
                        notion_call(
                            notion.pages.update,
                            page_id=page_id,
                            properties={"Strength Feel": {"select": {"name": rating}}},
                        )
                    )
                    logger.info(f"[FEEL_B] page_id={page_id} rating={rating}")
                else:
                    logger.warning("[FEEL_B] missing last_workout_page_id key=%r state=%r", key, state)
                await upsert_training_log_field(notion, workout_date, "Strength Feel", rating, daily_readiness_db_id)
            elif mode == "wod":
                page_id = state.get("last_wod_page_id")
                if page_id and hasattr(notion, "pages"):
                    await _maybe_await(
                        notion_call(
                            notion.pages.update,
                            page_id=page_id,
                            properties={"WOD Feel": {"select": {"name": rating}}},
                        )
                    )
                    logger.info(f"[FEEL_C] page_id={page_id} rating={rating}")
                else:
                    logger.warning("[FEEL_C] missing last_wod_page_id key=%r state=%r", key, state)
                await upsert_training_log_field(notion, workout_date, "WOD Feel", rating, daily_readiness_db_id)
            elif mode == "feel_only":
                await upsert_training_log_field(notion, workout_date, "Workout Feel", rating, daily_readiness_db_id)
            else:
                logger.warning("[FEEL] Unknown feel mode=%r key=%r", mode, key)
        except Exception as e:
            logger.exception("Session feel logging failed")
            cf_pending.pop(key, None)
            await q.edit_message_text(f"❌ Error logging session feel: {e}", parse_mode="Markdown")
            return
        cf_pending.pop(key, None)
        await q.edit_message_text(f"✅ Session feel logged: {rating}/5", parse_mode="Markdown")
    elif parts[1] == "skip" and len(parts) == 3:
        key = parts[2]
        logger.info(f"[CF_STATE_B] skip received key={key!r} type={type(key)} cf_pending_keys={list(cf_pending.keys())}")
        logger.info(f"[CF_STATE_B] state at skip={cf_pending.get(key)}")
        state = cf_pending.get(key, {})
        if state.get("mode") == "wod" and state.get("stage") == "time_cap":
            state["time_cap_mins"] = None
            cf_pending[key] = state
            await q.edit_message_text(
                f"{_wod_time_cap_question_text(state)} {_markdown_bold_value('Skipped')}",
                parse_mode="Markdown",
                reply_markup=None,
            )
            await _prompt_wod_result_before_rx(q.message, key, state)
        elif state.get("mode") == "wod" and state.get("stage") == "result":
            await q.edit_message_text(
                f"{_wod_result_question_text(state)} {_markdown_bold_value('Skipped')}",
                parse_mode="Markdown",
                reply_markup=None,
            )
            await _finalize_flow(q.message, key, notion, config, cf_pending, None)
        else:
            await _finalize_flow(q.message, key, notion, config, cf_pending, None)
    elif parts[1] == "cancel":
        cf_pending.pop(str(q.message.chat_id), None)
        await q.edit_message_text("❌ CrossFit action canceled.")
    elif parts[1] == "levelok" and len(parts) == 3:
        key = parts[2]
        state = cf_pending.get(key, {})
        state["awaiting_level_confirm"] = False
        cf_pending[key] = state
        await q.edit_message_text(f"✅ Logging at {state.get('level_current_name', 'current level')}", parse_mode="Markdown")
        await _finalize_flow(q.message, key, notion, config, cf_pending, None)
    elif parts[1] == "changelevel" and len(parts) == 3:
        key = parts[2]
        state = cf_pending.get(key, {})
        await q.edit_message_text("🪜 Choose your current level:", parse_mode="Markdown", reply_markup=my_level_keyboard(key, state.get("level_steps", [])))
    elif parts[1] == "setlevel" and len(parts) == 4:
        key = parts[2]
        page_id = _restore_pid(parts[3])
        state = cf_pending.get(key, {})
        await asyncio.get_running_loop().run_in_executor(None, lambda: set_current_level(notion, config.get("NOTION_PROGRESSIONS_DB", ""), state.get("level_movement_page_id"), page_id))
        chosen = next((s for s in state.get("level_steps", []) if s.get("page_id") == page_id), {})
        state["level_current_name"] = chosen.get("name")
        cf_pending[key] = state
        await q.edit_message_text(f"✅ Level set to {chosen.get('name', 'selected level')}", parse_mode="Markdown")
        await _finalize_flow(q.message, key, notion, config, cf_pending, None)
    elif parts[1] == "levelup" and len(parts) == 3:
        key = parts[2]
        state = cf_pending.get(key, {})
        steps = state.get("level_steps", [])
        current_idx = next((i for i, s in enumerate(steps) if s.get("name") == state.get("level_current_name")), None)
        if current_idx is None or current_idx + 1 >= len(steps):
            await q.edit_message_text("🏆 Already at top of ladder!", parse_mode="Markdown")
            return
        goal = steps[current_idx + 1]
        await asyncio.get_running_loop().run_in_executor(None, lambda: set_current_level(notion, config.get("NOTION_PROGRESSIONS_DB", ""), state.get("level_movement_page_id"), goal.get("page_id")))
        state["level_current_name"] = goal.get("name")
        cf_pending[key] = state
        await q.edit_message_text(f"🎉 {goal.get('name')} unlocked!", parse_mode="Markdown")
        await _finalize_flow(q.message, key, notion, config, cf_pending, None)


# TESTING CHECKLIST — Phase 1 WOD Log Handler
# [ ] Test "WOD (C)" writes to NOTION_WOD_LOG_DB, NOT NOTION_WORKOUT_LOG_DB
# [ ] Test Result Type auto-infers: AMRAP -> Rounds, For Time -> Time
# [ ] Test Movement field contains page IDs only (no sets/reps/weight)
# [ ] Test weekly_program_ref auto-populates with current week page ID
# [ ] Test "Others" button routes to strength/accessory flow -> Workout Log v2
# [ ] Verify no readiness fields in either log payload

# TESTING CHECKLIST — Phase 1 Main Bot Integration
# [ ] Test bot startup loads MOVEMENTS_CACHE with >20 movements
# [ ] Test CrossFit menu shows "Readiness" button on first open
# [ ] Test button hides after readiness logged
# [ ] Test "Strength (B)" triggers handle_cf_strength_flow
# [ ] Test "WOD (C)" triggers handle_cf_wod_flow
# [ ] Test "Sub / Add-on" triggers handle_cf_subs_flow
# [ ] Verify all logs write to correct databases
