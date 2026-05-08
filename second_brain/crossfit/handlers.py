from __future__ import annotations

import asyncio
import logging
import os
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .classify import parse_programme
from .keyboards import level_confirm_keyboard, my_level_keyboard, rx_scaled_keyboard, sub_type_keyboard, wod_format_keyboard
from .notion import create_strength_log, create_wod_log, get_movement_category, get_or_create_movement, get_progressions_for_movement, query_subs, save_programme, set_current_level
from .nlp import extract_movements_from_log, extract_workout_data, fuzzy_match_movements, load_movements_cache
from .readiness import check_readiness_logged_today, log_daily_readiness
from .weekly_program import get_current_week_program_url, get_todays_workout_day


log = logging.getLogger(__name__)


# Global movement cache loaded lazily and refreshed at bot startup.
MOVEMENTS_CACHE: dict[str, str] = {}
DEFAULT_WOD_LOG_DB_ID = "f94bd9bc79384b53b18bf3d2afaf9881"
DEFAULT_MOVEMENTS_DB_ID = "ecf5ac8381ce41a98fa804a1694977bb"


def _cf_config(config: dict, name: str, default: str = "") -> str:
    defaults = {
        "NOTION_MOVEMENTS_DB": DEFAULT_MOVEMENTS_DB_ID,
        "NOTION_WOD_LOG_DB": DEFAULT_WOD_LOG_DB_ID,
    }
    fallback = default or defaults.get(name, "")
    return str((config or {}).get(name) or os.environ.get(name) or fallback).strip()


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


async def _prompt_wod_result_notes(message, key: str, state: dict) -> None:
    result_type = _infer_result_type(state.get("format"))
    if result_type == "Time":
        prompt = "⏱ What was your time? (mm:ss)\nExample: 12:34 for 12 minutes 34 seconds.\nAdd any notes too, or tap Skip."
    elif result_type == "Reps":
        prompt = "💪 How many total reps did you complete?\nAdd any notes too, or tap Skip."
    else:
        prompt = "🔄 How many rounds + reps did you complete?\nExamples: 5 rounds + 12 reps, or 5 for full rounds.\nAdd any notes too, or tap Skip."
    await message.reply_text(
        prompt,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]),
    )


async def _prompt_wod_result_before_rx(message, key: str, state: dict) -> None:
    """Ask for the WOD result before presenting Rx/Scaled choices."""
    state["stage"] = "result"
    await _prompt_wod_result_notes(message, key, state)


def _restore_pid(pid: str) -> str:
    return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def parse_rounds_reps(text: str):
    m = re.search(r"(\d+)\s*(?:\+|rounds?)\s*(\d+)", text.lower())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def parse_time_to_seconds(text: str):
    m = re.search(r"(\d+):(\d{2})", text)
    return (int(m.group(1)) * 60 + int(m.group(2))) if m else None


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
        print(f"[DEBUG] Extracted workout data: {workout_data}")

    extracted_movements = workout_data.get("movements") or []
    movement_text = ", ".join(extracted_movements) if extracted_movements else (workout_result.get("movement") or "").strip()
    sets = workout_data.get("sets") if workout_data.get("sets") is not None else workout_result.get("sets")
    reps = workout_data.get("reps") if workout_data.get("reps") is not None else workout_result.get("reps")
    load_lbs = workout_data.get("weight_lbs") if workout_data.get("weight_lbs") is not None else workout_result.get("load_lbs")
    scheme = workout_data.get("scheme") or (f"{sets}x{reps}" if sets and reps else None)

    cf_pending[key] = {
        "mode": "strength",
        "stage": "movement" if not movement_text else "notes",
        "movement": movement_text,
        "load_lbs": load_lbs or 0,
        "sets": sets or 1,
        "reps": reps or 1,
        "workout_date": workout_data.get("date"),
        "effort_scheme": scheme,
        "notes": workout_data.get("notes"),
    }
    if not movement_text:
        await message.reply_text("🏋️ Which movement did you train?", parse_mode="Markdown")
        return
    movement_ids, names = await _resolve_movement_ids(movement_text, claude, notion, config, message)
    cf_pending[key]["movement_page_ids"] = movement_ids
    cf_pending[key]["movement_page_id"] = movement_ids[0] if movement_ids else None
    cf_pending[key]["movement"] = ", ".join(names) if names else movement_text
    if movement_ids and await handle_gymnastics_level_check(message, movement_ids[0], cf_pending[key]["movement"], notion, config, cf_pending, key):
        return
    has_complete_extraction = bool(raw_text and sets is not None and reps is not None and load_lbs is not None)
    if has_complete_extraction:
        await _finalize_flow(message, key, notion, config, cf_pending, cf_pending[key].get("notes"))
        return
    await message.reply_text("📝 Any notes about this session?\n(Reply with text, or tap Skip)", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]))


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
    del notion, config
    key = str(message.chat_id)
    cf_pending[key] = {"mode": "subs", "stage": "movement"}
    await message.reply_text("Which movement?", parse_mode="Markdown")


async def handle_cf_prs(message, notion, config):
    del notion, config
    await message.reply_text("🏆 Recent PRs\n\n(Connect PR DB to view entries)", parse_mode="Markdown")


async def _finalize_flow(message, key, notion, config, cf_pending, notes=None):
    state = cf_pending.get(key) or {}
    if state.get("mode") == "strength":
        movement_id = state.get("movement_page_id") or await asyncio.get_running_loop().run_in_executor(
            None, lambda: get_or_create_movement(notion, _cf_config(config, "NOTION_MOVEMENTS_DB"), state.get("movement") or "Unknown")
        )
        effort_sets = int(state.get("sets") or 1)
        effort_reps = int(state.get("reps") or 1)
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
        weekly_program_id = await get_current_week_program_url(notion)
        movement_ids = state.get("movement_page_ids") or [movement_id]
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: create_strength_log(
                notion,
                _cf_config(config, "NOTION_WORKOUT_LOG_DB"),
                movement_ids,
                state.get("movement") or "Unknown",
                float(state.get("load_lbs") or 0),
                effort_sets,
                effort_reps,
                False,
                weekly_program_id,
                None,
                None,
                state.get("workout_date"),
                state.get("effort_scheme"),
            ),
        )
        confirm_msg = "✅ Strength logged to Workout Log v2!"
        if state.get("workout_date"):
            confirm_msg += f"\n📅 Date: {state['workout_date']}"
        if state.get("movement"):
            confirm_msg += f"\n💪 Movement: {state['movement']}"
        if state.get("effort_scheme"):
            confirm_msg += f"\n📊 Scheme: {state['effort_scheme']}"
        if state.get("load_lbs"):
            confirm_msg += f"\n⚖️ Weight: {state['load_lbs']}lbs"
        await message.reply_text(confirm_msg, parse_mode="Markdown")
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
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: create_wod_log(
                notion,
                target_wod_db,
                _format_label(state.get("format")),
                None,
                None,
                result_type,
                result_seconds,
                result_rounds,
                result_reps,
                _rx_scaled_label(state.get("rx_scaled")),
                notes,
                False,
                None,
                state.get("movement_page_ids") or [],
                weekly_program_id,
                None,
            ),
        )
        await message.reply_text("✅ WOD logged to WOD Log!", parse_mode="Markdown")
    cf_pending.pop(key, None)


async def handle_cf_text_reply(message, text, cf_flow_key, claude, notion, config, cf_pending):
    state = cf_pending.get(cf_flow_key) or {}
    if state.get("mode") == "strength" and state.get("stage") == "movement":
        movement_name = text.strip()
        if not movement_name:
            await message.reply_text("Please send a movement name first.")
            return
        movement_ids, names = await _resolve_movement_ids(movement_name, claude, notion, config, message)
        movement_id = movement_ids[0] if movement_ids else None
        state["movement"] = ", ".join(names) if names else movement_name
        state["movement_page_ids"] = movement_ids
        state["movement_page_id"] = movement_id
        state["stage"] = "notes"
        cf_pending[cf_flow_key] = state
        if movement_id and await handle_gymnastics_level_check(message, movement_id, state["movement"], notion, config, cf_pending, cf_flow_key):
            return
        await message.reply_text("📝 Any notes about this session?\n(Reply with text, or tap Skip)", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{cf_flow_key}")]]))
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
        movement_ids, names = await _resolve_movement_ids(text, claude, notion, config, message)
        state["movements"] = names
        state["movement_page_ids"] = movement_ids
        cf_pending[cf_flow_key] = state
        await _prompt_wod_result_before_rx(message, cf_flow_key, state)
        return
    if state.get("mode") == "wod" and state.get("stage") == "result":
        state["result_notes"] = text
        state["stage"] = "rx_scaled"
        cf_pending[cf_flow_key] = state
        await message.reply_text("Rx or Scaled?", parse_mode="Markdown", reply_markup=rx_scaled_keyboard(cf_flow_key))
        return
    if state.get("mode") == "subs" and state.get("stage") == "movement":
        state["movement"] = text
        state["stage"] = "subtype"
        cf_pending[cf_flow_key] = state
        await message.reply_text("Select type:", parse_mode="Markdown", reply_markup=sub_type_keyboard(cf_flow_key))
        return
    if state.get("stage") == "notes":
        await _finalize_flow(message, cf_flow_key, notion, config, cf_pending, text)


async def _prompt_readiness_field(message, key: str, field: str):
    labels = {
        "sleep_quality": "Sleep quality",
        "energy": "Energy",
        "mood": "Mood",
        "stress": "Stress",
        "soreness": "Soreness",
    }
    from .keyboards import readiness_keyboard

    await message.reply_text(f"📊 {labels[field]} (1-5)?", reply_markup=readiness_keyboard(key, field))


async def handle_cf_callback(q, parts, claude, notion, config, cf_pending):
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
    elif parts[1] == "subs":
        await handle_cf_subs_flow(q.message, notion, config, cf_pending)
    elif parts[1] == "prs":
        await handle_cf_prs(q.message, notion, config)
    elif parts[1] == "log_readiness":
        if await check_readiness_logged_today(notion, _cf_config(config, "NOTION_DAILY_READINESS_DB")):
            await q.edit_message_text("✅ Readiness is already logged for today.")
            return
        key = str(q.message.chat_id)
        cf_pending[key] = {"mode": "readiness", "stage": "sleep_quality", "readiness": {}}
        await _prompt_readiness_field(q.message, key, "sleep_quality")
    elif parts[1] == "ready" and len(parts) == 5:
        key, field, value = parts[2], parts[3], parts[4]
        state = cf_pending.get(key, {"mode": "readiness", "readiness": {}})
        state.setdefault("readiness", {})[field] = value
        order = ["sleep_quality", "energy", "mood", "stress", "soreness"]
        try:
            next_field = order[order.index(field) + 1]
        except (ValueError, IndexError):
            scores = state.get("readiness", {})
            print(f"[DEBUG] All readiness scores collected: {scores}")
            print("[DEBUG] Calling log_daily_readiness...")
            try:
                await log_daily_readiness(
                    notion,
                    sleep_quality=scores.get("sleep_quality", value),
                    energy=scores.get("energy", value),
                    mood=scores.get("mood", value),
                    stress=scores.get("stress", value),
                    soreness=scores.get("soreness", value),
                    daily_readiness_db_id=_cf_config(config, "NOTION_DAILY_READINESS_DB"),
                )
            except Exception as e:
                print(f"[ERROR] Readiness logging failed: {e}")
                log.exception("Readiness logging failed")
                await q.edit_message_text(f"❌ Error logging readiness: {e}")
                return
            print("[DEBUG] Readiness logged successfully")
            cf_pending.pop(key, None)
            await q.edit_message_text("✅ Readiness logged!")
            return
        state["stage"] = next_field
        cf_pending[key] = state
        await q.edit_message_text(f"✅ {field.replace('_', ' ').title()}: {value}")
        await _prompt_readiness_field(q.message, key, next_field)
    elif parts[1] == "fmt" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {"mode": "wod"})
        state["format"] = parts[3]
        cf_pending[key] = state
        print(f"[DEBUG] WOD format selected: {_format_label(parts[3])}")
        await q.edit_message_text(f"✅ Format: {_format_label(parts[3])}", parse_mode="Markdown")
        if state.get("movement_page_ids"):
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
    elif parts[1] == "subtype" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {})
        rows = query_subs(notion, config.get("NOTION_SUBS_DB", ""), config.get("NOTION_MOVEMENTS_DB", ""), state.get("movement", ""), parts[3])
        await q.edit_message_text("Nothing in Subs & Recs for that movement yet." if not rows else "\n".join([f"{i+1}. {r['name']} — {r['difficulty']}" for i, r in enumerate(rows)]), parse_mode="Markdown")
        cf_pending.pop(key, None)
    elif parts[1] == "skip" and len(parts) == 3:
        await _finalize_flow(q.message, parts[2], notion, config, cf_pending, None)
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
# [ ] Test "Log WOD (C)" writes to NOTION_WOD_LOG_DB, NOT NOTION_WORKOUT_LOG_DB
# [ ] Test Result Type auto-infers: AMRAP -> Rounds, For Time -> Time
# [ ] Test Movement field contains page IDs only (no sets/reps/weight)
# [ ] Test weekly_program_ref auto-populates with current week page ID
# [ ] Test "Others" button routes to strength/accessory flow -> Workout Log v2
# [ ] Verify no readiness fields in either log payload

# TESTING CHECKLIST — Phase 1 Main Bot Integration
# [ ] Test bot startup loads MOVEMENTS_CACHE with >20 movements
# [ ] Test CrossFit menu shows "Log Readiness" button on first open
# [ ] Test button hides after readiness logged
# [ ] Test "Log Strength (B)" triggers handle_cf_strength_flow
# [ ] Test "Log WOD (C)" triggers handle_cf_wod_flow
# [ ] Test "Sub / Add-on" triggers handle_cf_subs_flow
# [ ] Verify all logs write to correct databases
