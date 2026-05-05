from __future__ import annotations

import asyncio
import logging
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .classify import parse_programme
from .keyboards import level_confirm_keyboard, my_level_keyboard, sub_type_keyboard, wod_format_keyboard
from .notion import create_strength_log, create_wod_log, get_movement_category, get_or_create_movement, get_progressions_for_movement, query_subs, save_programme, set_current_level


log = logging.getLogger(__name__)


def _restore_pid(pid: str) -> str:
    return f"{pid[:8]}-{pid[8:12]}-{pid[12:16]}-{pid[16:20]}-{pid[20:]}"


def parse_rounds_reps(text: str):
    m = re.search(r"(\d+)\s*(?:\+|rounds?)\s*(\d+)", text.lower())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def parse_time_to_seconds(text: str):
    m = re.search(r"(\d+):(\d{2})", text)
    return (int(m.group(1)) * 60 + int(m.group(2))) if m else None


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
    del claude
    if not config.get("NOTION_WORKOUT_LOG_DB") or not config.get("NOTION_MOVEMENTS_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.", parse_mode="Markdown")
        return
    key = str(message.chat_id)
    movement_name = workout_result.get("movement") or "Back Squat"
    movement_id = await asyncio.get_running_loop().run_in_executor(None, lambda: get_or_create_movement(notion, config["NOTION_MOVEMENTS_DB"], movement_name))
    cf_pending[key] = {"mode": "strength", "stage": "notes", "movement": movement_name, "movement_page_id": movement_id, "load_lbs": workout_result.get("load_lbs") or 0, "sets": workout_result.get("sets") or 1, "reps": workout_result.get("reps") or 1, "readiness": {}}
    if await handle_gymnastics_level_check(message, movement_id, movement_name, notion, config, cf_pending, key):
        return
    await message.reply_text("📝 Any notes about this session?\n(Reply with text, or tap Skip)", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]))


async def handle_cf_wod_flow(message, workout_result, notion, config, cf_pending):
    del notion
    if not config.get("NOTION_WOD_LOG_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.", parse_mode="Markdown")
        return
    key = str(message.chat_id)
    cf_pending[key] = {"mode": "wod", "stage": "format", "format": workout_result.get("format")}
    await message.reply_text("Select WOD format:", parse_mode="Markdown", reply_markup=wod_format_keyboard(key))


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
        movement_id = state.get("movement_page_id") or await asyncio.get_running_loop().run_in_executor(None, lambda: get_or_create_movement(notion, config["NOTION_MOVEMENTS_DB"], state.get("movement") or "Unknown"))
        await asyncio.get_running_loop().run_in_executor(None, lambda: create_strength_log(notion, config["NOTION_WORKOUT_LOG_DB"], movement_id, state.get("movement") or "Unknown", float(state.get("load_lbs") or 0), int(state.get("sets") or 1), int(state.get("reps") or 1), False, None, None, state.get("readiness")))
        await message.reply_text("✅ Strength logged!\n\n_Saved to Notion_", parse_mode="Markdown")
    elif state.get("mode") == "wod":
        await asyncio.get_running_loop().run_in_executor(None, lambda: create_wod_log(notion, config["NOTION_WOD_LOG_DB"], state.get("format") or "AMRAP", None, None, "Reps", None, None, None, "Rx", state.get("level_current_name") or notes, False, None, [], None, state.get("readiness")))
        await message.reply_text("✅ WOD logged!\n\n_Saved to Notion_", parse_mode="Markdown")
    cf_pending.pop(key, None)


async def handle_cf_text_reply(message, text, cf_flow_key, claude, notion, config, cf_pending):
    del claude
    state = cf_pending.get(cf_flow_key) or {}
    if state.get("mode") == "subs" and state.get("stage") == "movement":
        state["movement"] = text
        state["stage"] = "subtype"
        cf_pending[cf_flow_key] = state
        await message.reply_text("Select type:", parse_mode="Markdown", reply_markup=sub_type_keyboard(cf_flow_key))
        return
    if state.get("stage") == "notes":
        await _finalize_flow(message, cf_flow_key, notion, config, cf_pending, text)


async def handle_cf_callback(q, parts, claude, notion, config, cf_pending):
    if len(parts) < 2:
        await q.answer("Action unavailable.", show_alert=False)
        return
    if parts[1] == "log_strength":
        await handle_cf_strength_flow(q.message, {}, claude, notion, config, cf_pending)
    elif parts[1] == "log_wod":
        await handle_cf_wod_flow(q.message, {}, notion, config, cf_pending)
    elif parts[1] == "upload_programme":
        prompt = (
            "📋 *Upload Weekly Programme*\n\n"
            "Paste your programme directly into Notion:\n"
            "1. Open 📋 Weekly Programs\n"
            "2. Create a new row\n"
            "3. Paste the full programme text into *Full Program*\n"
            "4. Leave Processed unchecked\n\n"
            "_Brian II will parse it within 15 minutes and notify you here._\n\n"
            "Fallback: you can still paste short/single-day programmes in Telegram."
        )
        try:
            await q.edit_message_text(prompt, parse_mode="Markdown")
        except Exception:
            await q.message.reply_text(prompt, parse_mode="Markdown")
        cf_pending["__awaiting_upload__"] = True
        return
    elif parts[1] == "subs":
        await handle_cf_subs_flow(q.message, notion, config, cf_pending)
    elif parts[1] == "prs":
        await handle_cf_prs(q.message, notion, config)
    elif parts[1] == "fmt" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {"mode": "wod"})
        state["format"] = parts[3]
        state["stage"] = "notes"
        cf_pending[key] = state
        await q.edit_message_text("📝 Any notes about this session?\n(Reply with text, or tap Skip)", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]))
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
