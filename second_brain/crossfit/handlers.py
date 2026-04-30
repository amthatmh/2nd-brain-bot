from __future__ import annotations

import asyncio
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .classify import parse_programme
from .keyboards import sub_type_keyboard, wod_format_keyboard
from .notion import create_strength_log, create_wod_log, get_or_create_movement, query_subs, save_programme


def parse_rounds_reps(text: str):
    m = re.search(r"(\d+)\s*(?:\+|rounds?)\s*(\d+)", text.lower())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def parse_time_to_seconds(text: str):
    m = re.search(r"(\d+):(\d{2})", text)
    return (int(m.group(1)) * 60 + int(m.group(2))) if m else None


async def handle_cf_upload_programme(message, text, claude_client, notion, config) -> None:
    if not config.get("NOTION_WORKOUT_PROGRAM_DB") or not config.get("NOTION_MOVEMENTS_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.", parse_mode="Markdown")
        return
    thinking = await message.reply_text("🧠 Parsing your programme...", parse_mode="Markdown")
    parsed = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: parse_programme(text, claude_client, config.get("CLAUDE_MODEL", ""), config.get("CLAUDE_PARSE_MAX_TOKENS", 4000)),
    )
    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: save_programme(notion, config["NOTION_WORKOUT_PROGRAM_DB"], config["NOTION_MOVEMENTS_DB"], parsed, text),
    )
    await thinking.edit_text(f"📋 Saved to Weekly Programs DB\nDays found: {len(parsed.get('days', []))}", parse_mode="Markdown")


async def handle_cf_strength_flow(message, workout_result, claude, notion, config, cf_pending):
    del claude
    if not config.get("NOTION_WORKOUT_LOG_DB"):
        await message.reply_text("⚠️ CrossFit module isn't configured yet.", parse_mode="Markdown")
        return
    key = str(message.chat_id)
    cf_pending[key] = {
        "mode": "strength",
        "stage": "notes",
        "movement": workout_result.get("movement") or "Back Squat",
        "load_lbs": workout_result.get("load_lbs") or 0,
        "sets": workout_result.get("sets") or 1,
        "reps": workout_result.get("reps") or 1,
        "readiness": {},
    }
    await message.reply_text(
        "📝 Any notes about this session?\n(Reply with text, or tap Skip)",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]),
    )


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
        movement_id = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_or_create_movement(notion, config["NOTION_MOVEMENTS_DB"], state.get("movement") or "Unknown")
        )
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: create_strength_log(
                notion,
                config["NOTION_WORKOUT_LOG_DB"],
                movement_id,
                state.get("movement") or "Unknown",
                float(state.get("load_lbs") or 0),
                int(state.get("sets") or 1),
                int(state.get("reps") or 1),
                False,
                None,
                None,
                state.get("readiness"),
            ),
        )
        await message.reply_text("✅ Strength logged!\n\n_Saved to Notion_", parse_mode="Markdown")
    elif state.get("mode") == "wod":
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: create_wod_log(
                notion,
                config["NOTION_WOD_LOG_DB"],
                state.get("format") or "AMRAP",
                None,
                None,
                "Reps",
                None,
                None,
                None,
                "Rx",
                notes,
                False,
                None,
                [],
                None,
                state.get("readiness"),
            ),
        )
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
    if parts[1] == "log_strength":
        await handle_cf_strength_flow(q.message, {}, claude, notion, config, cf_pending)
        return
    if parts[1] == "log_wod":
        await handle_cf_wod_flow(q.message, {}, notion, config, cf_pending)
        return
    if parts[1] == "upload_programme":
        await q.edit_message_text(
            "📋 *Upload Weekly Programme*\n\nPaste the full programme text now.\n_Performance track only is fine — I'll ignore Fitness and Hyrox._",
            parse_mode="Markdown",
        )
        return
    if parts[1] == "subs":
        await handle_cf_subs_flow(q.message, notion, config, cf_pending)
        return
    if parts[1] == "prs":
        await handle_cf_prs(q.message, notion, config)
        return
    if parts[1] == "fmt" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {"mode": "wod"})
        state["format"] = parts[3]
        state["stage"] = "notes"
        cf_pending[key] = state
        await q.message.reply_text(
            "📝 Any notes about this session?\n(Reply with text, or tap Skip)",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Skip", callback_data=f"cf:skip:{key}")]]),
        )
        return
    if parts[1] == "subtype" and len(parts) >= 4:
        key = parts[2]
        state = cf_pending.get(key, {})
        rows = query_subs(notion, config.get("NOTION_SUBS_DB", ""), config.get("NOTION_MOVEMENTS_DB", ""), state.get("movement", ""), parts[3])
        if not rows:
            await q.message.reply_text("Nothing in Subs & Recs for that movement yet.", parse_mode="Markdown")
        else:
            await q.message.reply_text("\n".join([f"{i+1}. {r['name']} — {r['difficulty']}" for i, r in enumerate(rows)]), parse_mode="Markdown")
        cf_pending.pop(key, None)
        return
    if parts[1] == "skip" and len(parts) == 3:
        await _finalize_flow(q.message, parts[2], notion, config, cf_pending, None)
