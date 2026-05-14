"""Telegram routing dispatchers for incoming messages and callbacks."""

from __future__ import annotations

import logging
from second_brain.notion.properties import title_prop
from second_brain.state import STATE
import second_brain.palette as _palette
import second_brain.keyboards as _kb_direct
import second_brain.formatters as _fmt_direct
from second_brain.utils import local_today


log = logging.getLogger(__name__)


def _sync_main_globals() -> None:
    """Expose entrypoint globals needed by transitional routing code."""
    import second_brain.main as _main  # transition import

    globals().update({
        name: value
        for name, value in vars(_main).items()
        if not name.startswith("__") and name != "log"
    })


def _set_main_global(name: str, value) -> None:
    """Keep transitional router-owned assignments in sync with the entrypoint."""
    import second_brain.main as _main  # transition import

    setattr(_main, name, value)
    globals()[name] = value


async def route_classified_message_v10(message, text: str) -> None:
    _sync_main_globals()
    thinking = await message.reply_text("🧠 Got it...")
    if NOTION_WORKOUT_LOG_DB or NOTION_WOD_LOG_DB or NOTION_WORKOUT_PROGRAM_DB:
        try:
            workout_result = await asyncio.get_running_loop().run_in_executor(None, lambda: classify_workout_message(text, claude, CLAUDE_MODEL, CLAUDE_MAX_TOK))
        except Exception:
            workout_result = {"type": "none"}
        if workout_result.get("type") == "programme":
            await thinking.delete()
            await message.reply_text(
                "📋 Weekly programmes are parsed from Notion only now.\n"
                "Add a row in Weekly Programs, paste into *Full Program*, and leave *Processed* unchecked.",
                parse_mode="Markdown",
            )
            return
        if workout_result.get("type") in ("strength", "conditioning") and workout_result.get("confidence") == "high":
            workout_result["raw_text"] = text
            await thinking.delete()
            if workout_result.get("type") == "strength":
                await handle_cf_strength_flow(message, workout_result, claude, notion, _crossfit_config(), cf_pending)
            else:
                await handle_cf_wod_flow(message, workout_result, notion, _crossfit_config(), cf_pending)
            return
    if await wl.handle_photo_followup(notion, message, text):
        await thinking.delete()
        return
    try:
        result = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, lambda: ai_classify.classify_message(claude, CLAUDE_MODEL, text, list(habit_cache.keys()), bool(NOTION_WATCHLIST_DB), bool(NOTION_WANTSLIST_V2_DB), bool(NOTION_PHOTO_DB), bool(NOTION_NOTES_DB), local_today())),
            timeout=18,
        )
    except asyncio.TimeoutError:
        log.warning("Claude v10 classify timeout after 18s; falling back to task capture")
        await thinking.delete()
        await create_or_prompt_task(message, text)
        return
    except Exception as e:
        log.error(f"Claude v10 classify error: {e}")
        await thinking.delete()
        await create_or_prompt_task(message, text)
        return

    global _entertainment_counter
    intent = result.get("type")

    if intent == "watchlist":
        await thinking.delete()
        await wl.handle_watchlist_intent(notion, message, title=result.get("title", text), media_type=result.get("media_type", "Series"))
        return

    if intent == "wantslist":
        await thinking.delete()
        await wl.handle_wantslist_intent(message, item=result.get("item", text), category=result.get("category", "Other"))
        return

    if intent == "photo":
        await thinking.delete()
        await wl.handle_photo_intent(notion, message, subject=result.get("subject", text))
        return

    if intent == "note":
        await thinking.delete()
        await start_note_capture_flow(message, result.get("content", text))
        return

    if intent == "habit":
        habit_name = result.get("habit_name")
        confidence = result.get("confidence", "low")
        if habit_name and habit_name in habit_cache and confidence == "high":
            habit = habit_cache[habit_name]
            habit_pid = habit["page_id"]
            if already_logged_today(habit_pid):
                await thinking.edit_text(f"Already logged {habit_name} today! ✅")
            else:
                log_habit(habit_pid, habit_name)
                await thinking.edit_text(f"✅ Logged!\n\n{habit_name}\n📅 {date.today().strftime('%B %-d')}")
                asyncio.create_task(
                    check_and_notify_weekly_goals(
                        message.get_bot(),
                        MY_CHAT_ID,
                        notion,
                        NOTION_LOG_DB,
                        NOTION_HABIT_DB,
                        habit_cache,
                        notified_goals_this_week,
                        get_week_completion_count,
                        get_habit_frequency,
                    )
                )
        else:
            all_habits = [{"page_id": h["page_id"], "name": name} for name, h in habit_cache.items()]
            all_habits.sort(key=lambda h: h["name"].lower())
            await thinking.edit_text("Which habit did you complete?", reply_markup=kb.habit_buttons(all_habits, "manual", selected=set()))
            _store_habit_selection_session(thinking.message_id, all_habits)
        return

    if intent == "entertainment_log":
        title = (result.get("title") or "").strip()
        confidence = result.get("confidence", "low")
        result.setdefault("date", local_today().isoformat())
        date_result = _apply_shared_date_parse(result)
        if date_result and getattr(date_result, "ambiguous", False):
            key = str(_entertainment_counter)
            _set_main_global("_entertainment_counter", _entertainment_counter + 1)
            pending_map[key] = {"type": "entertainment_log", "payload": result, "raw_text": text}
            await thinking.edit_text("📅 Which date did you mean?", reply_markup=kb.date_pick_keyboard("ent", key, date_result))
            return
        if confidence == "high" and title:
            try:
                await thinking.delete()
                await handle_entertainment_log(notion, message, result)
            except Exception as e:
                log.error("Entertainment save error: %s", e)
                await message.reply_text("⚠️ I understood that as entertainment, but couldn't save to Notion.")
            return

        key = str(_entertainment_counter)
        _set_main_global("_entertainment_counter", _entertainment_counter + 1)
        pending_map[key] = {"type": "entertainment_log", "payload": result, "raw_text": text}
        preview = title or text
        await thinking.edit_text(
            f"🎬 I think this is an entertainment log:\n\n*{preview}*\n\nSave it?",
            parse_mode="Markdown",
            reply_markup=kb.entertainment_confirm_keyboard(key),
        )
        return

    await thinking.delete()
    await create_or_prompt_task(message, text)


async def handle_message_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _sync_main_globals()
    global _entertainment_counter
    user_id = update.effective_chat.id
    if str(user_id) in cf_pending:
        logger.info(f"[CF_ENTRY] function=handle_message_text user={user_id} stage={cf_pending.get(str(user_id), {}).get('stage')}")
    if user_id != MY_CHAT_ID:
        return
    message = update.message
    text    = (message.text or "").strip()
    if not text:
        return
    lower = text.lower().strip()
    lower = lower.replace("️", "").replace("‍", "")
    lower_normalized = re.sub(r"\s+", " ", lower).strip()
    if lower.startswith("/signoff"):
        note = text.split(" ", 1)[1].strip() if " " in text else ""
        await trigger_signoff_now(message, note=note or None)
        return
    if lower == "signoff":
        await trigger_signoff_now(message)
        return
    match_signoff_sb = re.match(r"signoff_secondbrain:\s*(.+)$", text, re.IGNORECASE)
    if match_signoff_sb:
        note = match_signoff_sb.group(1).strip()
        store_signoff_note("second_brain", note)
        await message.reply_text(
            f"📓 Second Brain signoff noted.\n\n_{note}_",
            parse_mode="Markdown",
        )
        return

    match_signoff_b2 = re.match(r"signoff_brian:\s*(.+)$", text, re.IGNORECASE)
    if match_signoff_b2:
        note = match_signoff_b2.group(1).strip()
        store_signoff_note("brian_ii", note)
        await message.reply_text(
            f"📓 Brian II signoff noted.\n\n_{note}_",
            parse_mode="Markdown",
        )
        return

    if not (
        lower.startswith("signoff_secondbrain:")
        or lower.startswith("signoff_brian:")
        or lower.startswith("/signoff")
        or lower == "signoff"
    ):
        track_claude_activity(text)

    if lower == "cancel":
        if message.reply_to_message and message.reply_to_message.message_id in digest_map:
            digest_map.pop(message.reply_to_message.message_id, None)
            await message.reply_text("✅ Dismissed")
            return
        await message.reply_text("Reply to a digest message with `cancel` to dismiss it.")
        return
    command_head = lower.split(maxsplit=1)[0] if lower else ""
    command_arg_text = text[len(text.split(maxsplit=1)[0]):].strip() if text.split(maxsplit=1) else ""

    global awaiting_packing_feedback
    if message.reply_to_message and message.reply_to_message.message_id in trip_awaiting_date_map:
        key = trip_awaiting_date_map.pop(message.reply_to_message.message_id)
        parsed = trips_mod.parse_trip_message(text, claude)
        dep, ret = parsed.get("departure_date"), parsed.get("return_date")
        if not dep or not ret:
            await message.reply_text("⚠️ I couldn't parse those dates. Try format like Jun 14-17.")
            return
        trip_map[key]["departure_date"] = dep
        trip_map[key]["return_date"] = ret
        nights = (date.fromisoformat(ret) - date.fromisoformat(dep)).days
        trip_map[key]["nights"] = nights
        trip_days = nights + 1
        trip_map[key]["duration_label"] = "Overnight" if trip_days <= 1 else ("2-3 Days" if trip_days <= 3 else "4-5 Days")
        purpose_str = " + ".join(trip_map[key]["purpose_list"])
        await message.reply_text(f"✈️ {trip_map[key]['destination']} — {trips_mod.format_trip_dates(dep, ret)} ({nights} night(s), {purpose_str})\n\nWhat field work are you doing?\n(Tap all that apply, then tap ✅ Done)", reply_markup=kb.field_work_keyboard(key, trip_map))
        return

    if awaiting_packing_feedback and not command_head.startswith('/'):
        _set_main_global("awaiting_packing_feedback", False)
        try:
            notion.pages.create(parent={"database_id": NOTION_PACKING_ITEMS_DB}, properties={"Item": title_prop(text[:100]), "Always": {"checkbox": True}})
            await message.reply_text("✅ Added to packing items.")
        except Exception:
            await message.reply_text("⚠️ Couldn't save packing feedback.")
        return

    if context.user_data.get("awaiting_mute_days"):
        try:
            days = int(text)
            if days <= 0:
                raise ValueError("days must be positive")
            STATE.mute_until = datetime.now(TZ) + timedelta(days=days)
            _save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await message.reply_text(
                f"🔕 Digests paused for {days} day(s), until {STATE.mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
            )
        except Exception:
            await message.reply_text("Please send a valid positive number of days (example: 3).")
        return

    if context.user_data.get("awaiting_location"):
        if wx.set_location_smart(text, claude):
            context.user_data["awaiting_location"] = False
            await message.reply_text(f"📍 Location updated to {wx.current_location}.")
            wx.save_location_state(wx.current_location)
            try:
                await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
            except Exception as e:
                log.error("Weather quick-action failed: %s", e)
                await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
        else:
            await message.reply_text(
                "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
            )
        return

    if command_head.startswith("/location"):
        requested_location = command_arg_text.strip()
        if requested_location:
            if wx.set_location_smart(requested_location, claude):
                context.user_data["awaiting_location"] = False
                await message.reply_text(f"📍 Location updated to {wx.current_location}.")
                wx.save_location_state(wx.current_location)
                try:
                    await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
                except Exception as e:
                    log.error("Weather quick-action failed: %s", e)
                    await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
            else:
                await message.reply_text(
                    "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
                )
            return
        context.user_data["awaiting_location"] = True
        await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
        return

    if lower.startswith("weather:"):
        requested_location = ""
        if lower.startswith("weather:"):
            requested_location = text.split(":", 1)[1].strip()
            if requested_location:
                if not wx.set_location_smart(requested_location, claude):
                    await message.reply_text(
                        "Couldn't find that location. Try city/state/country or ZIP (example: Chicago IL 60605)."
                    )
                    return
                wx.save_location_state(wx.current_location)
        if not wx.current_location:
            context.user_data["awaiting_location"] = True
            await message.reply_text("📍 What location should I use for weather? (city/state/country or ZIP)")
            return
        try:
            await message.reply_text(await handle_weather(wx.current_location), parse_mode="Markdown")
        except Exception as e:
            log.error("Weather quick-action failed: %s", e)
            await message.reply_text("⚠️ Weather is temporarily unavailable. Try /weather again in a moment or /location to reset.")
        return

    pending_sport_competition = ent_log.pending_sport_competition_map.get(update.effective_chat.id)
    if pending_sport_competition:
        competition = text.strip()
        if competition:
            schema = ent_log.entertainment_schemas.get("sports") or {}
            page_id = pending_sport_competition.get("page_id")
            props = ent_log._build_sport_competition_props(schema, competition)
            if page_id and props:
                try:
                    notion_call(notion.pages.update, page_id=page_id, properties=props)
                    ent_log.pending_sport_competition_map.pop(update.effective_chat.id, None)
                    await message.reply_text(
                        f"🏆 Competition set: *{competition}*\n_Saved to Notion_",
                        parse_mode="Markdown",
                    )
                    return
                except Exception as e:
                    log.error("Sports competition update error: %s", e)
                    await message.reply_text("⚠️ I couldn't update that competition in Notion.")
                    return
            if page_id and not props:
                ent_log.pending_sport_competition_map.pop(update.effective_chat.id, None)
                await message.reply_text(
                    "⚠️ I couldn't find a Competition property in your Sports Log schema to update."
                )
                return

    # ── Weekly programme parsing is Notion-driven (15-minute poller) ──
    upload_programme_aliases = {
        "📤 upload programme",
        "📤 upload program",
        "upload programme",
        "upload program",
        "📤 upload programme...",
        "📤 upload program...",
    }
    if lower in upload_programme_aliases or looks_like_crossfit_programme(text):
        await message.reply_text(
            "📋 Weekly programmes are parsed from Notion only now.\n\n"
            "1. Open *Weekly Programs*\n"
            "2. Add a row\n"
            "3. Paste full text into *Full Program*\n"
            "4. Leave *Processed* unchecked\n\n"
            "The 15-minute job will parse and backfill this row.",
            parse_mode="Markdown",
        )
        return

    pending_custom_topic = context.user_data.get("awaiting_note_custom_topic")
    if pending_custom_topic:
        key = pending_custom_topic.get("key")
        entry = pending_note_map.pop(key, None)
        context.user_data["awaiting_note_custom_topic"] = None
        if not entry:
            await message.reply_text("⚠️ This note prompt expired — please re-send the note.")
            return
        custom_topic = text.strip()[:60]
        if not custom_topic:
            await message.reply_text("⚠️ Topic can't be empty — please re-send the note.")
            return
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, entry["content"], custom_topic)
            topic_recency_map[custom_topic] = datetime.now(timezone.utc)
            await message.reply_text(
                f"✅ Note captured!\n🏷️ {custom_topic}\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Notion note custom-topic error: {e}")
            await message.reply_text("⚠️ Couldn't save note to Notion.")
        return

    awaiting_note_capture = context.user_data.get("awaiting_note_capture")
    if awaiting_note_capture:
        if not NOTION_NOTES_DB:
            context.user_data["awaiting_note_capture"] = None
            await message.reply_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return
        if awaiting_note_capture == "link" and not re.search(r"https?://\S+", text):
            await message.reply_text("Please send a valid URL starting with http:// or https://.")
            return
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, text)
            kind_label_map = {
                "quick": "note",
                "idea": "idea",
                "code": "code snippet",
                "link": "link",
            }
            kind_label = kind_label_map.get(awaiting_note_capture, "note")
            await message.reply_text(
                f"✅ {kind_label.capitalize()} saved to Notes.",
                reply_markup=kb.quick_actions_keyboard(BTN_REFRESH, BTN_ALL_OPEN, BTN_HABITS, BTN_CROSSFIT, BTN_NOTES, BTN_WEATHER),
            )
        except Exception as e:
            log.error("fn=handle_message_text event=note_quick_save_failed err=%s", e)
            await reply_notion_error(message, "save note")
        finally:
            context.user_data["awaiting_note_capture"] = None
        return

    # note: <text or url> — explicit inline command
    match_note = re.match(r"note:\s*(.+)$", text, re.IGNORECASE)
    if match_note:
        notes_pending.discard(update.effective_chat.id)
        await handle_note_input(message, match_note.group(1).strip())
        return

    # User is in note-capture mode — next message is the note content
    if update.effective_chat.id in notes_pending:
        await handle_note_input(message, text)
        return

    if lower == "done" and message.reply_to_message:
        replied_id = message.reply_to_message.message_id
        if replied_id in capture_map:
            captured = capture_map[replied_id]
            await complete_task_by_page_id(message, captured["page_id"], captured["name"])
            return
        if replied_id in digest_map:
            await message.reply_text("Reply with `done 1` or `done 1,3`, or use `done: task name`.", parse_mode="Markdown")
            return

    command_handler = COMMAND_DISPATCH.get(lower) or COMMAND_DISPATCH.get(lower_normalized)
    if command_handler:
        await command_handler(message, context)
        return

    explicit_entertainment = ent_log.parse_explicit_entertainment_log(text)
    if explicit_entertainment:
        date_result = _apply_shared_date_parse(explicit_entertainment)
        if date_result and getattr(date_result, "ambiguous", False):
            key = str(_entertainment_counter)
            _set_main_global("_entertainment_counter", _entertainment_counter + 1)
            pending_map[key] = {"type": "entertainment_log", "payload": explicit_entertainment, "raw_text": text}
            await message.reply_text("📅 Which date did you mean?", reply_markup=kb.date_pick_keyboard("ent", key, date_result))
            return
        try:
            prompted = await ent_log._maybe_prompt_explicit_venue(notion, message, explicit_entertainment, text)
            if prompted:
                return
            await handle_entertainment_log(notion, message, explicit_entertainment)
        except Exception as e:
            log.error("Explicit entertainment text save error: %s", e)
            await message.reply_text(_entertainment_save_error_text(e, explicit_entertainment))
        return

    numbers = _palette.parse_done_numbers_command(text)
    if numbers:
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        done_names: list[str] = []

        if source_id and source_id in digest_map:
            items = digest_map[source_id]
            for n in numbers:
                if 1 <= n <= len(items):
                    pid  = items[n - 1]["page_id"]
                    name = items[n - 1]["name"]
                    notion_tasks.mark_done(notion, pid)
                    suffix = " ↻ next queued" if notion_tasks.handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")
        elif message.reply_to_message:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            recovered = notion_tasks.recover_digest_items_from_text(notion, NOTION_DB_ID, replied_text)
            for n in numbers:
                task = recovered.get(n)
                if task:
                    pid = task["page_id"]
                    name = task["name"]
                    notion_tasks.mark_done(notion, pid)
                    suffix = " ↻ next queued" if notion_tasks.handle_done_recurring(pid) else ""
                    done_names.append(f"{name}{suffix}")

        if done_names:
            msg = "Marked done:\n" + "\n".join(f"✅ {n}" for n in done_names)
            await message.reply_text(msg)
        else:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    review_numbers = _palette.parse_review_numbers_command(text)
    if review_numbers:
        source_id = message.reply_to_message.message_id if message.reply_to_message else last_digest_msg_id
        queued = 0

        if source_id and source_id in digest_map:
            items = digest_map[source_id]
            for n in review_numbers:
                if 1 <= n <= len(items):
                    task = items[n - 1]
                    await message.reply_text(
                        f"{fmt.num_emoji(n)} {task['name']}\nChoose a new horizon:",
                        reply_markup=kb.review_keyboard(task["page_id"]),
                    )
                    queued += 1
        elif message.reply_to_message:
            replied_text = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
            recovered = notion_tasks.recover_digest_items_from_text(notion, NOTION_DB_ID, replied_text)
            for n in review_numbers:
                task = recovered.get(n)
                if task:
                    await message.reply_text(
                        f"{fmt.num_emoji(n)} {task['name']}\nChoose a new horizon:",
                        reply_markup=kb.review_keyboard(task["page_id"]),
                    )
                    queued += 1

        if queued == 0:
            await message.reply_text("No recent digest found. Try replying directly to a digest message.")
        return

    match_name = re.match(r"done:\s*(.+)$", text, re.IGNORECASE)
    if match_name:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_name.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_name.group(1).strip()}\".")
        return

    match_mark_done = re.match(r"mark\s+(.+?)\s+done$", text, re.IGNORECASE)
    if match_mark_done:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_mark_done.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            await complete_task_by_page_id(message, matched["page_id"], matched["name"])
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_mark_done.group(1).strip()}\".")
        return

    match_focus = re.match(r"focus:\s*(.+)$", text, re.IGNORECASE)
    if match_focus:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_focus.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            notion_tasks.set_focus(notion, matched["page_id"], True)
            await message.reply_text(f"🎯 Focused: {matched['name']} → *Doing*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_focus.group(1).strip()}\".")
        return

    match_unfocus = re.match(r"unfocus:\s*(.+)$", text, re.IGNORECASE)
    if match_unfocus:
        matched = notion_tasks.notion_tasks.fuzzy_match(match_unfocus.group(1).strip(), notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID))
        if matched:
            notion_tasks.set_focus(notion, matched["page_id"], False)
            await message.reply_text(f"⬜ Unfocused: {matched['name']} → *To Do*", parse_mode="Markdown")
        else:
            await message.reply_text(f"Couldn't find a task matching \"{match_unfocus.group(1).strip()}\".")
        return

    cf_flow_key = context.user_data.get("cf_flow_key")
    if cf_flow_key and cf_flow_key in cf_pending:
        await handle_cf_text_reply(message, text, cf_flow_key, claude, notion, _crossfit_config(), cf_pending)
        return

    match_force = re.match(r"force:\s*(.+)$", text, re.IGNORECASE)
    if match_force:
        await create_or_prompt_task(message, match_force.group(1).strip(), force_create=True); return

    await route_classified_message_v10(message, text)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _sync_main_globals()
    q     = update.callback_query
    data = q.data or ""
    is_habit_multi_select = data.startswith("h:toggle:") or data == "h:done" or data == "h:check:cancel"
    is_trip_field_work_multi_select = data.startswith("tw:")
    # Collapse the keyboard that was tapped — applies universally to all inline keyboards
    # except multi-select keyboards, which must stay visible while toggling.
    if not is_habit_multi_select and not is_trip_field_work_multi_select:
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass  # Message may already be edited or deleted — safe to ignore
        await q.answer()
    # Callback prefix registry
    # hc:{page_id}           — habit check-in (log habit); hl redirects here
    # h:toggle:{page_id}     — toggle habit selection
    # h:done                 — log selected habits
    # nt:{key}:{code}        — new task horizon picker
    # ntctx:{key}:{ctx}      — new task context picker
    # d:{page_id}            — mark task done
    # h:{page_id}:{code}     — reassign horizon
    # td:{key}:{idx}         — to-do picker mark done
    # tdc:{key}              — to-do picker cancel
    # dp:{key}:{idx}         — done picker select
    # dpp:{key}:{page}       — done picker paginate
    # dpc:{key}              — done picker cancel
    # el:{key}:{action}      — entertainment log confirm
    # qp:{action}            — command palette
    # qv:{view}              — quick horizon view
    # mq:{action}            — mute options
    # nq:{mode}              — notes quick capture
    # note_topic:{key}:{ref} — note topic picker
    # cf:{action}            — crossfit flow; cf:A aliases readiness logging
    # tw:{key}:{slug}        — trip field work picker
    # twd/tms/tcl:{key}      — trip flow steps
    # tcancel:{key}          — trip flow cancel
    # wl_save/wl_cancel      — wantslist confirm
    # tmdb_pick/skip/cancel  — watchlist TMDB picker
    # confirm_batch/cancel_batch:{message_id} — explicit multi-task confirmation
    # save_task/cancel_task:{message_id} — low-confidence task preview
    log.debug("Callback received: %s", q.data)
    parts = q.data.split(":")
    cf_chain_after_readiness = parts[:2] == ["cf", "log_readiness"]
    if len(parts) == 1 and q.data.startswith("cf_"):
        parts = ["cf", q.data.removeprefix("cf_")]
        cf_chain_after_readiness = parts[:2] == ["cf", "log_readiness"]
        log.debug("Normalized CrossFit callback to: %s", ":".join(parts))
    if parts[:2] == ["cf", "A"]:
        parts = ["cf", "log_readiness", *parts[2:]]
        cf_chain_after_readiness = False
        log.debug("Normalized CrossFit readiness callback to: %s", ":".join(parts))
    if parts[0] == "hl":
        parts[0] = "hc"
    if parts[0] == "confirm_batch" and len(parts) == 2:
        await on_confirm_batch(update, context)
        return
    if parts[0] == "cancel_batch" and len(parts) == 2:
        await on_cancel_batch(update, context)
        return
    if parts[0] in {"save_task", "cancel_task"} and len(parts) == 2:
        try:
            preview_message_id = int(parts[1])
        except ValueError:
            await q.edit_message_text("⚠️ This task preview is invalid — please send it again.")
            return
        entry = preview_map.pop(preview_message_id, None)
        if not entry:
            await q.edit_message_text("⚠️ This task preview expired — please send it again.")
            return
        if parts[0] == "cancel_task":
            await q.edit_message_text("❌ Task canceled.")
            return
        task_name = entry.get("task_name") or "Untitled task"
        deadline_days = entry.get("deadline_days")
        ctx = entry.get("context", "🏠 Personal")
        recurring = entry.get("recurring", "None") or "None"
        repeat_day = entry.get("repeat_day")
        try:
            page_id = notion_tasks.create_task(
                notion,
                NOTION_DB_ID,
                task_name,
                deadline_days,
                ctx,
                recurring=recurring,
                repeat_day=repeat_day,
            )
        except Exception as e:
            log.error("Notion error for preview task '%s': %s", task_name, e)
            await q.edit_message_text("⚠️ Preview confirmed but couldn't write to Notion.")
            return
        horizon_label = deadline_days_to_label(deadline_days)
        recur_tag = f"\n🔁 {recurring}" if recurring != "None" else ""
        await q.edit_message_text(
            f"✅ Captured!\n\n📝 {task_name}\n🕐 {horizon_label}  {ctx}{recur_tag}\n\n_Saved to Notion_",
            parse_mode="Markdown",
        )
        capture_map[q.message.message_id] = {"page_id": page_id, "name": task_name}
        return
    if await handle_v10_callback(q, parts):
        return
    if parts[0] == "date_pick" and len(parts) == 4:
        _, scope, choice, key = parts
        entry = pending_map.pop(key, None)
        if scope != "ent" or not entry or entry.get("type") != "entertainment_log":
            await q.edit_message_text("⚠️ This date prompt expired — please send it again.")
            return
        payload = dict(entry.get("payload") or {})
        payload["date"] = payload.get("raw_date_a") if choice == "a" else payload.get("raw_date_b")
        if not payload.get("date"):
            raw = parse_date(entry.get("raw_text"))
            payload["date"] = raw.option_a if choice == "a" else raw.option_b
        payload.pop("raw_date_a", None)
        payload.pop("raw_date_b", None)
        try:
            await handle_entertainment_log(notion, q.message, payload)
            await q.edit_message_text(f"✅ Date: {payload.get('date')}")
        except Exception as e:
            log.error("Entertainment date-pick save error: %s", e)
            await q.edit_message_text(_entertainment_save_error_text(e, payload))
        return
    if parts[0] == "tcancel" and len(parts) == 2:
        key = parts[1]
        trip_map.pop(key, None)
        await q.edit_message_reply_markup(reply_markup=None)
        return

    if parts[0] == "tw" and len(parts) == 3:
        _, key, slug = parts
        await q.answer()
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        # TEST: /trip work Austin Jun 14-17 → field work keyboard shows 7 options
        # TEST: Tap "Noise Measurements" → toggled with ✅ prefix, no crash
        # TEST: Tap "Vibration Measurements" → independent toggle
        # TEST: Tap "RT Measurements" → independent toggle
        # TEST: Tap "None" → clears all other selections
        # TEST: Tap ✅ Done → flow proceeds to multiple sites question
        # TEST: Old slug "st" no longer appears in keyboard or callback
        slug_to_label = {
            "sw": "Site Walk",
            "nm": "Noise Measurements",
            "vm": "Vibration Measurements",
            "rt": "RT Measurements",
            "it": "Isolation Testing",
            "hm": "24hr Monitoring",
            "nn": "None",
        }
        label = slug_to_label.get(slug)
        current = trip_map[key].get("field_work_types", [])
        if label == "None":
            trip_map[key]["field_work_types"] = ["None"]
        elif label in current:
            current.remove(label); trip_map[key]["field_work_types"] = current
        elif label:
            current = [x for x in current if x != "None"]; current.append(label); trip_map[key]["field_work_types"] = current
        new_markup = kb.field_work_keyboard(key, trip_map)
        current_markup = q.message.reply_markup
        if str(new_markup.inline_keyboard) != str(current_markup.inline_keyboard if current_markup else None):
            await q.edit_message_reply_markup(reply_markup=new_markup)
        return

    if parts[0] == "twd" and len(parts) == 2:
        key = parts[1]
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        if not trip_map[key].get("field_work_types"):
            trip_map[key]["field_work_types"] = ["None"]
        selected = ", ".join(trip_map[key].get("field_work_types") or []) or "None"
        await q.edit_message_text(f"🔬 Field work: {selected}", reply_markup=None)
        await q.message.reply_text(
            "Multiple sites on this trip?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Yes", callback_data=f"tms:{key}:y"),
                    InlineKeyboardButton("No", callback_data=f"tms:{key}:n"),
                ],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"tcancel:{key}")],
            ]),
        )
        return

    if parts[0] == "tms" and len(parts) == 3:
        _, key, ans = parts
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        trip_map[key]["multiple_sites"] = (ans == "y")
        summary = "Yes" if trip_map[key]["multiple_sites"] else "No"
        await q.edit_message_text(f"🏗️ Multiple sites: {summary}", reply_markup=None)
        await q.message.reply_text(
            "Checking a bag?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Yes", callback_data=f"tcl:{key}:y"),
                    InlineKeyboardButton("No", callback_data=f"tcl:{key}:n"),
                ],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"tcancel:{key}")],
            ]),
        )
        return

    if parts[0] == "tcl" and len(parts) == 3:
        _, key, ans = parts
        if key not in trip_map:
            await q.edit_message_text("⚠️ Trip session expired. Use /trip again.")
            return
        trip_map[key]["checked_luggage"] = (ans == "y")
        summary = "Yes" if trip_map[key]["checked_luggage"] else "No"
        await q.edit_message_text(f"🧳 Checked bag: {summary}", reply_markup=None)
        await q.message.reply_text("🧠 Building your packing list...")
        await trips_mod.execute_trip(
            key,
            q,
            notion=notion,
            claude=claude,
            trip_map=trip_map,
            set_awaiting_packing_feedback=lambda value: _set_main_global("awaiting_packing_feedback", value),
            fetch_weather=wx.fetch_weather,
            fetch_trip_weather_range=wx.fetch_trip_weather_range,
            schedule_weather_refresh=schedule_weather_refresh,
        )
        return

    if parts[0] == "cf":
        if len(parts) > 1 and parts[1] == "upload_programme":
            context.user_data["awaiting_programme_upload"] = True
            cf_pending["__awaiting_upload__"] = True
            await q.message.reply_text(
                "📋 *Upload Weekly Programme*\n\nPaste the full programme text now.\n_Paste the whole thing — I'll extract Performance, Fitness and Hyrox._",
                parse_mode="Markdown",
            )
            return
        else:
            context.user_data["cf_flow_key"] = str(q.message.chat_id)
        await handle_cf_callback(
            q,
            parts,
            claude,
            notion,
            _crossfit_config(),
            cf_pending,
            chain_after=cf_chain_after_readiness,
        )
        return

    if parts[0] == "kind_task" and len(parts) == 2:
        key = parts[1]
        text = pending_message_map.pop(key, None)
        if not text:
            await q.edit_message_text("⚠️ This prompt expired — please send it again.")
            return
        await q.edit_message_text("📌 Routed to task flow.")
        if looks_like_task_batch(text):
            await create_or_prompt_task(q.message, text)
        else:
            await route_classified_message_v10(q.message, text)
        return

    if parts[0] == "kind_refresh" and len(parts) == 2:
        key = parts[1]
        pending_message_map.pop(key, None)
        await q.edit_message_text("🔄 Refreshed.")
        await send_quick_reminder(q.message, mode="priority")
        return

    if parts[0] == "mq" and len(parts) == 2:
        action = parts[1]
        if action == "cancel":
            await q.edit_message_text("❌ Mute action canceled.")
            return
        if action == "status":
            await q.edit_message_text(fmt.mute_status_text())
            return
        if action == "unmute":
            STATE.mute_until = None
            _save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await q.edit_message_text("🔔 Digests resumed.")
            return
        if action in {"1", "3", "7"}:
            days = int(action)
            STATE.mute_until = datetime.now(TZ) + timedelta(days=days)
            _save_mute_state()
            context.user_data["awaiting_mute_days"] = False
            await q.edit_message_text(
                f"🔕 Digests paused for {days} day(s), until {STATE.mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
            )
            return

    if parts[0] == "nq" and len(parts) == 2:
        mode = parts[1]
        if mode == "cancel":
            await q.edit_message_text("❌ Notes action canceled.")
            return
        if not NOTION_NOTES_DB:
            await q.edit_message_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return
        capture_mode = mode if mode in {"quick", "idea", "code", "link"} else "quick"
        context.user_data["awaiting_note_capture"] = capture_mode
        prompt_map = {
            "quick": "📝 Send the note text you want to save.",
            "idea": "💡 Send the idea you want to save.",
            "code": "💻 Send the code snippet you want to save.",
            "link": "🔗 Send the link you want to save.",
        }
        prompt = prompt_map[capture_mode]
        await q.edit_message_text(prompt)
        return

    if parts[0] == "kind_note" and len(parts) == 2:
        key = parts[1]
        text = pending_message_map.pop(key, None)
        if not text:
            await q.edit_message_text("⚠️ This prompt expired — please send it again.")
            return
        await q.edit_message_text("📝 Routed to note flow.")
        await start_note_capture_flow(q.message, text)
        return

    if parts[0] == "note_topic" and len(parts) == 3:
        key = parts[1]
        topic_ref = parts[2]
        entry = pending_note_map.get(key)
        if not entry:
            await q.edit_message_text("⚠️ This note prompt expired — please re-send the note.")
            return
        if topic_ref == "add":
            context.user_data["awaiting_note_custom_topic"] = {"key": key}
            await q.edit_message_text("🏷️ Send the new topic name for this note.")
            return
        pending_note_map.pop(key, None)
        if topic_ref == "none":
            selected_topic = None
        else:
            try:
                selected_topic = entry["topic_order"][int(topic_ref)]
            except Exception:
                selected_topic = None
        try:
            notion_notes.create_note_entry(notion, NOTION_NOTES_DB, entry["content"], selected_topic)
            if selected_topic:
                topic_recency_map[selected_topic] = datetime.now(timezone.utc)
            topic_line = f"\n🏷️ {selected_topic}" if selected_topic else ""
            await q.edit_message_text(
                f"✅ Note captured!\n{topic_line}\n_Saved to Notion_",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.error(f"Notion note error: {e}")
            await q.edit_message_text("⚠️ Couldn't save note to Notion.")
        return

    if parts[0] == "notes_start":
        notes_pending.add(q.message.chat_id)
        await q.edit_message_text(
            "📒 *Notes* — send me a link or type a note:",
            parse_mode="Markdown",
        )
        return

    if q.data == "h:check:cancel":
        _habit_selections.pop(q.message.message_id, None)
        await q.edit_message_text("✅ Habit check closed.")
        await q.answer()
        return

    if q.data.startswith("h:toggle:"):
        t0 = time.time()
        log.info("[PERF] Toggle start: %s", q.data)

        pid_raw = q.data.removeprefix("h:toggle:").strip()
        if not pid_raw:
            await q.answer("Habit button expired. Please open Habits again.", show_alert=True)
            log.info("[PERF] TOTAL toggle time: %.0fms", (time.time() - t0) * 1000)
            return
        habit_page_id = _restore_pid(pid_raw)
        message_id = q.message.message_id

        session = _habit_selection_session(message_id)
        selected = session["selected"]
        if not isinstance(selected, set):
            selected = set()
            session["selected"] = selected
        t1 = time.time()
        log.info("[PERF] Session loaded in %.0fms", (t1 - t0) * 1000)

        if habit_page_id in selected:
            selected.remove(habit_page_id)
        else:
            selected.add(habit_page_id)
        t2 = time.time()
        log.info("[PERF] Toggle logic in %.0fms", (t2 - t1) * 1000)

        text = q.message.text or q.message.caption or ""
        check_type = "evening" if "Evening check-in" in text else "manual" if "Which habit" in text else "morning"
        page_time = datetime.now(TZ).strftime("%H:%M") if check_type == "evening" else None
        habits = session.get("habits", [])
        if not isinstance(habits, list) or not habits:
            log.warning("Habit selection cache missing for message_id=%s; falling back to Notion refresh", message_id)
            habits = pending_habits_for_digest(time_str=page_time)
            if check_type == "manual":
                habits = [
                    h for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
                    if not already_logged_today(h["page_id"])
                ]
            session["habits"] = habits
        t3 = time.time()
        log.info("[PERF] Habits loaded in %.0fms", (t3 - t2) * 1000)

        new_markup = kb.habit_buttons(habits, check_type, selected=selected)
        t4 = time.time()
        log.info("[PERF] Buttons rendered in %.0fms", (t4 - t3) * 1000)

        await q.edit_message_reply_markup(reply_markup=new_markup)
        t5 = time.time()
        log.info("[PERF] Message edited in %.0fms", (t5 - t4) * 1000)

        await q.answer()
        t6 = time.time()
        log.info("[PERF] Callback answered in %.0fms", (t6 - t5) * 1000)
        log.info("[PERF] TOTAL toggle time: %.0fms", (t6 - t0) * 1000)
        return

    if q.data == "h:done":
        message_id = q.message.message_id
        selected_ids = set(_habit_selection_selected(message_id))
        if not selected_ids:
            await q.answer("No habits selected!", show_alert=True)
            return

        selected_habits = [h for h in habit_cache.values() if h["page_id"] in selected_ids]
        selected_habits.sort(key=lambda h: h.get("sort") or 0)
        logged_names: list[str] = []
        failed_names: list[str] = []
        for habit in selected_habits:
            habit_name = habit.get("name", "Unknown")
            try:
                if already_logged_today(habit["page_id"]):
                    continue
                log_habit(habit["page_id"], habit_name)
                logged_names.append(habit_name)
            except Exception as notion_error:
                failed_names.append(habit_name)
                log.error("Habit log Notion error for %s: %s", habit_name, notion_error)

        _habit_selections.pop(message_id, None)
        await q.edit_message_reply_markup(reply_markup=None)
        if logged_names:
            await q.message.reply_text(f"✅ Logged: {', '.join(logged_names)}")
            asyncio.create_task(
                check_and_notify_weekly_goals(
                    q.bot,
                    MY_CHAT_ID,
                    notion,
                    NOTION_LOG_DB,
                    NOTION_HABIT_DB,
                    habit_cache,
                    notified_goals_this_week,
                    get_week_completion_count,
                    get_habit_frequency,
                )
            )
        if failed_names:
            await q.message.reply_text(f"⚠️ Couldn't log: {', '.join(failed_names)}")
        if not logged_names and not failed_names:
            await q.message.reply_text("✅ Selected habits were already logged today.")
        await q.answer()
        return

    if q.data.startswith("h:log:"):
        pid_raw = q.data.removeprefix("h:log:").strip()
        if not pid_raw:
            await q.edit_message_text("⚠️ Habit button expired. Please open 🎯 Habits again.")
            return
        habit_page_id = _restore_pid(pid_raw)
        habit_name = next((n for n, h in habit_cache.items() if h["page_id"] == habit_page_id), "Unknown")

        if already_logged_today(habit_page_id):
            try:
                await q.edit_message_text(f"✅ Already logged {habit_name} today!")
            except Exception as ui_error:
                log.warning("Habit dedupe UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text(f"Already logged {habit_name} today! ✅")
            return

        try:
            log_habit(habit_page_id, habit_name)
        except Exception as notion_error:
            log.error("Habit log Notion error for %s: %s", habit_name, notion_error)
            try:
                await q.edit_message_text("⚠️ Couldn't log to Notion.")
            except Exception as ui_error:
                log.warning("Habit log error UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text("⚠️ Couldn't log to Notion.")
            return

        try:
            await q.edit_message_text(f"✅ {habit_name} logged!")
        except Exception as ui_error:
            log.warning("Habit success UI update failed for %s: %s", habit_name, ui_error)
            await q.message.reply_text(f"✅ {habit_name} logged!")

        asyncio.create_task(check_and_notify_weekly_goals(q.bot, MY_CHAT_ID))
        return

    if parts[0] == "h" and len(parts) >= 2:
        if parts[1] == "check" and len(parts) == 3 and parts[2] == "cancel":
            await q.edit_message_text("✅ Habit check closed.")
            return

        if parts[1] != "log" or len(parts) != 3:
            return

        habit_page_id = _restore_pid(parts[2])
        habit_name = next((n for n, h in habit_cache.items() if h["page_id"] == habit_page_id), "Unknown")

        if already_logged_today(habit_page_id):
            try:
                await q.edit_message_text(f"✅ Already logged {habit_name} today!")
            except Exception as ui_error:
                log.warning("Habit dedupe UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text(f"Already logged {habit_name} today! ✅")
            return

        try:
            log_habit(habit_page_id, habit_name)
        except Exception as notion_error:
            log.error("Habit log Notion error for %s: %s", habit_name, notion_error)
            try:
                await q.edit_message_text("⚠️ Couldn't log to Notion.")
            except Exception as ui_error:
                log.warning("Habit log error UI update failed for %s: %s", habit_name, ui_error)
                await q.message.reply_text("⚠️ Couldn't log to Notion.")
            return

        try:
            await q.edit_message_text(f"✅ {habit_name} logged!")
        except Exception as ui_error:
            log.warning("Habit success UI update failed for %s: %s", habit_name, ui_error)
            await q.message.reply_text(f"✅ {habit_name} logged!")

        try:
            if q.message:
                await open_habit_picker(q.message)
            else:
                await q.bot.send_message(chat_id=update.effective_chat.id, text="🏃 Which habit did you complete?", reply_markup=kb.habit_buttons([
                    {"page_id": h["page_id"], "name": h["name"]}
                    for h in sorted(habit_cache.values(), key=lambda x: x["sort"])
                    if not already_logged_today(h["page_id"])
                ], "manual", selected=set()))
        except Exception as follow_up_error:
            log.error("Habit follow-up picker failed after logging %s: %s", habit_name, follow_up_error)
            if q.message:
                await q.message.reply_text("✅ Logged. Send /done to continue logging more habits.")
            else:
                await q.bot.send_message(chat_id=update.effective_chat.id, text="✅ Logged. Send /done to continue logging more habits.")

        asyncio.create_task(
            check_and_notify_weekly_goals(
                q.bot,
                MY_CHAT_ID,
                notion,
                NOTION_LOG_DB,
                NOTION_HABIT_DB,
                habit_cache,
                notified_goals_this_week,
                get_week_completion_count,
                get_habit_frequency,
            )
        )
        return

    if parts[0] == "hpag" and len(parts) == 3:
        _, prefix, page_str = parts
        page_time = datetime.now(TZ).strftime("%H:%M") if prefix == "evening" else None
        all_habits = _habit_selection_habits(q.message.message_id)
        if not all_habits:
            log.warning("Habit pagination cache missing for message_id=%s; falling back to Notion refresh", q.message.message_id)
            all_habits = pending_habits_for_digest(time_str=page_time)
            _habit_selection_session(q.message.message_id)["habits"] = all_habits
        try:
            await q.edit_message_reply_markup(
                reply_markup=kb.habit_buttons(
                    all_habits,
                    prefix,
                    page=int(page_str),
                    selected=_habit_selection_selected(q.message.message_id),
                )
            )
        except Exception as e:
            log.error(f"Habit pagination error: {e}")
            await q.edit_message_text("⚠️ Couldn't update habits view.")
        return

    if parts[0] == "el" and len(parts) == 3:
        _, key, action = parts
        entry = pending_map.pop(key, None)
        if not entry or entry.get("type") != "entertainment_log":
            await q.edit_message_text("⚠️ This entertainment prompt expired — please send it again.")
            return
        payload = dict(entry.get("payload") or {})
        if action == "no":
            payload = dict(entry.get("original_payload") or payload)
        elif action in ("cancel", "save"):
            # Backward compatibility with older inline keyboards.
            if action == "cancel":
                await q.edit_message_text("❌ Not saved.")
                return
        elif action != "yes":
            await q.edit_message_text("⚠️ Invalid choice — please send the log again.")
            return
        raw_text = entry.get("raw_text", "")
        if not (payload.get("title") or "").strip():
            payload["title"] = raw_text
        payload.setdefault("date", local_today().isoformat())
        try:
            entry_id, fav_saved = ent_log.create_entertainment_log_entry(notion, payload)
            rule_fav_saved = await _execute_entertainment_rules(payload)
            label = ENTERTAINMENT_LOG_LABELS.get(payload.get("log_type"), "Entertainment")
            suffix = "\n🎞️ Added to Favourite Films" if (fav_saved or rule_fav_saved) and payload.get("log_type") == "cinema" else ""
            await q.edit_message_text(
                f"✅ Logged to {label}\n\n🎫 {payload.get('title','Untitled')}\n📅 {payload.get('date')}{suffix}\n\n_Saved to Notion_",
                parse_mode="Markdown",
            )
            if payload.get("log_type") == "sport":
                _remember_pending_sport_competition(q.message, entry_id)
                await q.message.reply_text("🏆 Logged to Sports Log. Which competition should I set for this one?")
            log.info("Entertainment confirmed and saved page_id=%s", entry_id)
        except Exception as e:
            log.error("Entertainment callback save error: %s", e)
            await q.edit_message_text(_entertainment_save_error_text(e, payload))
        return



    if parts[0] == "d" and len(parts) == 2:
        page_id = _restore_pid(parts[1])
        try:
            notion_tasks.mark_done(notion, page_id)
            suffix = "\n↻ Next instance created" if notion_tasks.handle_done_recurring(page_id) else ""
            await q.edit_message_text(f"✅ Marked as done!{suffix}")
        except Exception as e:
            log.error(f"Notion done error: {e}"); await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "h" and len(parts) == 3:
        _, pid_clean, code = parts
        page_id       = _restore_pid(pid_clean)
        horizon_label = HORIZON_LABELS.get(code, "⚪ Backburner")
        try:
            notion_tasks.set_deadline_from_horizon_code(notion, page_id, code)
            await q.edit_message_text(f"Updated → {horizon_label} ✓")
        except Exception as e:
            log.error(f"Notion horizon error: {e}"); await q.edit_message_text("⚠️ Couldn't update Notion.")
        return

    if parts[0] == "tdc" and len(parts) == 2:
        _, key = parts
        todo_picker_map.pop(key, None)
        await q.edit_message_text("✖️ To Do picker canceled.")
        return

    if parts[0] == "td" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in todo_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `✅ To Do` again.", parse_mode="Markdown")
            return
        tasks = todo_picker_map[key]
        try:
            idx = int(idx_str)
            task = tasks[idx]
        except Exception:
            await q.answer("That task is no longer available.", show_alert=False)
            return
        if task.get("_done"):
            await q.answer("Already marked done.", show_alert=False)
            return
        try:
            notion_tasks.mark_done(notion, task["page_id"])
            notion_tasks.handle_done_recurring(task["page_id"])
            task["_done"] = True
        except Exception as e:
            log.error(f"To do picker error: {e}")
            await q.edit_message_text("⚠️ Couldn't mark that task done.")
            return

        done_count = sum(1 for t in tasks if t.get("_done"))
        remaining = len(tasks) - done_count
        if remaining == 0:
            todo_picker_map.pop(key, None)
            await q.edit_message_text("🎉 All done!")
            return
        await q.edit_message_text(
            f"✅ {done_count} done · {remaining} remaining",
            reply_markup=kb.todo_picker_keyboard(key, todo_picker_map, fmt.context_emoji),
        )
        return

    if parts[0] == "dp" and len(parts) == 3:
        _, key, idx_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown"); return
        try:
            task = done_picker_map[key][int(idx_str)]
            notion_tasks.mark_done(notion, task["page_id"])
            suffix = "\n↻ Next instance created" if notion_tasks.handle_done_recurring(task["page_id"]) else ""
            await q.edit_message_text(f"✅ Done: {task['name']}{suffix}")
        except Exception as e:
            log.error(f"Done picker error: {e}"); await q.edit_message_text("⚠️ Couldn't mark that task done.")
        return

    if parts[0] == "dpp" and len(parts) == 3:
        _, key, page_str = parts
        if key not in done_picker_map:
            await q.edit_message_text("⚠️ This picker expired. Send `done` again.", parse_mode="Markdown"); return
        await q.edit_message_reply_markup(reply_markup=kb.done_picker_keyboard(key, done_picker_map, page=int(page_str)))
        return

    if parts[0] == "noop":
        await q.answer()
        return

    if parts[0] == "dpc" and len(parts) == 2:
        done_picker_map.pop(parts[1], None)
        await q.edit_message_text("Done picker closed.")
        return

    if parts[0] == "qp" and len(parts) >= 2:
        action = parts[1]

        if action == "digest":
            try:
                message, keyboard = _palette.format_digest_view(
                    notion_tasks=notion_tasks,
                    notion=notion,
                    notion_db_id=NOTION_DB_ID,
                    local_today_fn=local_today,
                    back_to_palette_keyboard=_kb_direct.back_to_palette_keyboard,
                    weather_card=_fmt_direct.format_digest_weather_card(),
                )
                await q.edit_message_text(message, reply_markup=keyboard)
            except Exception as e:
                log.error("Palette digest callback error: %s", e)
                await q.edit_message_text("⚠️ Couldn't load digest view right now.")
            return

        if action == "todo":
            context.user_data["palette_done_indices"] = set()
            message, keyboard = _palette.format_todo_view(
                notion_tasks=notion_tasks,
                notion=notion,
                notion_db_id=NOTION_DB_ID,
                local_today_fn=local_today,
                num_emoji=_fmt_direct.num_emoji,
            )
            await q.edit_message_text(message, reply_markup=keyboard)
            return

        if action == "done" and len(parts) == 3:
            try:
                idx = int(parts[2])
            except ValueError:
                await q.answer("Invalid task selection.", show_alert=False)
                return

            tasks = _get_today_tasks_for_palette()
            if idx < 0 or idx >= len(tasks):
                await q.answer("That task is no longer available.", show_alert=False)
                message, keyboard = _palette.format_todo_view(
                    notion_tasks=notion_tasks,
                    notion=notion,
                    notion_db_id=NOTION_DB_ID,
                    local_today_fn=local_today,
                    num_emoji=_fmt_direct.num_emoji,
                    marked_done_indices=context.user_data.get("palette_done_indices", set()),
                )
                await q.edit_message_text(message, reply_markup=keyboard)
                return

            done_indices = set(context.user_data.get("palette_done_indices", set()))
            if idx in done_indices:
                await q.answer("Already marked done.", show_alert=False)
            else:
                task = tasks[idx]
                try:
                    notion_tasks.mark_done(notion, task["page_id"])
                    notion_tasks.handle_done_recurring(task["page_id"])
                    done_indices.add(idx)
                    context.user_data["palette_done_indices"] = done_indices
                except Exception as e:
                    log.error("Palette done callback error: %s", e)
                    await q.edit_message_text("⚠️ Couldn't mark that task done.")
                    return

            message, keyboard = _palette.format_todo_view(
                notion_tasks=notion_tasks,
                notion=notion,
                notion_db_id=NOTION_DB_ID,
                local_today_fn=local_today,
                num_emoji=_fmt_direct.num_emoji,
                marked_done_indices=done_indices,
            )
            await q.edit_message_text(message, reply_markup=keyboard)
            return

        if action == "back":
            context.user_data.pop("palette_done_indices", None)
            await q.edit_message_text(
                "🎯 *Quick Access*",
                parse_mode="Markdown",
                reply_markup=kb.format_command_palette(),
            )
            return

        if action == "habits":
            await q.edit_message_text("🎯 Loading habits…")
            await send_daily_habits_list(q.bot)
            return

        if action == "notes":
            if NOTION_NOTES_DB:
                await q.edit_message_text("📝 Notes connected. Choose an option:", reply_markup=kb.notes_options_keyboard())
            else:
                await q.edit_message_text("📝 Notes DB isn't configured yet — add NOTION_NOTES_DB first.")
            return

        if action == "weather":
            weather_text = append_trip_reminders_to_text(fmt.format_weather_snapshot(), within_days=2)
            await q.edit_message_text(weather_text, parse_mode="Markdown")
            return

        if action == "mute":
            await q.edit_message_text(
                "🔕 Choose a mute option:",
                reply_markup=kb.mute_options_keyboard(),
            )
            return

    if parts[0] == "qv" and len(parts) == 2 and parts[1] in {"week", "backlog"}:
        try:
            message, ordered = fmt.format_week_view(parts[1])
            await q.edit_message_text(
                text=message,
                parse_mode="Markdown",
                reply_markup=kb.horizon_view_back_keyboard(),
            )
            if ordered and q.message:
                digest_map[q.message.message_id] = ordered
        except Exception as e:
            log.error("Quick-view callback error (%s): %s", q.data, e)
            await q.edit_message_text("⚠️ Couldn't load that view right now.")
        return

    if q.data == "digest:today":
        try:
            tasks = notion_tasks.get_today_and_overdue_tasks(notion, NOTION_DB_ID)
            message, ordered = fmt.format_hybrid_digest(tasks)
            await q.edit_message_text(text=message, parse_mode="Markdown")
            if ordered and q.message:
                digest_map[q.message.message_id] = ordered
        except Exception as e:
            log.error("Digest today callback error: %s", e)
            await q.edit_message_text("⚠️ Couldn't refresh today's digest right now.")
        return
