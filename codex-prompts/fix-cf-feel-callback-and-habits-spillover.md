# Fix: CrossFit feel callback crash + Habits active-habit spillover

## Fix 1 — CrossFit feel callback crashes with "Something went wrong while handling routers"

### Root cause

Two independent issues:

**A.** `handle_callback` in `second_brain/routers.py` calls `await q.answer()` outside any try-except. When the Telegram callback query expires or any API error occurs, the exception propagates from `routers.py::handle_callback` — the innermost `second_brain/` frame — producing exactly the error the user sees: _"Something went wrong while handling routers (second_brain.routers.handle_callback)"_.

**B.** Inside `_cf_feel` in `second_brain/crossfit/handlers.py`, the success-path code after the try-except block (`await q.edit_message_text(...)` and the chain prompt) is unguarded. Also, the except block uses `parse_mode="Markdown"` when formatting the error string — if the exception text contains Markdown special characters the edit itself throws a second exception that escapes.

---

### Change A — `second_brain/routers.py`, `handle_callback` (~line 1960)

```python
# Before
        await q.answer()

# After
        try:
            await q.answer()
        except Exception:
            log.debug("Could not answer callback query", exc_info=True)
```

---

### Change B — `second_brain/crossfit/handlers.py`, `_cf_feel`

```python
# Before
    except Exception as e:
        logger.exception("Session feel logging failed")
        cf_pending.pop(key, None)
        await q.edit_message_text(f"❌ Error logging session feel: {e}", parse_mode="Markdown")
        return
    chain = list(state.get("session_chain") or [])
    origin = state.get("session_origin")
    cf_pending.pop(key, None)
    await q.edit_message_text(f"✅ Session feel logged: {rating}/5", parse_mode="Markdown")
    if mode == "strength" and "c" in chain:
        cf_pending[key] = {"session_chain": chain, "session_origin": origin}
        await q.message.reply_text("🏆 Did you do Section C (WOD) today?", reply_markup=_chain_keyboard("c"))
    elif mode == "wod":
        cf_pending.pop(key, None)

# After
    except Exception as e:
        logger.exception("Session feel logging failed")
        cf_pending.pop(key, None)
        try:
            await q.edit_message_text(f"❌ Error logging session feel: {e}")
        except Exception:
            logger.debug("Could not send feel error message", exc_info=True)
        return
    try:
        chain = list(state.get("session_chain") or [])
        origin = state.get("session_origin")
        cf_pending.pop(key, None)
        await q.edit_message_text(f"✅ Session feel logged: {rating}/5")
        if mode == "strength" and "c" in chain:
            cf_pending[key] = {"session_chain": chain, "session_origin": origin}
            await q.message.reply_text("🏆 Did you do Section C (WOD) today?", reply_markup=_chain_keyboard("c"))
        elif mode == "wod":
            cf_pending.pop(key, None)
    except Exception:
        logger.exception("Session feel post-logging step failed")
```

---

## Fix 2 — Habit keyboard hides active habits after first click

### Root cause

`_cb_h_toggle` in `second_brain/routers.py` re-renders the keyboard from `session["habits"]` — a snapshot saved when the digest was first sent, filtered by `show_after` at that time. Habits whose `show_after` time falls after the digest was sent are absent from the snapshot. When the user clicks any habit the keyboard re-renders without those habits, making them disappear.

---

### Change — `second_brain/routers.py`, `_cb_h_toggle`

After the block that populates `habits` (after the last `session["habits"] = habits` inside the `if not habits:` guard, just before `t3 = time.time()`), add:

```python
    # Merge in any habits that became active (passed show_after) after the digest was sent
    now_hhmm = datetime.now(TZ).strftime("%H:%M")
    known_pids = {h.get("page_id") for h in habits}
    for h in sorted(_habit_cache().values(), key=lambda h: h.get("sort", 0)):
        pid = h.get("page_id")
        if pid in known_pids:
            continue
        show_after = h.get("show_after") or ""
        if not show_after or now_hhmm >= show_after:
            habits.append(h)
            known_pids.add(pid)
    session["habits"] = habits
```

`TZ`, `datetime`, and `_habit_cache()` are already imported/defined in `routers.py`.
