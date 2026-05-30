# Fix: Sleep sync edge cases, habit digest check bug, and error reporting channel

## Context

Sleep sync was added May 29 and has had several iterative fixes to the Google Health API v4
integration. Runtime log review revealed remaining edge cases. Separately, the habit check
in `send_daily_digest` can silently suppress habits when config and caller disagree. Finally,
sync errors must route to the system log channel (`send_system_log`) and never appear in the
main bot chat — a screenshot confirmed Asana sync errors are leaking into the main chat.

---

## Fix 1 — Sleep sync: stage duration parsing double-conversion

**File: `second_brain/healthtrack/sleep.py`** — `_stage_minutes()` (~line 142)

When `stages_summary` is a dict that contains a nested `stages` or `summary` list, the code
calls `_stage_minutes(nested, ...) * 60000` to convert the returned minutes back to ms. But
inside the list branch, `_duration_ms(item)` is called on each stage dict and treats the value
as ms — so the result is already in ms (divided by 60000 at the end of the list branch). The
`* 60000` in the dict→list recursive call is correct, but only if the list branch truly returns
minutes. Verify and add a guard so there is no double-conversion if the nested list items
already expose a `durationMs` field directly:

```python
# In the list branch, change:
        total_ms += _duration_ms(item)
# to be explicit — Google Health stages expose "durationMs" at the item level:
        total_ms += _duration_ms(item.get("durationMs") or item.get("duration") or item)
```

Also, if `startTime` or `endTime` is empty after normalisation, `_parse_dt` raises a bare
`ValueError`. Catch it in `parse_sleep_data_point` and re-raise with the target date so logs
are actionable:

```python
# Before (sleep.py ~line 172)
    start_dt = _parse_dt(point.get("startTime"))
    end_dt   = _parse_dt(point.get("endTime"))

# After
    try:
        start_dt = _parse_dt(point.get("startTime"))
        end_dt   = _parse_dt(point.get("endTime"))
    except ValueError as exc:
        raise ValueError(f"sleep_sync: unparseable time fields — {exc} | point keys={list(point.keys())}") from exc
```

---

## Fix 2 — Sleep sync: backfill sends no-data result for last day silently

**File: `second_brain/healthtrack/sleep.py`** — `handle_sleep_backfill_job()` (~line 319)

When `handle_sleep_sync` returns `{"action": "no_data", ...}`, the backfill loop silently
continues. Add a log warning per missing day so it's visible in the system log:

```python
# Inside the while loop, after awaiting handle_sleep_sync:
        result = await handle_sleep_sync(
            notion=notion,
            metrics_db_id=NOTION_HEALTH_METRICS_DB,
            client_id=GOOGLE_HEALTH_CLIENT_ID,
            client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
            refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
            target_date=cursor,
            tz=TZ,
        )
        results[cursor.isoformat()] = result
        if result.get("action") == "no_data":
            log.warning("sleep_backfill: no data for wake date %s", cursor.isoformat())
```

---

## Fix 3 — Habit digest check: config silently overrides caller's include_habits

**File: `second_brain/digest.py`** — `send_daily_digest()` (~line 451)

Current logic:
```python
habits_enabled = include_habits
if config and config.get("include_habits") is not None:
    habits_enabled = bool(config.get("include_habits"))
```

If the Notion digest config page has `include_habits = False`, habits are suppressed even
when the caller explicitly passes `include_habits=True`. This causes habits to silently
disappear from digests. The config should only take effect when the caller didn't explicitly
set it. Fix: let the caller's explicit `True` always win; config only restricts when caller
passed `False` or didn't specify.

```python
# Replace lines 451-453 with:
    # Config may restrict habits, but never enables them beyond what the caller requested.
    habits_enabled = include_habits
    if habits_enabled and config and config.get("include_habits") is False:
        habits_enabled = False
```

Also strengthen the log line to catch future silent suppression:
```python
    log.info(
        "Digest habits check: habits_enabled=%s include_habits_param=%s "
        "config_include_habits=%s habit_count=%d",
        habits_enabled, include_habits,
        config.get("include_habits") if config else None,
        len(habits) if habits_enabled else -1,
    )
```

---

## Fix 4 — Error messages must go to system log channel, not main chat

**Rule (apply across all sync jobs and background tasks):**

All error paths in sync jobs must call `send_system_log(bot, ...)` — never
`bot.send_message(chat_id=MY_CHAT_ID, ...)` or `message.reply_text(...)` for system errors.

Verified correct in:
- `second_brain/healthtrack/sleep.py` → `handle_sleep_sync_job()` — ✅ uses `send_system_log`
- `second_brain/healthtrack/routes.py` → `_run_backfill()` — ✅ uses `send_system_log`
- `second_brain/main.py` → `run_asana_sync()` — ✅ uses `send_system_log`

**Action:** Audit any other background tasks or scheduler jobs that catch exceptions and send
messages. Find every occurrence of `bot.send_message` or `message.reply_text` inside an
`except` block or background task, and replace with `send_system_log` if it is a system/sync
error rather than a user-facing response.

Pattern to grep for misrouted errors:
```
grep -rn "send_message\|reply_text" second_brain/ \
  --include="*.py" | grep -v "test_" | grep -v "MY_CHAT_ID"
```

For any result inside an `except` block or scheduled job that is NOT a direct user reply,
change to:
```python
from second_brain.error_reporting import send_system_log
await send_system_log(bot, f"🚨 <Job name> failed\n{type(exc).__name__}: {exc}")
```

`send_system_log` is defined in `second_brain/error_reporting.py` and sends to
`SYSTEM_LOGS_CHAT_ID` — not to `MY_CHAT_ID`.

---

## Verification

1. **Sleep stage parsing**: Trigger a sleep sync for a known date and check logs for
   `sleep_sync: raw dataPoint` — confirm stage minutes in the Notion row match the source data.
2. **Backfill no-data warning**: Run backfill over a range with a missing day and confirm
   `sleep_backfill: no data for wake date` appears in logs.
3. **Habit digest**: Send a manual digest at a time when `include_habits=True` and Notion
   config has `include_habits` unchecked — habits should still appear.
4. **Error channel**: Trigger a deliberate failure (e.g., bad token) and confirm the error
   message appears in the system log channel, not the main bot chat.
