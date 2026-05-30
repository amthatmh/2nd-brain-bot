# Fix: Sleep sync remaining issues + Habit digest check bug

## Context

Sleep sync was added in d1e624d and patched three times (5e6ede4, d5c7cf7, 1f05ca7) to correct
Google Health API v4 response parsing. Remaining issues were identified from log review.
Habit digest check has a config-override logic bug where habits are silently suppressed.

**Rule for all fixes:** Any error that reaches an except block must be sent via
`send_system_log(bot, ...)` (from `second_brain.error_reporting`), not via `message.reply_text`
or any other path that reaches the main chat.

---

## Fix 1 — Sleep sync: stage minutes double-conversion

**File: `second_brain/healthtrack/sleep.py`**, function `_stage_minutes` (~line 142)

When a dict's nested `stages` key holds a list, the code multiplies the list-branch result
(already in minutes) by 60 000 to add it to the ms accumulator — which is correct — but when
that **same** list contains dicts without a duration key, `_duration_ms(item)` falls through to
return `0.0` instead of summing the stage duration that is stored under `item["durationMs"]` or
similar. The list branch must extract the duration from the right field.

```python
# Before (~line 157)
    if isinstance(stages_summary, list):
        total_ms = 0.0
        for item in stages_summary:
            if not isinstance(item, dict):
                continue
            name = _normalise_stage_name(item.get("stage") or item.get("type") or item.get("name"))
            if name in stage_names:
                total_ms += _duration_ms(item)
        return round(total_ms / 60000, 2)

# After
    if isinstance(stages_summary, list):
        total_ms = 0.0
        for item in stages_summary:
            if not isinstance(item, dict):
                continue
            name = _normalise_stage_name(item.get("stage") or item.get("type") or item.get("name"))
            if name in stage_names:
                # Prefer explicit ms keys; fall back to generic _duration_ms
                dur = (
                    item.get("durationMs")
                    or item.get("totalDurationMs")
                    or item.get("durationMillis")
                )
                total_ms += float(dur) if dur is not None else _duration_ms(item)
        return round(total_ms / 60000, 2)
```

---

## Fix 2 — Sleep sync: unhandled ValueError from _parse_dt propagates silently

**File: `second_brain/healthtrack/sleep.py`**, function `parse_sleep_data_point` (~line 170)

If `startTime` or `endTime` is empty after normalisation, `_parse_dt` raises `ValueError` with a
generic message. The exception bubbles out of `handle_sleep_sync` without the target date in the
message, making triage from logs hard. Wrap in `handle_sleep_sync` and re-raise with context.

```python
# In handle_sleep_sync, after:   parsed = parse_sleep_data_point(point, tz)
# Before
    parsed = parse_sleep_data_point(point, tz)

# After
    try:
        parsed = parse_sleep_data_point(point, tz)
    except (ValueError, KeyError) as exc:
        raise ValueError(f"sleep_sync: failed to parse data point for {target_day}: {exc}") from exc
```

---

## Fix 3 — Sleep sync: backfill does not report per-date errors to system log

**File: `second_brain/healthtrack/sleep.py`**, function `handle_sleep_backfill_job` (~line 319)

A single bad date causes the whole backfill to abort. Catch per-date errors, record them in
results, and send a summary via `send_system_log` at the end.

```python
# Before
    while cursor <= end_day:
        results[cursor.isoformat()] = await handle_sleep_sync(
            notion=notion,
            metrics_db_id=NOTION_HEALTH_METRICS_DB,
            client_id=GOOGLE_HEALTH_CLIENT_ID,
            client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
            refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
            target_date=cursor,
            tz=TZ,
        )
        cursor += timedelta(days=1)
        if cursor <= end_day:
            await asyncio.sleep(0.35)

    return {"ok": True, "results": results}

# After
    errors: list[str] = []
    while cursor <= end_day:
        try:
            results[cursor.isoformat()] = await handle_sleep_sync(
                notion=notion,
                metrics_db_id=NOTION_HEALTH_METRICS_DB,
                client_id=GOOGLE_HEALTH_CLIENT_ID,
                client_secret=GOOGLE_HEALTH_CLIENT_SECRET,
                refresh_token=GOOGLE_HEALTH_REFRESH_TOKEN,
                target_date=cursor,
                tz=TZ,
            )
        except Exception as exc:
            log.exception("sleep_backfill: failed for %s", cursor)
            results[cursor.isoformat()] = {"action": "error", "error": str(exc)}
            errors.append(f"{cursor}: {type(exc).__name__}: {exc}")
        cursor += timedelta(days=1)
        if cursor <= end_day:
            await asyncio.sleep(0.35)

    if errors and bot is not None:
        from second_brain.error_reporting import send_system_log
        await send_system_log(bot, f"⚠️ Sleep backfill partial failure ({len(errors)} date(s)):\n" + "\n".join(errors))

    return {"ok": not errors, "results": results, "errors": errors}
```

Note: the `del bot` at the top of the function must be removed for this to work.

---

## Fix 4 — Habit digest check: config silently overrides caller's include_habits=True

**File: `second_brain/digest.py`**, function `send_daily_digest` (~line 451)

```python
# Current logic (line 451-453)
    habits_enabled = include_habits
    if config and config.get("include_habits") is not None:
        habits_enabled = bool(config.get("include_habits"))
```

The problem: if a caller explicitly passes `include_habits=True` (e.g., a manual digest trigger),
the config `include_habits: False` silently wins and no habits appear. The config should only
restrict, never override an explicit `True` from the caller when the caller is a user-triggered
action. Fix by only letting config turn habits **off** when the caller hasn't forced them on:

```python
# After
    habits_enabled = include_habits
    if not include_habits and config and config.get("include_habits") is not None:
        habits_enabled = bool(config.get("include_habits"))
```

This preserves the config gate for scheduled digests (which pass `include_habits` from the slot
config, not forced True) while letting a forced `include_habits=True` always win.

---

## Fix 5 — Habit digest check: missing habits because already_logged_today uses stale cache

**File: `second_brain/digest.py`**, function `send_daily_digest` (~line 462)

The lambda `already_logged_today=lambda pid: notion_habits.already_logged_today(_notion, NOTION_LOG_DB, pid, TZ)`
makes a fresh Notion API call per habit. If `_notion` is `None` (not yet initialised), this
raises `AttributeError` inside a list comprehension, which silently produces an empty habits
list with no error logged.

```python
# Before
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

# After
        if _notion is None:
            log.warning("send_daily_digest: _notion not initialised, skipping habits")
            habits = []
        else:
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
```

---

## Fix 6 — Error messages: all sync error paths must use send_system_log

Audit every `except` block in the following files and ensure any user-visible error uses
`send_system_log` (from `second_brain.error_reporting`), not `message.reply_text` or
`bot.send_message(chat_id=MY_CHAT_ID, ...)`:

- `second_brain/healthtrack/sleep.py`
- `second_brain/healthtrack/routes.py` (sleep backfill route at ~line 1028)
- `second_brain/digest.py` (`send_daily_digest` and `send_digest_for_slot`)

Pattern to follow (already used correctly in `handle_sleep_sync_job`):

```python
except Exception as exc:
    log.exception("descriptive context: job failed")
    if bot is not None:
        await send_system_log(bot, f"🚨 Descriptive context failed: {type(exc).__name__}: {exc}")
    raise  # re-raise so scheduler marks job as failed
```

Do NOT surface raw exception text to the main chat. If the function has no `bot` reference,
log the error and let it propagate — never send to `MY_CHAT_ID`.

---

## Verification

1. Trigger sleep sync backfill via `POST /api/v1/sleep-sync/backfill` with a date range that
   includes one date with no data — confirm partial results returned and system log receives the
   per-date error summary, not the main chat.
2. Trigger a manual digest (`/digest`) when `include_habits=True` — confirm habits appear even
   if the slot config has `include_habits: false`.
3. Temporarily set `_notion = None` before `send_daily_digest` in a test and confirm a warning
   is logged rather than a silent empty habits list.
4. Run `pytest tests/test_health_sleep.py` — all tests should pass.
