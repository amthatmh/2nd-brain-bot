# Fix: Sleep sync — reconciled endpoint response format

## What we know from live logs

The reconciled endpoint:
```
GET /v4/users/me/dataTypes/sleep/dataPoints:reconcile
    ?filter=sleep.interval.civil_end_time >= "YYYY-MM-DD" AND ...
```
Returns 200 OK but the dataPoint has this shape (confirmed from error log):
```
point keys = ['startTime', 'endTime', 'sleepSummary', 'stagesSummary']
```
- `startTime` / `endTime` are **empty strings** at the top level
- `sleepSummary` — dict, contents unknown (need debug log below to confirm)
- `stagesSummary` — based on Google Health API v4 docs, this is a list:
  `[{"type": "DEEP", "minutes": "114", "count": "10"}, ...]`
  where **`minutes` is a plain-English string in minutes, NOT milliseconds**

The official docs for the list endpoint show times inside `sleep.interval.startTime`.
The reconciled endpoint flattens this differently — times are likely inside `sleepSummary`.

---

## Step 1 — Add targeted debug logging FIRST (deploy this alone)

**File: `second_brain/healthtrack/sleep.py`**, function `fetch_sleep_data`, after line 74

```python
# Before (lines 73-74)
    log.info("sleep_sync: raw dataPoint keys=%s", list(first.keys()))
    log.info("sleep_sync: raw dataPoint=%s", first)

# After
    log.info("sleep_sync: raw dataPoint keys=%s", list(first.keys()))
    sleep_summary_debug = first.get("sleepSummary") or {}
    stages_debug = first.get("stagesSummary") or []
    log.info(
        "sleep_sync: sleepSummary keys=%s sleepSummary=%s",
        list(sleep_summary_debug.keys()) if isinstance(sleep_summary_debug, dict) else type(sleep_summary_debug).__name__,
        sleep_summary_debug,
    )
    log.info(
        "sleep_sync: stagesSummary type=%s first_item=%s",
        type(stages_debug).__name__,
        stages_debug[0] if stages_debug else "empty",
    )
    log.info("sleep_sync: raw dataPoint=%s", first)
```

Deploy, re-run the backfill for one date, share the new log output.
The `sleepSummary=` line will confirm where the actual times are stored.

---

## Step 2 — Fix parsing once sleepSummary structure is confirmed

Apply these fixes together after Step 1 confirms the field names.

### Fix A — Time extraction: fall back to sleepSummary

**File: `second_brain/healthtrack/sleep.py`**, function `fetch_sleep_data` (~line 79)

```python
# Before
    interval = first.get("interval") or {}

    def _pick_time(top_key: str, interval_key: str, interval_civil_key: str, top_civil_key: str) -> str:
        return (
            first.get(top_key)
            or interval.get(interval_key)
            or interval.get(interval_civil_key)
            or first.get(top_civil_key)
            or ""
        )

# After
    interval = first.get("interval") or {}
    sleep_summary_raw = first.get("sleepSummary") or {}

    def _pick_time(top_key: str, interval_key: str, interval_civil_key: str, top_civil_key: str) -> str:
        return (
            first.get(top_key)
            or interval.get(interval_key)
            or interval.get(interval_civil_key)
            or first.get(top_civil_key)
            or sleep_summary_raw.get(top_key)
            or sleep_summary_raw.get(top_civil_key)
            or (sleep_summary_raw.get("interval") or {}).get(interval_key)
            or (sleep_summary_raw.get("interval") or {}).get(interval_civil_key)
            or ""
        )
```

### Fix B — Stage minutes: parse as minutes (string), not milliseconds

**File: `second_brain/healthtrack/sleep.py`**, function `_stage_minutes` (~line 157)

The API returns `{"type": "DEEP", "minutes": "114", "count": "10"}` — minutes already in minutes as a string.
Current code calls `_duration_ms(item)` which looks for `durationMs`/`totalDurationMs` and returns 0.

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
        total_min = 0.0
        for item in stages_summary:
            if not isinstance(item, dict):
                continue
            name = _normalise_stage_name(item.get("stage") or item.get("type") or item.get("name"))
            if name in stage_names:
                # API sends minutes as a plain string; fall back to ms conversion for other formats
                minutes_raw = item.get("minutes")
                if minutes_raw is not None:
                    try:
                        total_min += float(minutes_raw)
                    except (ValueError, TypeError):
                        total_min += _duration_ms(item) / 60000
                else:
                    total_min += _duration_ms(item) / 60000
        return round(total_min, 2)
```

### Fix C — Total sleep: use minutesAsleep / minutesInSleepPeriod

**File: `second_brain/healthtrack/sleep.py`**, function `parse_sleep_data_point` (~line 180)

```python
# Before
    total_sleep_min = round(_duration_ms(sleep_summary.get("totalDurationMs")) / 60000, 2)

# After
    def _minutes_from_summary(key: str) -> float:
        val = sleep_summary.get(key)
        if val is None:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return _duration_ms(val) / 60000

    total_sleep_min = round(
        _minutes_from_summary("minutesAsleep")
        or _minutes_from_summary("minutesInSleepPeriod")
        or _duration_ms(sleep_summary.get("totalDurationMs")) / 60000,
        2,
    )
```

---

## Fix 3 — Error routing: suppress errors from main bot

**File: `second_brain/error_reporting.py`**, function `send_system_log`

`SYSTEM_LOGS_CHAT_ID` defaults to `MY_CHAT_ID` when the env var is unset (config.py line 37).
This causes all sync errors to appear in the main bot.

```python
# Before
async def send_system_log(bot, text: str) -> None:
    from second_brain.config import SYSTEM_LOGS_CHAT_ID
    if bot is None:
        log.error("System log bot unavailable: %s", text)
        return
    try:
        await bot.send_message(chat_id=SYSTEM_LOGS_CHAT_ID, text=text)
    except Exception as exc:
        log.error("Failed to send system log: %s", exc)

# After
async def send_system_log(bot, text: str) -> None:
    from second_brain.config import MY_CHAT_ID, SYSTEM_LOGS_CHAT_ID
    if bot is None:
        log.error("System log bot unavailable: %s", text)
        return
    if not SYSTEM_LOGS_CHAT_ID or str(SYSTEM_LOGS_CHAT_ID) == str(MY_CHAT_ID):
        log.warning("system_log (SYSTEM_LOGS_CHAT_ID not set separately): %s", text)
        return
    try:
        await bot.send_message(chat_id=SYSTEM_LOGS_CHAT_ID, text=text)
    except Exception as exc:
        log.error("Failed to send system log: %s", exc)
```

**Railway action required:** Create a separate Telegram group for system logs, get its chat ID,
add `SYSTEM_LOGS_CHAT_ID=<id>` in Railway env vars. Until then the guard above silences errors
from the main bot while still logging to Railway.

---

## Execution order

1. Deploy Step 1 (debug logging only) → run backfill for one date → share `sleepSummary=` log line
2. Confirm field names, then deploy Step 2 fixes A + B + C together
3. Deploy Fix 3 (error routing) independently — safe to ship at any time
4. Set `SYSTEM_LOGS_CHAT_ID` in Railway to a dedicated channel

## Verification

- Backfill for a known sleep date → Notion row shows non-zero total sleep, stage breakdown, efficiency
- Backfill for a date with no data → returns `{"action": "no_data"}`, no crash
- Run `pytest tests/test_health_sleep.py`
