# Fix: Sleep sync parse failure + error message routing

## Context

Live test via backfill endpoint revealed two bugs:

1. Google Health API returns a data point where top-level `startTime`/`endTime` are empty strings;
   the real times are inside the `sleepSummary` sub-object. The current `_pick_time` helper never
   checks there, so `_parse_dt` raises ValueError.

2. `send_system_log` correctly routes to `SYSTEM_LOGS_CHAT_ID`, but that env var is currently set
   to the same chat as `MY_CHAT_ID`, so errors land in the main bot. This is a **Railway env var
   change**, not a code change — but the code should also guard against double-posting.

---

## Fix 1 — Sleep parse: fall back to sleepSummary for start/end times

**File: `second_brain/healthtrack/sleep.py`**, function `fetch_sleep_data` (~line 76)

The data point returned by the API has this shape:

```json
{
  "startTime": "",
  "endTime": "",
  "sleepSummary": {
    "startTime": "2026-05-29T22:00:00-05:00",
    "endTime": "2026-05-30T06:15:00-05:00",
    "totalDurationMs": 29700000
  },
  "stagesSummary": { ... }
}
```

Extend `_pick_time` to also check `sleepSummary` before giving up:

```python
# Before (~line 81)
    def _pick_time(top_key: str, interval_key: str, interval_civil_key: str, top_civil_key: str) -> str:
        return (
            first.get(top_key)
            or interval.get(interval_key)
            or interval.get(interval_civil_key)
            or first.get(top_civil_key)
            or ""
        )

# After
    sleep_summary_raw = first.get("sleepSummary") or {}

    def _pick_time(top_key: str, interval_key: str, interval_civil_key: str, top_civil_key: str) -> str:
        return (
            first.get(top_key)
            or interval.get(interval_key)
            or interval.get(interval_civil_key)
            or first.get(top_civil_key)
            or sleep_summary_raw.get(top_key)
            or sleep_summary_raw.get(top_civil_key)
            or ""
        )
```

Also update the log line that reports the raw dataPoint to include `sleepSummary` keys for
easier future debugging:

```python
# Before (~line 73)
    log.info("sleep_sync: raw dataPoint keys=%s", list(first.keys()))
    log.info("sleep_sync: raw dataPoint=%s", first)

# After
    log.info("sleep_sync: raw dataPoint keys=%s sleepSummary keys=%s",
             list(first.keys()), list((first.get("sleepSummary") or {}).keys()))
    log.info("sleep_sync: raw dataPoint=%s", first)
```

---

## Fix 2 — Error routing: guard against SYSTEM_LOGS_CHAT_ID == MY_CHAT_ID

**File: `second_brain/error_reporting.py`**, function `send_system_log`

Add a guard so that if the two chat IDs are the same, the message is only logged (not sent to
the chat), preventing error spam in the main bot:

```python
# Before
async def send_system_log(bot, text: str) -> None:
    """Send an internal error report to the configured system logs channel."""
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
    """Send an internal error report to the configured system logs channel."""
    from second_brain.config import MY_CHAT_ID, SYSTEM_LOGS_CHAT_ID

    if bot is None:
        log.error("System log bot unavailable: %s", text)
        return
    if not SYSTEM_LOGS_CHAT_ID or str(SYSTEM_LOGS_CHAT_ID) == str(MY_CHAT_ID):
        log.warning("System log (SYSTEM_LOGS_CHAT_ID not configured separately): %s", text)
        return
    try:
        await bot.send_message(chat_id=SYSTEM_LOGS_CHAT_ID, text=text)
    except Exception as exc:
        log.error("Failed to send system log: %s", exc)
```

**Railway env var action required (separate step):**
Create a new Telegram group/channel for system logs, get its chat ID, then in Railway set:
```
SYSTEM_LOGS_CHAT_ID=<new_channel_chat_id>
```
Until that is done, the guard above will suppress error spam from the main bot while still
logging errors to Railway logs.

---

## Verification

1. Trigger `POST /api/v1/sleep-sync/backfill` with today's date.
2. Confirm the sync succeeds and a row is created/updated in Notion.
3. Trigger with a date that has no data — confirm `{"action": "no_data"}` result and no crash.
4. Confirm error messages no longer appear in the main bot chat.
5. Run `pytest tests/test_health_sleep.py`.
