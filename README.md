# Second Brain Bot

A personal Telegram bot that acts as a command-line interface to your life — tasks, habits, notes, entertainment, travel, workouts, and daily digests, all backed by Notion.

---

## Overview

Second Brain Bot bridges Telegram and Notion to keep your personal operating system in your pocket. Send a plain-text message and the bot classifies your intent (via Claude AI), then routes it to the right Notion database. Scheduled digests surface what needs attention before you think to ask.

**Stack:** Python 3.13 · python-telegram-bot 21.11.1 · APScheduler 3 · Notion API · Anthropic Claude · aiohttp · OpenWeather

---

## Features

### Tasks
- Capture tasks by free-text message; Claude classifies intent and creates Notion entries
- Horizon labeling: Today / This Week / This Month / Backburner
- Recurring tasks (daily, weekly, monthly, quarterly) auto-spawn on schedule
- `/done` — fuzzy-match picker to mark tasks complete
- `/remind` / `/r` — show open tasks grouped by horizon

### Habits
- Inline keyboard to log any active habit in one tap
- Weekly frequency targets with completion tracking
- `Show After` time gates — habits only surface in digests after a configured time
- Weekly streak recording every Monday

### Digest
- Notion-driven schedule: configure digest times and contexts in a Notion selector DB
- Each slot independently configures: time, weekday/weekend, contexts (Personal/Work/Health/HK), max items, weather, UV index, feel prompt
- Habit list appended when slot is configured to include habits
- Upcoming trip reminders injected automatically
- `/sync` — trigger a digest manually

### Notes
- `/notes` — topic picker (Acoustics, Work, Personal, Health, LEED, WELL, Ideas, Research)
- Free-text capture saved to Notion Notes DB with topic tag

### Entertainment Logging (`/log`)
- Cinema: movie/show title, date, venue, rating — TMDB lookup for metadata
- Performances: concerts, theatre, etc.
- Sports: matches attended
- AI-parsed from natural language; disambiguation prompts for ambiguous inputs

### Work Trips (`/trip`)
- Log trip destination, dates, purpose, field work type
- Packing list generation based on trip type
- 5-day weather forecast fetched 3 days before departure
- Weather-triggered packing additions (rain gear, warm layers, etc.)
- Reminder injected into morning digest before departure

### CrossFit / Fitness
- WOD capture and logging to Notion
- Weekly program parsing
- Strength flow with set/rep tracking
- Daily readiness check-in
- Movement library lookup

### Health Tracking
- Step count ingestion via webhook (Apple Health / Shortcuts)
- Automatic habit log creation when daily step goal is met
- Steps dashboard served over HTTP
- Health metrics DB integration

### Weather (`/weather`)
- Current conditions + multi-day forecast snapshot
- UV index alerting (threshold configurable)
- Location override via `/location`

### Asana Sync
- Bidirectional sync between a Notion tasks DB and an Asana project
- Configurable interval; optional orphan archiving
- Smoke-test on startup

### Cinema Sync
- Pulls TMDB watch history into a Notion Cinema Log DB
- Scheduled periodic sync via Utility Scheduler

### Watchlist & Wants List
- Track movies/shows to watch, items to buy
- Inline keyboard browsing

### Mute
- `/mute [duration]` — suppress scheduled digests temporarily
- `/unmute` — resume immediately

### Rules Engine
- Configurable post-save rules triggered on entertainment log entries
- Extensible for custom automation

### Admin
- Operational alert channel with thread support
- Boot log written to Notion on every startup (version, SHA, feature flags, Asana status)
- `/syncstatus` — show last sync timestamps per subsystem

---

## Architecture

```
Telegram Update
      │
      ▼
  main.py  ◄── handler registration, scheduler init, HTTP server
      │
      ├── routers.py          # callback query dispatch, text classification
      ├── ai/classify.py      # Claude intent classification
      ├── digest.py           # digest schedule build + send logic
      ├── notion/             # thin Notion API wrappers (DI, no globals)
      │   ├── tasks.py
      │   ├── habits.py
      │   ├── notes.py
      │   └── daily_log.py
      ├── entertainment/      # cinema / sport / performance handlers
      ├── crossfit/           # workout classification + logging
      ├── healthtrack/        # steps webhook + metrics
      ├── trips.py            # trip logging + weather scheduling
      ├── asana/sync.py       # Asana ↔ Notion reconciliation
      └── cinema/sync.py      # TMDB → Notion cinema log
```

**Key design rules:**
- `notion/` helpers accept `notion` and `db_id` as parameters — no module-level globals
- Business logic lives in domain modules; `main.py` contains only handler wiring and scheduler setup
- `config.py` is the single source of truth for all env-var-backed constants
- `state.py` (`BotState` dataclass) owns all mutable runtime state
- `ExpiringDict` (TTL-backed dict) used for habit cache and pending interaction maps
- All scheduled jobs run with `max_instances=1` to prevent overlap

**Scheduler jobs (APScheduler/AsyncIO):**

| Job | Schedule | Description |
|-----|----------|-------------|
| Digest slots | Notion-configured cron | Send task/habit digest per slot |
| Digest schedule refresh | Every N min (default 10) | Reload slot config + habit cache from Notion |
| Recurring task generator | Every 5 min | Spawn recurring Notion task instances |
| Pending interaction cleanup | Every 5 min | Expire stale task confirmations |
| Batch confirmation cleanup | Every 60 sec | Expire pending batch captures |
| Weekly streak recorder | Monday morning | Record habit goal met/missed to streak DB |
| Trip weather refresh | 3 days pre-departure | Fetch and cache forecast per trip |
| Cinema sync | Utility-Scheduler-driven | Sync TMDB watch history |
| Utility Scheduler | Every 10 min | Load and apply Notion-driven job configs |

---

## Module Reference

| Path | Purpose |
|------|---------|
| `second_brain/main.py` | Entry point — handler registration, scheduler init, HTTP server |
| `second_brain/config.py` | All env-var constants; single source of truth |
| `second_brain/state.py` | `BotState` dataclass — all mutable runtime state |
| `second_brain/routers.py` | Callback query dispatch and text classification routing |
| `second_brain/digest.py` | Digest schedule, slot logic, habit pending list |
| `second_brain/boot.py` | Startup boot log, `git_sha()` helper |
| `second_brain/formatters.py` | Telegram message formatters |
| `second_brain/keyboards.py` | Inline keyboard builders |
| `second_brain/utils.py` | `ExpiringDict`, `local_today`, `fuzzy_match`, `parse_time_to_minutes` |
| `second_brain/mute.py` | Mute/unmute state persistence |
| `second_brain/trips.py` | Trip logging, packing lists, weather scheduling |
| `second_brain/weather.py` | OpenWeather API wrapper |
| `second_brain/watchlist.py` | Watchlist / wants-list handlers |
| `second_brain/palette.py` | Task color palette display |
| `second_brain/rules/` | Post-save rules engine |
| `second_brain/ai/` | Claude classify (`classify.py`) + client singleton (`client.py`) |
| `second_brain/notion/` | Notion API wrappers: `tasks`, `habits`, `notes`, `daily_log`, `properties` |
| `second_brain/notes/` | Notes capture flow |
| `second_brain/entertainment/` | Cinema / sport / performance log handlers |
| `second_brain/crossfit/` | CrossFit WOD classification, logging, weekly program |
| `second_brain/healthtrack/` | Steps webhook, metrics, dashboard, scheduler |
| `second_brain/asana/` | Asana ↔ Notion sync |
| `second_brain/cinema/` | TMDB → Notion cinema log sync |
| `second_brain/monitoring/` | Job execution tracking, weekly metrics, health checks |
| `second_brain/handlers/` | Command handlers, admin commands |
| `second_brain/services/` | Task parsing, note utilities |

---

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `TELEGRAM_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Your personal chat ID (bot only responds to this) |
| `ANTHROPIC_API_KEY` | Claude API key |
| `NOTION_TOKEN` | Notion integration token |
| `NOTION_DB_ID` | Tasks / To-Do database |
| `NOTION_HABIT_DB` | Habits definition database |
| `NOTION_LOG_DB` | Habit completion log database |
| `NOTION_STREAK_DB` | Weekly habit streak database |
| `NOTION_NOTES_DB` | Notes database |
| `NOTION_DIGEST_SELECTOR_DB` | Digest schedule configuration database |

### Optional — Core

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMEZONE` | `America/Chicago` | IANA timezone string |
| `RECURRING_CHECK_TIME` | `7:00` | HH:MM for recurring task spawn job |
| `UTILITY_SCHEDULER_RELOAD_MINUTES` | `10` | How often to reload Notion-driven job configs |
| `APP_VERSION` | `v14.0.0` | Displayed in boot log |
| `CLAUDE_MODEL` | `claude-sonnet-4-6` | Anthropic model for classification |
| `CLAUDE_MAX_TOKENS` | `200` | Token cap for classification responses |
| `CLAUDE_PARSE_MAX_TOKENS` | `4000` | Token cap for parsing responses |
| `ALERT_CHANNEL_ID` | — | Telegram channel ID for operational alerts |
| `TELEGRAM_ALERT_THREAD_ID` | — | Thread ID within alert channel |
| `NOTION_BOOT_LOG_DB` | — | Notion DB for boot log entries |
| `ENV_DB_ID` | — | Notion DB for runtime environment config |

### Optional — Features

| Variable | Description |
|----------|-------------|
| `OPENWEATHER_KEY` | Enables weather commands and trip forecasts |
| `WEATHER_LOCATION` | Default location (e.g. `Chicago,IL`). Default: `Chicago,IL` |
| `UV_THRESHOLD` | UV index alert threshold. Default: `3` |
| `NOTION_DAILY_LOG_DB` | Daily log / signoff database |
| `NOTION_TRIPS_DB` | Work trips database |
| `NOTION_PACKING_ITEMS_DB` | Packing list items database |
| `NOTION_CINEMA_LOG_DB` | Cinema log database |
| `NOTION_PERFORMANCE_LOG_DB` | Performances log database |
| `NOTION_SPORTS_LOG_DB` | Sports attendance log database |
| `NOTION_FAVE_DB` | Favourites database |
| `NOTION_HEALTH_METRICS_DB` | Health metrics database |
| `NOTION_WATCHLIST_DB` | Watchlist database |
| `NOTION_WANTSLIST_V2_DB` | Wants list database |
| `NOTION_PHOTO_DB` | Photo log database |
| `NOTION_UTILITY_SCHEDULER_DB` | Notion-driven utility job scheduler database |
| `TMDB_API_KEY` | TMDB API key for cinema metadata lookup |
| `ASANA_PAT` | Asana personal access token — enables Asana sync |
| `ASANA_PROJECT_GID` | Asana project GID |
| `ASANA_WORKSPACE_GID` | Asana workspace GID |
| `ASANA_SYNC_INTERVAL` | Asana sync interval in minutes. Default: `60` |
| `ASANA_SYNC_SOURCE` | `project` or `workspace`. Default: `project` |
| `ASANA_ARCHIVE_ORPHANS` | Archive Notion tasks not in Asana. Default: `0` |
| `NOTION_MOVEMENTS_DB` | CrossFit movements library |
| `NOTION_WORKOUT_PROGRAM_DB` | Weekly workout programs |
| `NOTION_WORKOUT_DAYS_DB` | Workout days database |
| `NOTION_WORKOUT_LOG_DB` | Workout log database |
| `NOTION_WOD_LOG_DB` | WOD log database |
| `NOTION_PROGRESSIONS_DB` | Movement progressions database |
| `NOTION_DAILY_READINESS_DB` | Daily readiness check-in database |
| `NOTION_CYCLES_DB` | Training cycles database |
| `HEALTH_STEPS_THRESHOLD` | Daily step goal (enables auto habit logging) |
| `HEALTH_HABIT_NAME` | Notion habit name for step goal. Default: `Steps` |
| `PORT` | HTTP server port. Default: `8080` |
| `WEEKS_HISTORY` | Weeks of history for metrics. Default: `52` |
| `FEATURE_RECURRING` | Enable recurring task spawn. Default: `1` |

---

## Commands

| Command | Description |
|---------|-------------|
| `/done` | Mark a task or habit done (fuzzy picker) |
| `/remind` `/r` | Show open tasks grouped by horizon |
| `/habits` | Show pending habits with one-tap logging |
| `/notes` | Open note capture with topic picker |
| `/weather` | Current weather + forecast snapshot |
| `/location` | Set weather location |
| `/log` | Log cinema, performance, or sport entry |
| `/trip` | Log a work trip |
| `/sync` | Send a digest now |
| `/syncstatus` | Show sync status per subsystem |
| `/mute [duration]` | Pause scheduled digests |
| `/unmute` | Resume scheduled digests |

Free-text messages are classified by Claude and routed automatically (task capture, habit log, note, or CrossFit entry).

---

## Development

**Setup:**
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in required vars
python main.py
```

**Tests:**
```bash
pytest tests/ -v --tb=short
```

CI runs on Python 3.13 only (`runtime.txt` and `.python-version` match).

**Adding a new Notion-backed feature** — see `CONTRIBUTING.md` for the full protocol. Short version:
1. Add env var in `config.py`
2. Add Notion helpers in `notion/<feature>.py` (DI — no module globals)
3. Add keyboards/formatters if needed
4. Wire handler in `main.py`

The canonical reference flow is Notes: `notion/notes.py` → `notes/flow.py` → `ai/classify.py` → `main.py`.
