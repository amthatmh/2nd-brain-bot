# Railway `worker` production env var audit (2026-04-27)

Scope: searched entire repo (`rg -n --hidden --glob '!.git'`) for exact names and truncated prefixes provided.

## Classification report

| Variable (Railway) | Classification | Evidence (file:line) | What it appears to control | Confidence |
|---|---|---|---|---|
| `ANTHROPIC_API_KEY` | Used in runtime code | `second_brain/main.py:137`; `second_brain/config.py:32` (tests also set it) | Anthropic client auth for Claude features. | High |
| `ASANA_PAT` | Used in runtime code | `second_brain/main.py:168,5034,5045,5422,5444`; `asana_sync.py:598` | Asana API token for sync jobs and startup checks. | High |
| `ASANA_PROJECT_GID` | Used in runtime code | `second_brain/main.py:169,5046,5296,5445`; `asana_sync.py:602,707` | Project identifier when `ASANA_SYNC_SOURCE=project`. | High |
| `ASANA_SYNC_EVERY_SECONDS` (`ASANA_SYNC_EVERY_S...`) | Used in runtime code | `second_brain/main.py:172` | Poll/sync interval for Asana loop. | High |
| `ASANA_SYNC_SOURCE` | Used in runtime code | `second_brain/main.py:171,5048,5295,5425,5447,5486`; `asana_sync.py:600,705` | Sync mode selector (`project` vs `my_tasks`). | High |
| `ASANA_WORKSPACE_GID` | Used in runtime code | `second_brain/main.py:170,5047,5297,5425,5446`; `asana_sync.py:604,709` | Workspace identifier required for `my_tasks` mode. | High |
| `CINEMA_SYNC_ENABLED` | Used only in docs/tests/examples | `cinema/README.md:16` | Mentioned as a config idea; no runtime reads found. | High |
| `CLAUDE_MAX_TOKENS` | Used in runtime code | `second_brain/main.py:158`; `second_brain/config.py:46` | Claude output token cap. | High |
| `CLAUDE_MODEL` | Used in runtime code | `second_brain/main.py:157,491,1097,1211,1303,1403`; `second_brain/config.py:45` | Claude model selection. | High |
| `DIGEST_TIME_WEEKDAY` | Used in runtime code | `second_brain/main.py:153`; `second_brain/config.py:41` | Weekday digest schedule time. | High |
| `DIGEST_TIME_WEEKEND` | Used in runtime code | `second_brain/main.py:154`; `second_brain/config.py:42` | Weekend digest schedule time. | High |
| `GIT_SHA` | Used in runtime code | `second_brain/main.py:5276` | Build/deploy receipt version metadata fallback. | High |
| `NOTION_CINEMA_DB` | Used in runtime code | `second_brain/main.py:142` (fallback); `cinema/config.py:5`; `cinema/config.py:16` | Cinema Notion DB id; also fallback for `NOTION_CINEMA_LOG_DB`. | High |
| `NOTION_CINEMA_LOG_DB` (`NOTION_CINEMA_LOG_...`) | Used in runtime code | `second_brain/main.py:142,3009,3045,3104,3150,3262,3270,5321,5348` | Entertainment/cinema log DB used for reads+writes. | High |
| `NOTION_DB_ID` | Used in runtime code | `second_brain/main.py:139,1869,1925,2332,2577,5044,5316,5423,5443`; `second_brain/config.py:34` | Primary Notion tasks/items DB. | High |
| `NOTION_DIGEST_SELECTOR_DB` (`NOTION_DIGEST_SELE...`) | Used in runtime code | `second_brain/main.py:150,914,5320`; `second_brain/config.py:38` | Digest selection database. | High |
| `NOTION_HABIT_DB` | Used in runtime code | `second_brain/main.py:140,727,770,5317`; `second_brain/config.py:35` | Habit tracking DB. | High |
| `NOTION_LOG_DB` | Used in runtime code | `second_brain/main.py:141,1762,1777,1797,1828,5118,5318`; `second_brain/config.py:36` | Logging/history DB. | High |
| `NOTION_NOTES_DB` | Used in runtime code | `second_brain/main.py:149,296,299,1220,1336,2690,2746,4110,4320,4617,5319,5682`; `second_brain/config.py:37` | Notes feature DB. | High |
| `NOTION_PERFORMANCE...` (legacy names) | Referenced but likely obsolete | `second_brain/main.py:145` (`NOTION_PERFORMANCE_DB` + `NOTION_PERFORMANCE_LOG_DB` fallback only); `tests/test_entertainment_logging.py:385,391` | Backward-compatible aliases for renamed performances DB var. | Medium-High |
| `NOTION_PHOTO_DB` | Used in runtime code | `second_brain/main.py:177,1111,1581,1678,5304` | Photo bucket-list DB. | High |
| `NOTION_PLACES_DB` | No references found | No matches in repository. | Likely old/unused planned feature. | High |
| `NOTION_SPORTS_LOG_DB` (`NOTION_SPORTS_LOG_...`) | Used in runtime code | `second_brain/main.py:147,3184,3209,3264,3274,5323,5350` | Sports entertainment log DB. | High |
| `NOTION_TOKEN` | Used in runtime code | `second_brain/main.py:138,186`; `second_brain/config.py:33` | Notion API authentication. | High |
| `NOTION_WANTSLIST_V2_DB` (`NOTION_WANTSLIST_V...`) | Used in runtime code | `second_brain/main.py:176,1110,1556,1663,5303` | Wantslist DB id. | High |
| `NOTION_WATCHLIST_DB` | Used in runtime code | `second_brain/main.py:175,1109,1522,1528,1617,5302,5325` | Watchlist DB id. | High |
| `OPENWEATHER_KEY` | Used in runtime code | `second_brain/main.py:163,380,385,536,541,562,580,594,674,711,5307,5373` | OpenWeather API access for weather enrichment. | High |
| `RECURRING_CHECK_TIME` (`RECURRING_CHECK_TI...`) | Used in runtime code | `second_brain/main.py:155`; `second_brain/config.py:43` | Recurring-check scheduler time. | High |
| `TMDB_API_KEY` | Used in runtime code | `second_brain/main.py:60,1424,1430,1453,5079,5305`; `cinema/config.py:7` | TMDB metadata enrichment for cinema/watchlist. | High |
| `TELEGRAM_CHAT_ID` | Used in runtime code | `second_brain/main.py:134`; `second_brain/config.py:29` | Primary Telegram destination chat id. | High |
| `TELEGRAM_TOKEN` | Used in runtime code | `second_brain/main.py:133,5725`; `second_brain/config.py:28` | Telegram bot authentication token. | High |
| `TIMEZONE` | Used in runtime code | `second_brain/main.py:152,2812`; `second_brain/config.py:40` | Timezone for scheduling/date payloads. | High |
| `WEEKS_HISTORY` | Used in runtime code | `second_brain/main.py:160,5114,5160` | Rolling history window for review/report logic. | High |
| `NOTION_FAVE_DB` | Used in runtime code | `cinema/config.py:6`; `second_brain/main.py:59,5078` | Favourite films DB used by cinema sync module. | High |

## Final shortlist: probably safe to remove from Railway (worker/production)

1. `NOTION_PLACES_DB` — no repository references.
2. `CINEMA_SYNC_ENABLED` — only appears in docs; runtime currently hardcodes cinema sync as enabled.
3. `NOTION_PERFORMANCE_DB` / `NOTION_PERFORMANCE_LOG_DB` — only legacy fallback aliases; remove **only after** confirming `NOTION_PERFORMANCES_DB` is set in production and no older deployments rely on aliases.

## Notes on truncated names from Railway UI

- `ASANA_SYNC_EVERY_S...` resolved to `ASANA_SYNC_EVERY_SECONDS`.
- `NOTION_CINEMA_LOG_...` resolved to `NOTION_CINEMA_LOG_DB`.
- `NOTION_DIGEST_SELE...` resolved to `NOTION_DIGEST_SELECTOR_DB`.
- `NOTION_PERFORMANCE...` maps to current `NOTION_PERFORMANCES_DB` plus legacy aliases `NOTION_PERFORMANCE_DB` and `NOTION_PERFORMANCE_LOG_DB`.
- `NOTION_SPORTS_LOG_...` resolved to `NOTION_SPORTS_LOG_DB`.
- `NOTION_WANTSLIST_V...` resolved to `NOTION_WANTSLIST_V2_DB`.
- `RECURRING_CHECK_TI...` resolved to `RECURRING_CHECK_TIME`.
