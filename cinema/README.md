# Cinema Module

This package contains Cinema Log TMDB sync implementation and configuration.

## Files

- `sync.py`: TMDB URL backfill + favourites promotion sync.
- `config.py`: environment-driven configuration and validation helpers.
- `handlers.py`: reserved for future Telegram handlers.

## Environment variables

- `NOTION_CINEMA_LOG_DB` (preferred) or `NOTION_CINEMA_DB` (legacy alias)
- `NOTION_FAVE_DB`
- `TMDB_API_KEY` (preferred) or `TMDB_KEY` (legacy alias)

## Runtime behavior

- On each new cinema log created by the bot, the bot immediately attempts to resolve and write `TMDB URL`.
- Hourly background sync scans entries with empty `TMDB URL` and a non-empty `Film` title (falls back to detected Notion title property if schema differs).
- `/sync` includes cinema sync and reports concise cinema stats.
- `/sync cinema` runs cinema-only sync and reports: scanned, updated, skipped, failed.
