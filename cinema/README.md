# Cinema Module

This package contains the Cinema Log sync implementation and configuration.

## Files

- `sync.py`: daily sync from Cinema Log to Favourite Shows.
- `config.py`: environment-driven configuration and validation helpers.
- `handlers.py`: reserved for future Telegram handlers.

## Environment variables

- `NOTION_CINEMA_DB`
- `NOTION_FAVE_DB`
- `TMDB_API_KEY` (optional)
- `CINEMA_SYNC_ENABLED`
- `CINEMA_SYNC_HOUR` (default `23`)
- `CINEMA_SYNC_MINUTE` (default `30`)
