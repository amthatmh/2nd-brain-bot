"""Cinema Log Sync configuration."""

import os

CINEMA_DB_ID = os.environ.get("NOTION_CINEMA_DB", "")
FAVE_DB_ID = os.environ.get("NOTION_FAVE_DB", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
CINEMA_SYNC_HOUR = int(os.environ.get("CINEMA_SYNC_HOUR", "23"))
CINEMA_SYNC_MINUTE = int(os.environ.get("CINEMA_SYNC_MINUTE", "30"))


def validate_config() -> tuple[bool, list[str]]:
    """Return whether cinema sync config is valid and any problems."""
    problems: list[str] = []
    if not CINEMA_DB_ID:
        problems.append("NOTION_CINEMA_DB is missing or empty")
    if not (0 <= CINEMA_SYNC_HOUR <= 23):
        problems.append("CINEMA_SYNC_HOUR must be between 0 and 23")
    if not (0 <= CINEMA_SYNC_MINUTE <= 59):
        problems.append("CINEMA_SYNC_MINUTE must be between 0 and 59")
    return len(problems) == 0, problems


def get_config_summary() -> dict:
    """Return a safe, loggable summary of cinema configuration."""
    return {
        "enabled": True,
        "cinema_db_set": bool(CINEMA_DB_ID),
        "fave_db_set": bool(FAVE_DB_ID),
        "tmdb_key_set": bool(TMDB_API_KEY),
        "sync_time": f"{CINEMA_SYNC_HOUR:02d}:{CINEMA_SYNC_MINUTE:02d} UTC",
    }
