"""Entertainment handler helpers extracted from the Telegram entrypoint."""

from __future__ import annotations

import logging

from second_brain.entertainment import log as ent_log
from second_brain import keyboards as kb  # noqa: F401 - imported for transition parity
from second_brain.utils import ExpiringDict, local_today, reply_notion_error  # noqa: F401
from second_brain.config import (  # noqa: F401
    NOTION_CINEMA_LOG_DB,
    NOTION_PERFORMANCE_LOG_DB,
    NOTION_SPORTS_LOG_DB,
    NOTION_FAVE_DB,
)
from second_brain.state import STATE  # noqa: F401


log = logging.getLogger(__name__)


def _entertainment_rule_entry_data(payload: dict) -> dict:
    """Build normalized rule-engine entry data from an entertainment payload."""
    return {
        "Title": payload.get("title"),
        "DateWatched": payload.get("date") or local_today().isoformat(),
        "Favourite": bool(payload.get("favourite")),
    }


async def _execute_entertainment_rules(notion, rule_engine, payload: dict) -> bool:
    """Run post-save entertainment rules and return whether a favourite action succeeded."""
    if not rule_engine or payload.get("log_type") != "cinema":
        return False
    results = await rule_engine.execute_on_save(
        source_db="cinema_log",
        entry_data=_entertainment_rule_entry_data(payload),
        db_ids={
            "cinema_log": NOTION_CINEMA_LOG_DB,
            "favourite_films": NOTION_FAVE_DB,
        },
    )
    fav_rule_success = False
    for result in results:
        if result.get("success"):
            log.info("✅ Rule %s: %s", result.get("rule_id"), result.get("message"))
            if result.get("rule_id") == "cinema_to_favourite":
                fav_rule_success = True
        else:
            log.warning("⚠️ Rule %s: %s", result.get("rule_id"), result.get("message"))
    return fav_rule_success


async def handle_entertainment_log(notion, message, payload: dict) -> None:
    entry_id, fav_saved = ent_log.create_entertainment_log_entry(notion, payload)
    import second_brain.main as _main  # transition import

    rule_fav_saved = await _execute_entertainment_rules(notion, _main.rule_engine, payload)
    title = payload.get("title", "Untitled")
    log_type = payload.get("log_type", "cinema")
    venue = payload.get("venue")
    notes = payload.get("notes")
    when_iso = payload.get("date") or local_today().isoformat()

    summary_lines = [
        f"✅ Logged to { {'cinema': 'Cinema', 'performance': 'Performance', 'sport': 'Sports'}.get(log_type, 'Entertainment') }",
        "",
        f"🎫 {title}",
        f"📅 {when_iso}",
    ]
    if venue:
        summary_lines.append(f"📍 {venue}")
    if notes:
        summary_lines.append(f"📝 {notes}")
    if (fav_saved or rule_fav_saved) and log_type == "cinema":
        summary_lines.append("🎞️ Added to Favourite Films")
    summary_lines.append("")
    summary_lines.append("_Saved to Notion_")
    await message.reply_text("\n".join(summary_lines), parse_mode="Markdown")
    if log_type == "sport":
        ent_log._remember_pending_sport_competition(message, entry_id)
        await message.reply_text("🏆 Logged to Sports Log. Which competition should I set for this one?")
    log.info("Entertainment logged type=%s title=%s page_id=%s", log_type, title, entry_id)


async def _maybe_prompt_explicit_venue(notion, message, payload: dict, raw_text: str) -> bool:
    return await ent_log._maybe_prompt_explicit_venue(notion, message, payload, raw_text)


def load_entertainment_schemas(notion) -> None:
    ent_log.load_entertainment_schemas(notion)


def _resolve_known_cinema_venue(venue: str | None, schema: dict) -> str | None:
    import second_brain.main as _main  # transition import

    return ent_log._resolve_known_cinema_venue(_main.notion, venue, schema)


def _find_existing_cinema_venue(title: str, schema: dict) -> str | None:
    import second_brain.main as _main  # transition import

    return ent_log._find_existing_cinema_venue(_main.notion, title, schema)


def _suggest_known_venue(payload: dict) -> tuple[str | None, str | None]:
    import second_brain.main as _main  # transition import

    return ent_log._suggest_known_venue(_main.notion, payload)
