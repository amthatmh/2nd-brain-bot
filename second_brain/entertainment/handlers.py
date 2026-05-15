"""Entertainment handler helpers extracted from the Telegram entrypoint."""

from __future__ import annotations

import logging

from second_brain.entertainment import log as ent_log
from second_brain.utils import local_today
from second_brain.config import NOTION_CINEMA_LOG_DB, NOTION_FAVE_DB

log = logging.getLogger(__name__)

ENTERTAINMENT_LOG_LABELS = {
    "cinema": "🍿 Cinema Log",
    "performance": "🎟️ Performances Viewings",
    "sport": "🏟️ Sports Log",
}


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


async def handle_entertainment_log(notion, message, payload: dict, *, rule_engine=None) -> None:
    entry_id, fav_saved = ent_log.create_entertainment_log_entry(notion, payload)
    rule_fav_saved = await _execute_entertainment_rules(notion, rule_engine, payload)
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



