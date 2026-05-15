"""
Weekly Program lookup and auto-linking for workout logs.

These helpers leverage the existing process_pending_programmes scheduler job,
which marks Weekly Programs as processed and creates Workout Day rows.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

from second_brain.notion import notion_call

from second_brain.crossfit.notion import parse_weekly_program_text, save_programme_from_notion_row
from second_brain.notion.properties import query_all, rich_text_prop

log = logging.getLogger(__name__)


def _movement_cache() -> dict:
    from second_brain.crossfit.handlers import MOVEMENTS_CACHE

    return MOVEMENTS_CACHE


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _weekly_programs_db_id() -> str:
    return os.getenv("NOTION_WEEKLY_PROGRAMS_DB") or os.getenv("NOTION_WORKOUT_PROGRAM_DB", "")


async def get_current_week_program_url(notion_client) -> Optional[str]:
    """
    Query Weekly Programs for the current week's processed program.

    The function name is kept for compatibility with the Phase 1 plan, but it
    returns the Notion page ID because relation properties require page IDs.
    """
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    week_string = f"Week of {monday.strftime('%Y-%m-%d')}"
    weekly_programs_db_id = _weekly_programs_db_id()
    if not weekly_programs_db_id or notion_client is None:
        return None

    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=weekly_programs_db_id,
                filter={
                    "and": [
                        {"property": "Name", "title": {"contains": week_string}},
                        {"property": "Processed", "checkbox": {"equals": True}},
                    ]
                },
                page_size=1,
            )
        )
        if results.get("results"):
            return results["results"][0].get("id")
        return None
    except Exception as e:
        log.error("Error fetching current week program: %s", e)
        return None


async def get_todays_workout_day(notion_client) -> Optional[Dict]:
    """
    Get today's Workout Day entry for movement and format reference.

    Returns selected Section B/C properties and relations, or None if no row is
    found for the current weekday in the last seven days.
    """
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    day_name = today.strftime("%A")
    workout_days_db_id = os.getenv("NOTION_WORKOUT_DAYS_DB", "")
    if not workout_days_db_id or notion_client is None:
        return None

    try:
        results = await _maybe_await(
            notion_call(
                notion_client.databases.query,
                database_id=workout_days_db_id,
                filter={
                    "and": [
                        {"property": "Day", "select": {"equals": day_name}},
                        {"property": "Week Of", "date": {"equals": monday.strftime("%Y-%m-%d")}},
                    ]
                },
                page_size=10,
            )
        )
        if not results.get("results"):
            return None
        pages = results.get("results", [])
        props = pages[0].get("properties", {})

        def select_name(key: str):
            return (props.get(key, {}).get("select") or {}).get("name")

        def relation_ids(key: str):
            return [rel.get("id") for rel in props.get(key, {}).get("relation", []) if rel.get("id")]

        def rich_text(key: str):
            return "".join(x.get("plain_text", "") for x in props.get(key, {}).get("rich_text", [])) or None

        all_b_movements: list[str] = []
        all_c_movements: list[str] = []
        for matched_page in pages:
            props = matched_page.get("properties", {})
            for movement_id in relation_ids("Section B Movements"):
                if movement_id not in all_b_movements:
                    all_b_movements.append(movement_id)
            for movement_id in relation_ids("Section C Movements"):
                if movement_id not in all_c_movements:
                    all_c_movements.append(movement_id)
        props = pages[0].get("properties", {})
        return {
            "Section B Type": select_name("Section B Type"),
            "Section C Format": select_name("Section C Format"),
            "Section B Movements": all_b_movements,
            "Section C Movements": all_c_movements,
            "Section B": rich_text("Section B"),
            "Section C": rich_text("Section C"),
        }
    except Exception as e:
        log.error("Error fetching today's workout: %s", e)
        return None


# TESTING CHECKLIST — Phase 1 Weekly Program Auto-Linking
# [ ] Test get_current_week_program_url returns current week page ID
# [ ] Test returns None if no matching week or Processed = false
# [ ] Test get_todays_workout_day returns correct Day (Monday/Tuesday/etc)
# [ ] Test Section C Format matches expected value (AMRAP/For Time/etc)
# [ ] Verify both functions called in handle_strength_log() and handle_wod_log()


async def process_pending_programmes(notion, bot, *, workout_program_db: str, chat_id: int) -> None:
    """Poll Weekly Programs DB for unprocessed rows and parse/save asynchronously."""
    if not workout_program_db:
        return

    try:
        rows = query_all(
            notion,
            workout_program_db,
            filter={
                "and": [
                    {"property": "Processed", "checkbox": {"equals": False}},
                    {"property": "Full Program", "rich_text": {"is_not_empty": True}},
                ]
            },
        )
    except Exception as e:
        log.error("process_pending_programmes: query failed: %s", e)
        return

    if not rows:
        return

    log.info("process_pending_programmes: found %d unprocessed row(s)", len(rows))

    for row in rows:
        page_id = row["id"]
        props = row.get("properties", {})
        title_parts = props.get("Name", {}).get("title", [])
        week_name = title_parts[0].get("plain_text", "") if title_parts else "Unknown week"

        rt = props.get("Full Program", {}).get("rich_text", [])
        full_text = "".join(chunk.get("plain_text", "") for chunk in rt).strip()
        if not full_text:
            continue

        log.info("process_pending_programmes: processing '%s' (%d chars)", week_name, len(full_text))

        try:
            parsed = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: parse_weekly_program_text(full_text, week_name),
            )

            result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: save_programme_from_notion_row(
                    notion,
                    page_id,
                    os.getenv("NOTION_WORKOUT_DAYS_DB", ""),
                    os.getenv("NOTION_MOVEMENTS_DB", ""),
                    parsed,
                    workout_program_db,
                    os.getenv("NOTION_CYCLES_DB", ""),
                    _movement_cache(),
                ),
            )
            days_created = result["days_created"]

            try:
                movement_ids = result.get("movement_ids", [])
                if movement_ids:
                    notion_call(
                        notion.pages.update,
                        page_id=page_id,
                        properties={"Movements": {"relation": [{"id": mid} for mid in movement_ids]}},
                    )
                    log.info(
                        "process_pending_programmes: wrote %d movements to Weekly Programs row",
                        len(movement_ids),
                    )
            except Exception as e:
                log.warning("process_pending_programmes: could not write movements rollup: %s", e)

            cycle_line = None
            try:
                new_cycle = props.get("New Cycle", {}).get("checkbox", False)
                if new_cycle:
                    all_rows = query_all(notion, workout_program_db, page_size=100)
                    cycle_num = max(
                        (r.get("properties", {}).get("Cycle", {}).get("number") or 0 for r in all_rows),
                        default=0,
                    ) + 1
                    week_num = 1
                else:
                    recent_processed = notion_call(
                        notion.databases.query,
                        database_id=workout_program_db,
                        filter={"property": "Processed", "checkbox": {"equals": True}},
                        sorts=[{"property": "Week", "direction": "descending"}],
                        page_size=10,
                    ).get("results", [])
                    cycle_num = next(
                        (r.get("properties", {}).get("Cycle", {}).get("number")
                         for r in recent_processed
                         if r.get("properties", {}).get("Cycle", {}).get("number")),
                        1,
                    )
                    cycle_rows = query_all(
                        notion,
                        workout_program_db,
                        filter={"and": [
                            {"property": "Processed", "checkbox": {"equals": True}},
                            {"property": "Cycle", "number": {"equals": cycle_num}},
                        ]},
                    )
                    week_num = len(cycle_rows) + 1

                notion_call(
                    notion.pages.update,
                    page_id=page_id,
                    properties={
                        "Cycle": {"number": cycle_num},
                        "Week": {"number": week_num},
                    },
                )
                log.info("[CYCLE] Cycle %d, Week %d", cycle_num, week_num)
                cycle_line = (
                    f"🔁 Cycle {cycle_num} started — Week 1"
                    if new_cycle
                    else f"📅 Cycle {cycle_num} — Week {week_num}"
                )
            except Exception as e:
                log.warning("[CYCLE] Non-fatal error in cycle logic: %s", e)

            notion_call(
                notion.pages.update,
                page_id=page_id,
                properties={"Processed": {"checkbox": True}, "Parse Error": {"rich_text": []}},
            )

            tracks = parsed.get("tracks", []) if isinstance(parsed, dict) else []
            parsed_week_label = parsed.get("week_label") if isinstance(parsed, dict) else None
            display_week_name = (parsed_week_label or week_name or "Week").strip()
            track_names = ", ".join(t.get("track", "") for t in tracks if t.get("track"))
            lines = [
                f"📋 *{display_week_name}* parsed ✅",
                "",
                f"Tracks: {track_names or 'N/A'}",
                f"Day rows created: {days_created}",
                "_Saved to Workout Days_",
            ]
            if cycle_line:
                lines.append(cycle_line)
            await bot.send_message(
                chat_id=chat_id,
                text="\n".join(lines),
                parse_mode="Markdown",
            )
            log.info("process_pending_programmes: completed '%s'", week_name)
        except Exception as e:
            log.error(f"[PARSER] Failed to parse week {week_name}: {e}")
            try:
                notion_call(
                    notion.pages.update,
                    page_id=page_id,
                    properties={"Parse Error": rich_text_prop(str(e)[:1900])},
                )
            except Exception as inner:
                log.error("process_pending_programmes: could not write error to Notion: %s", inner)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ Couldn't parse *{week_name}*\n\n`{str(e)[:300]}`",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
