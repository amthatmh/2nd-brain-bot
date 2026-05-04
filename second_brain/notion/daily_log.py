from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from second_brain.notion import tasks as notion_tasks

log = logging.getLogger(__name__)


def get_recent_carried_forward(notion, notion_daily_log_db: str, tz, days: int = 3) -> list[dict]:
    """
    Fetch Carried Forward content from the last N Daily Log entries.
    Returns list of dicts: {date: str, carried_forward: str}
    Sorted oldest to newest so Claude reads them in chronological order.
    """
    if not notion_daily_log_db:
        return []
    try:
        cutoff = (datetime.now(tz).date() - timedelta(days=days)).isoformat()
        results = notion.databases.query(
            database_id=notion_daily_log_db,
            filter={
                "property": "Generated At",
                "date": {"on_or_after": cutoff},
            },
            sorts=[{"property": "Generated At", "direction": "ascending"}],
        )
        entries = []
        for page in results.get("results", []):
            props = page.get("properties", {})

            title_parts = props.get("Date", {}).get("title", [])
            date_label = "".join(
                part.get("text", {}).get("content", "") for part in title_parts
            ).strip()

            cf_parts = props.get("Carried Forward", {}).get("rich_text", [])
            carried_forward = "".join(
                part.get("text", {}).get("content", "") for part in cf_parts
            ).strip()

            if date_label and carried_forward:
                entries.append({
                    "date": date_label,
                    "carried_forward": carried_forward,
                })
        return entries
    except Exception as e:
        log.error("get_recent_carried_forward: error: %s", e)
        return []


def get_existing_daily_log(notion, notion_daily_log_db: str, date_label: str) -> str | None:
    """
    Check if a Daily Log entry already exists for the given date label.
    Returns the Notion page_id if found, None otherwise.
    """
    if not notion_daily_log_db:
        return None
    try:
        results = notion.databases.query(
            database_id=notion_daily_log_db,
            filter={
                "property": "Date",
                "title": {"equals": date_label},
            },
        )
        pages = results.get("results", [])
        return pages[0]["id"] if pages else None
    except Exception as e:
        log.error("get_existing_daily_log: error querying for %s: %s", date_label, e)
        return None


def _notion_markdown_to_blocks(text: str) -> list[dict]:
    """
    Convert a simple markdown string to Notion block objects.
    Supports: ## headings, bullet lines starting with •, plain paragraphs.
    Italic markers (_text_) are stripped for paragraph blocks.
    """
    blocks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("## "):
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": line[3:]}}]
                },
            })
        elif line.startswith("• "):
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [{"type": "text", "text": {"content": line[2:]}}]
                },
            })
        else:
            content = line.strip("_")
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": content}}]
                },
            })
    return blocks


def generate_daily_log(
    notion,
    notion_daily_log_db: str,
    notion_db_id: str,
    notion_log_db: str,
    notion_notes_db: str,
    claude,
    claude_model: str,
    tz,
    signoff_note: str = "",
) -> str | None:
    """
    Generates end-of-day narrative log and writes it to 📓 Daily Log Notion DB.
    Triggered by a Digest Selector slot with Signoff=True (typically 23:59).
    Runs silently — no Telegram message at generation time.
    Link is sent next morning via send_daily_digest().
    """
    if not notion_daily_log_db:
        log.warning("generate_daily_log: NOTION_DAILY_LOG_DB not configured, skipping")
        return None

    today = datetime.now(tz).date()
    today_str = today.isoformat()
    date_label = today.strftime("%A, %B %-d, %Y")

    log.info("generate_daily_log: starting for %s", today_str)
    completed_tasks = []
    try:
        try:
            completed_results = notion.databases.query(database_id=notion_db_id, filter={"and": [{"property": "Done", "checkbox": {"equals": True}}, {"property": "Last Edited Time", "date": {"equals": today_str}}]})
            completed_tasks = [notion_tasks._get_prop(p["properties"], "Name", "title") or "Untitled" for p in completed_results.get("results", [])]
        except Exception:
            log.warning("generate_daily_log: Last Edited Time filter unsupported, using Python fallback")
            all_done = notion.databases.query(database_id=notion_db_id, filter={"property": "Done", "checkbox": {"equals": True}})
            for p in all_done.get("results", []):
                if (p.get("last_edited_time") or "")[:10] == today_str:
                    completed_tasks.append(notion_tasks._get_prop(p["properties"], "Name", "title") or "Untitled")
    except Exception as e:
        log.error("generate_daily_log: error fetching completed tasks: %s", e)

    deferred_tasks = []
    try:
        deferred_results = notion.databases.query(database_id=notion_db_id, filter={"and": [{"property": "Done", "checkbox": {"equals": False}}, {"property": "Deadline", "date": {"equals": today_str}}]})
        deferred_tasks = [notion_tasks._get_prop(p["properties"], "Name", "title") or "Untitled" for p in deferred_results.get("results", [])]
    except Exception as e:
        log.error("generate_daily_log: error fetching deferred tasks: %s", e)

    habits_logged = []
    try:
        habit_log_results = notion.databases.query(database_id=notion_log_db, filter={"and": [{"property": "Completed", "checkbox": {"equals": True}}, {"property": "Date", "date": {"equals": today_str}}]})
        for p in habit_log_results.get("results", []):
            entry_parts = p["properties"].get("Entry", {}).get("title", [])
            entry_text = "".join(part.get("text", {}).get("content", "") for part in entry_parts)
            habit_name = entry_text.split(" — ")[0].strip()
            if habit_name:
                habits_logged.append(habit_name)
    except Exception as e:
        log.error("generate_daily_log: error fetching habit logs: %s", e)
    habits_count = len(habits_logged)

    notes_captured = []
    try:
        if notion_notes_db:
            notes_results = notion.databases.query(database_id=notion_notes_db, filter={"property": "Date Created", "date": {"equals": today_str}})
            for p in notes_results.get("results", []):
                title_parts = p["properties"].get("Title", {}).get("title", [])
                title_text = "".join(part.get("text", {}).get("content", "") for part in title_parts).strip()
                if title_text:
                    notes_captured.append(title_text)
    except Exception as e:
        log.error("generate_daily_log: error fetching notes: %s", e)

    recent_carried_forward = get_recent_carried_forward(notion, notion_daily_log_db, tz, days=3)

    def _bullet_list(items: list[str]) -> str:
        return "\n".join(f"- {i}" for i in items) if items else "None"

    if recent_carried_forward:
        cf_context = "\n\n".join(
            f"{entry['date']}:\n{entry['carried_forward']}"
            for entry in recent_carried_forward
        )
        cf_section = f"""CARRIED FORWARD FROM PREVIOUS DAYS (unresolved threads, up to 3 days):
{cf_context}

"""
    else:
        cf_section = "CARRIED FORWARD FROM PREVIOUS DAYS:\nNone — this may be the first log entry.\n\n"

    prompt = f"""You are writing the end-of-day log entry for a personal second brain system.
Today is {date_label}.

{cf_section}Here is what happened today:

TASKS COMPLETED ({len(completed_tasks)}):
{_bullet_list(completed_tasks)}

TASKS DUE TODAY BUT DEFERRED ({len(deferred_tasks)}):
{_bullet_list(deferred_tasks)}

HABITS LOGGED ({habits_count}):
{_bullet_list(habits_logged)}

NOTES CAPTURED ({len(notes_captured)}):
{_bullet_list(notes_captured)}

USER'S SIGNOFF NOTE (their own words about what they worked on today, including any Claude.ai conversations and decisions made):
{signoff_note if signoff_note else "None provided"}

Write a daily log entry in three parts. Be concise, honest, and personal.

PART 1 — SUMMARY:
1 to 3 lines maximum. Can be a single line or empty string on a light day.
Describe the shape of the day — not a list, a narrative.

PART 2 — KEY LEARNINGS:
What actually mattered today. What was built, decided, deferred and why,
what patterns emerged.
If a signoff note was provided, weight it heavily — it reflects the user's
own assessment of the day including work done outside this system.
If carried forward threads were resolved today, note it explicitly.
If the day was light, say so honestly in one line. Never pad.
Maximum 5 bullet points starting with •. Can be fewer. Can be empty.

PART 3 — CARRIED FORWARD:
Distill what is still live and unresolved going into tomorrow.
Sources to draw from:
- Unresolved threads from previous Carried Forward entries that did not
  resolve today
- Deferred tasks that have appeared more than once (pattern of avoidance)
- Ideas or decisions from today's Key Learnings that have no action yet
- Anything from the signoff note that implies follow-up

Rules:
- Drop anything that resolved today — completed tasks, closed decisions
- Maximum 5 bullet points starting with •
- Can be empty if everything is resolved or the day was truly light
- Write each point as a live thread, not a task — e.g.
  "• Daily Log Carried Forward logic — shipped v13.1, watching for edge cases"
  not "• Check if Carried Forward works"

Return ONLY valid JSON, no markdown fences:
{{
  "summary": "1-3 line narrative, or empty string if nothing notable",
  "key_learnings": "bullet points as single string, each starting with • on new line, or empty string",
  "carried_forward": "bullet points as single string, each starting with • on new line, or empty string"
}}"""

    summary = ""
    key_learnings = ""
    carried_forward = ""
    try:
        resp = claude.messages.create(model=claude_model, max_tokens=800, messages=[{"role": "user", "content": prompt}])
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        result = json.loads(raw)
        summary = (result.get("summary") or "").strip()
        key_learnings = (result.get("key_learnings") or "").strip()
        carried_forward = (result.get("carried_forward") or "").strip()
    except Exception as e:
        log.error("generate_daily_log: Claude call failed: %s", e)
        summary = ""
        key_learnings = f"Log generation failed: {e}"
        carried_forward = ""

    page_body_parts = []
    if summary: page_body_parts.append(f"## Summary\n\n{summary}")
    if key_learnings: page_body_parts.append(f"## Key Learnings\n\n{key_learnings}")
    if completed_tasks: page_body_parts.append("## Completed\n\n" + "\n".join(f"• {t}" for t in completed_tasks))
    if deferred_tasks: page_body_parts.append("## Deferred\n\n" + "\n".join(f"• {t}" for t in deferred_tasks))
    if signoff_note: page_body_parts.append(f"## Signoff Note\n\n_{signoff_note}_")
    if carried_forward:
        page_body_parts.append(
            f"## Carried Forward\n\n{carried_forward}"
        )
    page_content = "\n\n".join(page_body_parts) if page_body_parts else "_Light day — nothing notable to log._"

    props: dict[str, Any] = {
        "Date": {"title": [{"text": {"content": date_label}}]},
        "Tasks Completed": {"number": len(completed_tasks)},
        "Habits Logged": {"number": habits_count},
        "Generated At": {
            "date": {
                "start": datetime.now(tz).isoformat(),
            }
        },
    }
    if summary:
        props["Summary"] = {"rich_text": [{"text": {"content": summary[:2000]}}]}
    if key_learnings:
        props["Key Learnings"] = {"rich_text": [{"text": {"content": key_learnings[:2000]}}]}
    if signoff_note:
        props["Signoff Note"] = {"rich_text": [{"text": {"content": signoff_note[:2000]}}]}
    if carried_forward:
        props["Carried Forward"] = {
            "rich_text": [{"text": {"content": carried_forward[:2000]}}]
        }

    try:
        existing_page_id = get_existing_daily_log(notion, notion_daily_log_db, date_label)

        if existing_page_id:
            notion.pages.update(
                page_id=existing_page_id,
                properties=props,
            )
            existing_blocks = notion.blocks.children.list(
                block_id=existing_page_id
            ).get("results", [])
            for block in existing_blocks:
                try:
                    notion.blocks.delete(block_id=block["id"])
                except Exception as block_err:
                    log.warning(
                        "generate_daily_log: could not delete block %s: %s",
                        block["id"],
                        block_err,
                    )
            notion.blocks.children.append(
                block_id=existing_page_id,
                children=_notion_markdown_to_blocks(page_content),
            )
            page_id = existing_page_id
            log.info(
                "generate_daily_log: updated existing entry for %s", today_str
            )
        else:
            page = notion.pages.create(
                parent={"database_id": notion_daily_log_db},
                properties=props,
                children=_notion_markdown_to_blocks(page_content),
            )
            page_id = page["id"]
            log.info(
                "generate_daily_log: created new entry for %s", today_str
            )

        log.info(
            "generate_daily_log: complete for %s — %d completed, %d deferred, %d habits",
            today_str,
            len(completed_tasks),
            len(deferred_tasks),
            habits_count,
        )
        return f"https://www.notion.so/{page_id.replace('-', '')}"
    except Exception as e:
        log.error("generate_daily_log: Notion write failed: %s", e)
        return None
