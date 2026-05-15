from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from second_brain.notion.properties import (
    date_prop,
    extract_rich_text,
    extract_title,
    number_prop,
    query_all,
    rich_text_prop,
    title_prop,
)
from utils.alert_handlers import alert_claude_auth_failure

log = logging.getLogger(__name__)


async def query_project_memory(
    claude,
    project_id: str | None,
    project_name: str,
    date_label: str,
    model: str,
) -> dict:
    """
    Query Claude's memory for work done on a specific date in a project or non-project scope.

    Args:
        claude: Anthropic client
        project_id: Project UUID (None for non-project conversations)
        project_name: Human-readable name (for logging and context)
        date_label: Date string like "Saturday, May 9, 2026"
        model: Claude model string

    Returns:
        Dict with keys: files_touched, functions_changed, architectural_decisions,
        documents_created, professional_decisions, testing_notes, learnings, signoff_note
    """
    scope_description = f"the '{project_name}' project" if project_id else "non-project conversations"

    prompt = f"""Review your conversation memory with Ambrose for {date_label} in {scope_description}.

Project context:
- Second Brain: Python Telegram bot, Notion API integration, system architecture
- Brian II: Architectural/environmental acoustics consulting, LEED and WELL standards compliance
- Non-project: Ad-hoc technical work, research, general coding

Extract work completed on {date_label} in {scope_description}. Return ONLY valid JSON, no markdown fences:
{{
  "files_touched": ["file.py", "file2.py"],
  "functions_changed": [
    {{"name": "function_name", "action": "added|removed|refactored", "description": "brief description"}}
  ],
  "architectural_decisions": ["decision made"],
  "documents_created": ["report.docx", "analysis.xlsx"],
  "professional_decisions": ["acoustics/LEED/WELL decisions for Brian II work"],
  "testing_notes": ["what was tested or validated"],
  "learnings": ["concrete technical or professional insights"],
  "signoff_note": "If Ambrose explicitly said 'signoff:' during the conversation, extract it here. Otherwise empty string."
}}

If no work happened on {date_label} in {scope_description}, return all fields as empty arrays or empty strings."""

    try:
        # Note: Anthropic API does not currently support project_id parameter in messages.create
        # This will query across all conversations. Project-specific filtering may require
        # conversation_search tool or future API support.
        resp = claude.messages.create(
            model=model,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        result = json.loads(raw)
        log.info(
            "Memory query for %s: %d learnings, %d files",
            project_name,
            len(result.get("learnings", [])),
            len(result.get("files_touched", [])),
        )
        return result
    except Exception as e:
        log.error("Memory query failed for %s: %s", project_name, e)
        return {
            "files_touched": [],
            "functions_changed": [],
            "architectural_decisions": [],
            "documents_created": [],
            "professional_decisions": [],
            "testing_notes": [],
            "learnings": [],
            "signoff_note": "",
        }


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
        rows = query_all(
            notion,
            notion_daily_log_db,
            filter={
                "property": "Generated At",
                "date": {"on_or_after": cutoff},
            },
            sorts=[{"property": "Generated At", "direction": "ascending"}],
        )
        entries = []
        for page in rows:
            props = page.get("properties", {})

            date_label = extract_title(props.get("Date"))
            carried_forward = extract_rich_text(props.get("Carried Forward"))

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
    Supports: ## headings, bullet lines starting with • or -, plain paragraphs.
    Italic markers (_text_) are stripped for paragraph blocks.
    """
    blocks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("### "):
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"text": {"content": line[4:]}}]
                },
            })
        elif line.startswith("## "):
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"text": {"content": line[3:]}}]
                },
            })
        elif line.startswith("• ") or line.startswith("- "):
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [{"text": {"content": line[2:]}}]
                },
            })
        else:
            content = line.strip("_")
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"text": {"content": content}}]
                },
            })
    return blocks


async def generate_daily_log(
    notion,
    notion_daily_log_db: str,
    notion_db_id: str,
    notion_log_db: str,
    notion_notes_db: str,
    claude,
    claude_model: str,
    tz,
    signoff_notes: dict[str, str] | None = None,
    claude_activity: list[str] | None = None,
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
    signoff_notes = signoff_notes or {"second_brain": "", "brian_ii": ""}

    # Query Claude memory from three sources
    memory_sb = await query_project_memory(
        claude=claude,
        project_id="019302e9-131d-8001-afee-fbafec663b4d",
        project_name="Second Brain",
        date_label=date_label,
        model=claude_model,
    )

    memory_b2 = await query_project_memory(
        claude=claude,
        project_id="019deb16-b7d9-7181-a8f8-36ca090db279",
        project_name="Brian II",
        date_label=date_label,
        model=claude_model,
    )

    memory_nonproj = await query_project_memory(
        claude=claude,
        project_id=None,
        project_name="Non-Project",
        date_label=date_label,
        model=claude_model,
    )

    log.info("generate_daily_log: starting for %s", today_str)
    completed_tasks: list[str] = []
    try:
        done_pages = query_all(
            notion,
            notion_db_id,
            filter={
                "and": [
                    {"property": "Done", "checkbox": {"equals": True}},
                    {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": today_str}},
                ]
            },
        )
        for p in done_pages:
            if (p.get("last_edited_time") or "")[:10] != today_str:
                continue
            completed_tasks.append(extract_title(p.get("properties", {}).get("Name")) or "Untitled")
    except Exception as e:
        log.warning("generate_daily_log: timestamp filter failed, using broad fallback: %s", e)
        try:
            done_pages = query_all(notion, notion_db_id, filter={"property": "Done", "checkbox": {"equals": True}})
            for p in done_pages:
                if (p.get("last_edited_time") or "")[:10] == today_str:
                    completed_tasks.append(extract_title(p.get("properties", {}).get("Name")) or "Untitled")
        except Exception as inner_e:
            log.error("generate_daily_log: error fetching completed tasks: %s", inner_e)

    deferred_tasks = []
    try:
        deferred_pages = query_all(notion, notion_db_id, filter={"and": [{"property": "Done", "checkbox": {"equals": False}}, {"property": "Deadline", "date": {"equals": today_str}}]})
        deferred_tasks = [extract_title(p["properties"].get("Name")) or "Untitled" for p in deferred_pages]
    except Exception as e:
        log.error("generate_daily_log: error fetching deferred tasks: %s", e)

    habits_logged = []
    try:
        habit_log_pages = query_all(notion, notion_log_db, filter={"and": [{"property": "Completed", "checkbox": {"equals": True}}, {"property": "Date", "date": {"equals": today_str}}]})
        for p in habit_log_pages:
            entry_text = extract_title(p["properties"].get("Entry"))
            habit_name = entry_text.split(" — ")[0].strip()
            if habit_name:
                habits_logged.append(habit_name)
    except Exception as e:
        log.error("generate_daily_log: error fetching habit logs: %s", e)
    habits_count = len(habits_logged)

    notes_captured = []
    try:
        if notion_notes_db:
            note_pages = query_all(notion, notion_notes_db, filter={"property": "Date Created", "date": {"equals": today_str}})
            for p in note_pages:
                title_text = extract_title(p["properties"].get("Title"))
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

    # Build memory data summaries for synthesis
    def _format_memory_section(mem: dict, project_name: str) -> str:
        if not any(mem.get(k) for k in ["files_touched", "functions_changed", "learnings"]):
            return f"[{project_name}: No development work captured]"

        lines = [f"### {project_name}"]
        if mem.get("files_touched"):
            lines.append(f"Files: {', '.join(mem['files_touched'])}")
        if mem.get("functions_changed"):
            for fn in mem["functions_changed"]:
                lines.append(f"- {fn['name']} ({fn['action']}): {fn['description']}")
        if mem.get("architectural_decisions"):
            for dec in mem["architectural_decisions"]:
                lines.append(f"- {dec}")
        if mem.get("professional_decisions"):
            for dec in mem["professional_decisions"]:
                lines.append(f"- {dec}")
        if mem.get("documents_created"):
            lines.append(f"Documents: {', '.join(mem['documents_created'])}")
        return "\n".join(lines)

    memory_summary = "\n\n".join([
        _format_memory_section(memory_sb, "Second Brain"),
        _format_memory_section(memory_b2, "Brian II"),
        _format_memory_section(memory_nonproj, "Non-Project"),
    ])

    # Merge signoff notes from memory and manual Telegram signoffs
    signoff_sb = signoff_notes.get("second_brain", "") or memory_sb.get("signoff_note", "")
    signoff_b2 = signoff_notes.get("brian_ii", "") or memory_b2.get("signoff_note", "")

    prompt = f"""You are writing a daily development log for a software developer and acoustics consultant.
Today is {date_label}.

{cf_section}

DATA FROM TELEGRAM BOT (tasks/habits completed today):

TASKS COMPLETED ({len(completed_tasks)}):
{_bullet_list(completed_tasks)}

TASKS DEFERRED ({len(deferred_tasks)}):
{_bullet_list(deferred_tasks)}

HABITS LOGGED ({habits_count}):
{_bullet_list(habits_logged)}

NOTES CAPTURED ({len(notes_captured)}):
{_bullet_list(notes_captured)}

DATA FROM CLAUDE MEMORY (work done in Claude.ai conversations today):

{memory_summary}

MANUAL SIGNOFF NOTES:
- Second Brain: {signoff_sb if signoff_sb else "None provided"}
- Brian II: {signoff_b2 if signoff_b2 else "None provided"}

Generate a daily log in 7 sections. Synthesize BOTH Telegram data AND Claude memory data.

Return ONLY valid JSON, no markdown fences:
{{
  "summary": "2–4 sentence narrative covering both bot development and professional work. Honest, not padded.",
  "completed": "bullet list of completed tasks from Telegram, each starting with • on new line. Empty string if none.",
  "code_logic_changes": "Multi-subsection field with ### Second Brain, ### Brian II, ### Non-Project headings. Under each heading, list files touched, functions changed, architectural/professional decisions from memory data. Omit subsections where memory returned no data. Empty string if no development work across all sources.",
  "testing_validation": "bullet list from memory testing_notes + Telegram task completions related to testing. Empty string if nothing tested.",
  "issues_bugs": "bullet list of bugs/issues from Telegram deferred tasks + notes. Empty string if none.",
  "key_learnings": "Multi-subsection field with ### Second Brain and ### Brian II headings. Under each, list learnings from memory + patterns from task data. Max 5 bullets per subsection. Omit subsections where no learnings exist. Empty string if nothing notable.",
  "carried_forward": "bullet list of live unresolved threads. Max 5 bullets. Empty string if everything resolved."
}}

CRITICAL RULES:
- code_logic_changes: Source from memory data (files_touched, functions_changed, decisions). Organize by project. If all memory queries returned empty, return empty string.
- key_learnings: Source from memory learnings + task deferral patterns. Organize by project subsections.
- testing_validation: Source from memory testing_notes.
- If memory data is empty AND no Telegram notes/signoff provided, DO NOT pad sections with "Data not available" text. Return empty string for those sections.
- Subsection headers (### Second Brain, ### Brian II) only appear if that project has data."""

    try:
        resp = claude.messages.create(model=claude_model, max_tokens=2000, messages=[{"role": "user", "content": prompt}])
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        result = json.loads(raw)
        summary            = (result.get("summary") or "").strip()
        completed_text     = (result.get("completed") or "").strip()
        code_logic         = (result.get("code_logic_changes") or "").strip()
        testing            = (result.get("testing_validation") or "").strip()
        issues             = (result.get("issues_bugs") or "").strip()
        key_learnings      = (result.get("key_learnings") or "").strip()
        carried_forward    = (result.get("carried_forward") or "").strip()
    except Exception as e:
        alert_claude_auth_failure(str(e))
        log.error("generate_daily_log: Claude call failed: %s", e)
        summary = ""; completed_text = ""; code_logic = ""
        testing = ""; issues = ""; key_learnings = ""
        carried_forward = f"Log generation failed: {e}"

    page_body_parts = []
    page_body_parts.append(f"# Daily Development Log — {date_label}")

    if summary:
        page_body_parts.append(f"## Summary\n\n{summary}")

    if completed_text:
        page_body_parts.append(f"## Completed\n\n{completed_text}")

    if key_learnings:
        page_body_parts.append(f"## Key Learnings / Decisions\n\n{key_learnings}")

    if code_logic:
        page_body_parts.append(f"## Code / Logic Changes\n\n{code_logic}")

    if testing:
        page_body_parts.append(f"## Testing / Validation\n\n{testing}")

    if issues:
        page_body_parts.append(f"## Issues / Bugs Found\n\n{issues}")

    # Render signoff notes if provided (manual or from memory)
    if signoff_sb or signoff_b2:
        signoff_parts = []
        if signoff_sb:
            signoff_parts.append(f"### Second Brain\n\n_{signoff_sb}_")
        if signoff_b2:
            signoff_parts.append(f"### Brian II\n\n_{signoff_b2}_")
        page_body_parts.append("## Signoff Notes\n\n" + "\n\n".join(signoff_parts))

    if carried_forward:
        page_body_parts.append(f"## Carried Forward\n\n{carried_forward}")

    if not page_body_parts or len(page_body_parts) == 1:  # Only header
        page_body_parts.append("_Light day — nothing notable to log._")

    page_content = "\n\n".join(page_body_parts)

    daily_log_db_properties: dict[str, Any] = {}
    try:
        daily_log_db_properties = notion.databases.retrieve(
            database_id=notion_daily_log_db
        ).get("properties", {})
    except Exception as e:
        log.warning("generate_daily_log: could not retrieve daily log schema: %s", e)

    props: dict[str, Any] = {
        "Date": title_prop(date_label),
        "Tasks Completed": number_prop(len(completed_tasks)),
        "Habits Logged": number_prop(habits_count),
        "Generated At": date_prop(datetime.now(tz).isoformat()),
    }
    if summary:
        props["Summary"] = rich_text_prop(summary[:2000])
    if key_learnings:
        props["Key Learnings"] = rich_text_prop(key_learnings[:2000])
    if code_logic and daily_log_db_properties.get("Code Changes"):
        props["Code Changes"] = rich_text_prop(code_logic[:2000])
    if signoff_sb or signoff_b2:
        combined_signoff = []
        if signoff_sb:
            combined_signoff.append(f"[Second Brain] {signoff_sb}")
        if signoff_b2:
            combined_signoff.append(f"[Brian II] {signoff_b2}")
        props["Signoff Note"] = rich_text_prop("\n\n".join(combined_signoff)[:2000])
    if carried_forward:
        props["Carried Forward"] = rich_text_prop(carried_forward[:2000])

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
