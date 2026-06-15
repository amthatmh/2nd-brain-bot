"""Sync the Notion Work Sync library to Markdown files served over HTTP."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_DB_ID = "380302e9131d80f6a1e8cc6ee930db43"
DEFAULT_OUT = "./work-sync"


# ── Rich-text & block rendering ──────────────────────────────────────────────

def _render_rich_text(parts: list[dict]) -> str:
    out: list[str] = []
    for part in parts:
        text = part.get("plain_text", "")
        ann = part.get("annotations", {})
        href = part.get("href")
        if href:
            text = f"[{text}]({href})"
        if ann.get("code"):
            text = f"`{text}`"
        if ann.get("bold"):
            text = f"**{text}**"
        if ann.get("italic"):
            text = f"*{text}*"
        out.append(text)
    return "".join(out)


def _blocks_to_md(blocks: list[dict], depth: int = 0) -> list[str]:
    lines: list[str] = []
    indent = "  " * depth
    for block in blocks:
        btype = block.get("type", "")
        content = block.get(btype, {}) or {}
        rt = content.get("rich_text", [])
        text = _render_rich_text(rt)
        children = block.get("children", [])

        if btype in ("heading_1", "heading_2", "heading_3"):
            level = int(btype[-1])
            lines.append(f"{'#' * level} {text}")
        elif btype == "paragraph":
            lines.append(text or "")
        elif btype == "bulleted_list_item":
            lines.append(f"{indent}- {text}")
            lines.extend(_blocks_to_md(children, depth + 1))
            continue
        elif btype == "numbered_list_item":
            lines.append(f"{indent}1. {text}")
            lines.extend(_blocks_to_md(children, depth + 1))
            continue
        elif btype == "to_do":
            mark = "x" if content.get("checked") else " "
            lines.append(f"{indent}- [{mark}] {text}")
            lines.extend(_blocks_to_md(children, depth + 1))
            continue
        elif btype == "quote":
            lines.append(f"> {text}")
        elif btype == "callout":
            icon = content.get("icon") or {}
            emoji = icon.get("emoji", "") if icon.get("type") == "emoji" else ""
            lines.append(f"{emoji} {text}".strip() if emoji else text)
        elif btype == "code":
            lang = content.get("language", "") or ""
            code_text = "\n".join(p.get("plain_text", "") for p in rt)
            lines.append(f"```{lang}")
            lines.append(code_text)
            lines.append("```")
        elif btype == "divider":
            lines.append("---")
        else:
            if text:
                lines.append(text)

        if children and btype not in ("bulleted_list_item", "numbered_list_item", "to_do"):
            lines.extend(_blocks_to_md(children, depth))

    return lines


# ── Notion fetching ───────────────────────────────────────────────────────────

def _fetch_block_tree(notion: Any, block_id: str) -> list[dict]:
    """Recursively fetch block children, attaching them under block['children']."""
    blocks: list[dict] = []
    cursor: str | None = None
    while True:
        kwargs: dict[str, Any] = {"block_id": block_id, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.blocks.children.list(**kwargs)
        for block in resp.get("results", []):
            if block.get("has_children"):
                block["children"] = _fetch_block_tree(notion, block["id"])
            else:
                block["children"] = []
            blocks.append(block)
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


def _query_active_rows(notion: Any, db_id: str) -> list[dict]:
    rows: list[dict] = []
    cursor: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "database_id": db_id,
            "filter": {"property": "Active", "checkbox": {"equals": True}},
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.databases.query(**kwargs)
        rows.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return rows


def _prop_title(props: dict, key: str) -> str:
    parts = (props.get(key) or {}).get("title") or []
    return "".join(p.get("plain_text", "") for p in parts).strip()


def _prop_select(props: dict, key: str) -> str:
    sel = (props.get(key) or {}).get("select") or {}
    return sel.get("name", "").strip()


def _prop_rich_text(props: dict, key: str) -> str:
    parts = (props.get(key) or {}).get("rich_text") or []
    return "".join(p.get("plain_text", "") for p in parts).strip()


# ── Naming helpers ────────────────────────────────────────────────────────────

def name_to_slug(name: str) -> str:
    """Convert a display name to a hyphenated slug for skill directories."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def readme_filename(name: str) -> str:
    """Return the output filename for a README row."""
    if name.strip() == "Work Context":
        return "README.md"
    safe = name.strip().replace(" ", "_")
    safe = re.sub(r"[^A-Za-z0-9_\-]", "", safe)
    return f"{safe}.md"


# ── Core sync ─────────────────────────────────────────────────────────────────

def run_sync(
    out: str | Path,
    db_id: str | None = None,
    notion: Any = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch active rows from the Work Sync Notion DB and write Markdown files.

    Returns a summary dict with keys: written, skipped, errors.
    """
    from dotenv import load_dotenv

    load_dotenv()

    if notion is None:
        from notion_client import Client
        token = os.environ.get("NOTION_TOKEN", "").strip()
        if not token:
            raise RuntimeError("NOTION_TOKEN is not set")
        notion = Client(auth=token)

    resolved_db_id = db_id or os.environ.get("NOTION_WORK_SYNC_DB", DEFAULT_DB_ID)
    out_dir = Path(out)

    log.info("work_sync: querying db %s", resolved_db_id)
    rows = _query_active_rows(notion, resolved_db_id)
    log.info("work_sync: %d active rows", len(rows))

    written: list[str] = []
    skipped: list[str] = []
    errors: list[str] = []

    for row in rows:
        props = row.get("properties", {})
        name = _prop_title(props, "Name")
        row_type = _prop_select(props, "Type")
        description = _prop_rich_text(props, "Description")
        page_id = row["id"]

        if not name:
            log.warning("work_sync: row %s has no Name — skipping", page_id)
            skipped.append(page_id)
            continue

        if row_type not in ("README", "Skill"):
            log.warning("work_sync: row '%s' has unknown Type '%s' — skipping", name, row_type)
            skipped.append(name)
            continue

        try:
            blocks = _fetch_block_tree(notion, page_id)
        except Exception as exc:
            log.error("work_sync: failed to fetch blocks for '%s': %s", name, exc)
            errors.append(name)
            continue

        body_lines = _blocks_to_md(blocks)
        body = "\n".join(body_lines).strip()

        if row_type == "README":
            rel_path = readme_filename(name)
            content = body + "\n"
            dest = out_dir / rel_path
        else:
            slug = name_to_slug(name)
            rel_path = f"skills/{slug}/SKILL.md"
            frontmatter = f"---\nname: {slug}\ndescription: {description}\n---\n\n"
            content = frontmatter + body + "\n"
            dest = out_dir / "skills" / slug / "SKILL.md"

        if dry_run:
            print(f"[dry-run] would write {dest}:\n{content[:200]}...")
            written.append(rel_path)
            continue

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content, encoding="utf-8")
            log.info("work_sync: wrote %s", dest)
            written.append(rel_path)
        except Exception as exc:
            log.error("work_sync: failed to write %s: %s", dest, exc)
            errors.append(rel_path)

    summary = {"written": written, "skipped": skipped, "errors": errors}
    log.info("work_sync: done — written=%d skipped=%d errors=%d", len(written), len(skipped), len(errors))
    return summary
