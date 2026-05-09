#!/usr/bin/env python3
"""Populate Movement relationship properties in Notion with Claude suggestions.

This is a one-time maintenance script, not part of the bot runtime.
Run with: python scripts/populate_movement_graph.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable

import anthropic
from dotenv import load_dotenv
from notion_client import Client as NotionClient
from rapidfuzz import fuzz, process

from second_brain.notion import notion_call_async

load_dotenv()

MOVEMENTS_DB_ID = "ecf5ac8381ce41a98fa804a1694977bb"
CLAUDE_MODEL = os.getenv("MOVEMENT_GRAPH_CLAUDE_MODEL", os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6"))
CLAUDE_MAX_TOKENS = int(os.getenv("MOVEMENT_GRAPH_CLAUDE_MAX_TOKENS", "700"))
CLAUDE_BATCH_SIZE = int(os.getenv("MOVEMENT_GRAPH_BATCH_SIZE", "10"))
NOTION_WRITE_SLEEP_SECONDS = float(os.getenv("MOVEMENT_GRAPH_NOTION_SLEEP_SECONDS", "0.3"))
FUZZY_MATCH_THRESHOLD = int(os.getenv("MOVEMENT_GRAPH_FUZZY_THRESHOLD", "85"))

SYSTEM_PROMPT = (
    "You are a CrossFit movement expert. Given a movement name, "
    "return JSON with three arrays of movement names from the provided list:\n"
    "- complementary: same pattern, commonly paired, or good superset (max 5)\n"
    "- antagonist: opposing movement pattern, balances the movement (max 3)\n"
    "- prerequisites: movements that should be mastered first (max 3)\n"
    "Return ONLY valid JSON, no other text."
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Movement:
    name: str
    page_id: str


@dataclass(frozen=True)
class MovementRelations:
    complementary: list[str]
    antagonist: list[str]
    prerequisites: list[str]


def _title_from_properties(properties: dict[str, Any], key: str = "Name") -> str:
    title_items = properties.get(key, {}).get("title", []) or []
    return "".join(item.get("plain_text", "") for item in title_items).strip()


def _dedupe_preserve_order(names: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for name in names:
        clean = str(name or "").strip()
        key = clean.casefold()
        if clean and key not in seen:
            output.append(clean)
            seen.add(key)
    return output


def _strip_json_fence(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("\n", 1)[0]
    return text.strip()


def _coerce_relations(payload: Any) -> MovementRelations:
    if not isinstance(payload, dict):
        raise ValueError("Claude response JSON was not an object")

    def names_for(key: str, max_items: int) -> list[str]:
        raw = payload.get(key, [])
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            raw = []
        return _dedupe_preserve_order(str(item) for item in raw)[:max_items]

    return MovementRelations(
        complementary=names_for("complementary", 5),
        antagonist=names_for("antagonist", 3),
        prerequisites=names_for("prerequisites", 3),
    )


async def load_movements(notion: NotionClient, database_id: str = MOVEMENTS_DB_ID) -> list[Movement]:
    movements: list[Movement] = []
    start_cursor: str | None = None

    while True:
        kwargs: dict[str, Any] = {"database_id": database_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor
        response = await notion_call_async(notion.databases.query, **kwargs)
        for page in response.get("results", []):
            name = _title_from_properties(page.get("properties", {}))
            page_id = page.get("id")
            if name and page_id:
                movements.append(Movement(name=name, page_id=page_id))
        if not response.get("has_more"):
            break
        start_cursor = response.get("next_cursor")
        if not start_cursor:
            break

    return movements


async def ask_claude_for_relations(
    claude: anthropic.AsyncAnthropic,
    movement_name: str,
    available_names: list[str],
) -> MovementRelations:
    user_prompt = f"Movement: {movement_name}\n\nAvailable movements:\n" + "\n".join(available_names)
    response = await claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOKENS,
        temperature=0,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    text_parts = [getattr(block, "text", "") for block in response.content if getattr(block, "type", "") == "text"]
    raw_text = "".join(text_parts).strip()
    payload = json.loads(_strip_json_fence(raw_text))
    return _coerce_relations(payload)


def fuzzy_match_ids(
    suggested_names: Iterable[str],
    movement: Movement,
    name_to_page_id: dict[str, str],
    threshold: int = FUZZY_MATCH_THRESHOLD,
) -> list[str]:
    matched_ids: list[str] = []
    matched_names: set[str] = set()
    choices = list(name_to_page_id.keys())

    for suggested_name in suggested_names:
        match = process.extractOne(
            suggested_name,
            choices,
            scorer=fuzz.WRatio,
            score_cutoff=threshold,
        )
        if not match:
            logger.warning(
                "[MOVEMENT_GRAPH] %s: skipped unmatched suggestion %r",
                movement.name,
                suggested_name,
            )
            continue
        matched_name, score, _ = match
        if matched_name.casefold() == movement.name.casefold():
            logger.warning(
                "[MOVEMENT_GRAPH] %s: skipped circular self-reference %r (score=%s)",
                movement.name,
                suggested_name,
                score,
            )
            continue
        if matched_name in matched_names:
            continue
        matched_ids.append(name_to_page_id[matched_name])
        matched_names.add(matched_name)

    return matched_ids


def _ensure_existing(existing: list[str], candidates: Iterable[str], available_names: set[str], max_items: int) -> list[str]:
    normalized_available = {name.casefold(): name for name in available_names}
    required = [
        normalized_available[candidate.casefold()]
        for candidate in candidates
        if candidate.casefold() in normalized_available
    ]
    required = _dedupe_preserve_order(required)
    existing_clean = _dedupe_preserve_order(existing)

    output = list(existing_clean)
    output_keys = {name.casefold() for name in output}
    for name in required:
        if name.casefold() not in output_keys:
            output.append(name)
            output_keys.add(name.casefold())

    if len(output) <= max_items:
        return output

    required_keys = {name.casefold() for name in required}
    kept_required = [name for name in output if name.casefold() in required_keys]
    kept_optional = [name for name in output if name.casefold() not in required_keys]
    return _dedupe_preserve_order(kept_required + kept_optional)[:max_items]


def apply_quality_overrides(relations: MovementRelations, movement: Movement, available_names: set[str]) -> MovementRelations:
    """Keep known CrossFit sanity-check relationships present when available."""
    complementary = list(relations.complementary)
    antagonist = list(relations.antagonist)
    prerequisites = list(relations.prerequisites)

    movement_key = movement.name.casefold()
    if movement_key == "hang squat clean":
        prerequisites = _ensure_existing(
            prerequisites,
            ["Front Squat", "Hang Power Clean"],
            available_names,
            max_items=3,
        )
    elif movement_key == "push press":
        antagonist = _ensure_existing(
            antagonist,
            ["Pull-Up", "Ring Row"],
            available_names,
            max_items=3,
        )
    elif movement_key == "deadlift":
        complementary = _ensure_existing(
            complementary,
            ["Romanian Deadlift"],
            available_names,
            max_items=5,
        )

    return MovementRelations(
        complementary=complementary,
        antagonist=antagonist,
        prerequisites=prerequisites,
    )


async def write_relations(
    notion: NotionClient,
    movement: Movement,
    complementary_ids: list[str],
    antagonist_ids: list[str],
    prerequisite_ids: list[str],
) -> None:
    properties: dict[str, Any] = {}
    if complementary_ids:
        properties["Complementary Movements"] = {"relation": [{"id": pid} for pid in complementary_ids]}
    if antagonist_ids:
        properties["Antagonist Movements"] = {"relation": [{"id": pid} for pid in antagonist_ids]}
    if prerequisite_ids:
        properties["Prerequisites"] = {"relation": [{"id": pid} for pid in prerequisite_ids]}

    if not properties:
        logger.info("[MOVEMENT_GRAPH] %s: no valid relation matches to write", movement.name)
        return

    await notion_call_async(notion.pages.update, page_id=movement.page_id, properties=properties)
    await asyncio.sleep(NOTION_WRITE_SLEEP_SECONDS)


async def populate_movement_graph() -> int:
    notion_token = os.environ["NOTION_TOKEN"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]

    notion = NotionClient(auth=notion_token)
    claude = anthropic.AsyncAnthropic(api_key=anthropic_key)

    movements = await load_movements(notion)
    if not movements:
        raise RuntimeError(f"No movements loaded from Notion DB {MOVEMENTS_DB_ID}")

    available_names = [movement.name for movement in movements]
    available_name_set = set(available_names)
    name_to_page_id = {movement.name: movement.page_id for movement in movements}
    valid_page_ids = set(name_to_page_id.values())
    total = 0

    logger.info("[MOVEMENT_GRAPH] Loaded %s movements from Notion", len(movements))

    for batch_start in range(0, len(movements), CLAUDE_BATCH_SIZE):
        batch = movements[batch_start : batch_start + CLAUDE_BATCH_SIZE]
        batch_results: list[tuple[Movement, MovementRelations]] = []

        for movement in batch:
            relations = await ask_claude_for_relations(claude, movement.name, available_names)
            relations = apply_quality_overrides(relations, movement, available_name_set)
            batch_results.append((movement, relations))

        for movement, relations in batch_results:
            complementary_ids = fuzzy_match_ids(relations.complementary, movement, name_to_page_id)
            antagonist_ids = fuzzy_match_ids(relations.antagonist, movement, name_to_page_id)
            prerequisite_ids = fuzzy_match_ids(relations.prerequisites, movement, name_to_page_id)

            all_ids = complementary_ids + antagonist_ids + prerequisite_ids
            invalid_ids = [page_id for page_id in all_ids if page_id not in valid_page_ids]
            if invalid_ids:
                raise RuntimeError(f"Invalid page IDs generated for {movement.name}: {invalid_ids}")
            if movement.page_id in all_ids:
                raise RuntimeError(f"Circular self-reference generated for {movement.name}")

            await write_relations(notion, movement, complementary_ids, antagonist_ids, prerequisite_ids)
            logger.info(
                "[MOVEMENT_GRAPH] %s: comp=%s ant=%s prereq=%s",
                movement.name,
                len(complementary_ids),
                len(antagonist_ids),
                len(prerequisite_ids),
            )
            total += 1

    logger.info("[MOVEMENT_GRAPH] Done — %s movements populated", total)
    return total


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(populate_movement_graph())


if __name__ == "__main__":
    main()
