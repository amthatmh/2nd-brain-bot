"""
Movement extraction and fuzzy matching for CrossFit logs.

This module turns raw workout text into canonical movement names and maps
those names to pages in the Movements database.
"""

from __future__ import annotations

import inspect
import json
import os
import re
from typing import Dict, List, Optional, Tuple

import anthropic
try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - fallback for minimal test envs
    from difflib import SequenceMatcher

    class fuzz:  # type: ignore[no-redef]
        @staticmethod
        def token_sort_ratio(a, b):
            a_tokens = " ".join(sorted(str(a).lower().split()))
            b_tokens = " ".join(sorted(str(b).lower().split()))
            return SequenceMatcher(None, a_tokens, b_tokens).ratio() * 100

from second_brain.notion import notion_call

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _strip_json_fence(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("\n", 1)[0]
    return text.strip()


async def extract_movements_from_log(
    log_message: str,
    claude_client: anthropic.Anthropic,
) -> List[str]:
    """
    Extract canonical movement names from a workout log message.

    Examples:
    - "4xHang squat and clean, did 6 sets at 115lb" -> ["Hang Clean"]
    - "Wall Walks, 6 Hang Cleans (115/85), 9 Burpees Over Bar, 12 V-Ups"
      -> ["Wall Walks", "Hang Clean", "Burpee Over Bar", "V-Up"]

    Returns a list of canonical movement names stripped of sets, reps, and
    weight details. If Claude is unavailable or returns malformed JSON, the
    original message is returned as a conservative fallback.
    """
    if not log_message:
        return []
    if claude_client is None:
        return [log_message.strip()]

    system_prompt = """You are a CrossFit movement extraction expert. Extract canonical movement names from workout logs.

RULES:
1. Remove ALL sets/reps/weight indicators (4x, 115lb, 6 sets, etc.)
2. Standardize variants to canonical forms:
   - "hang squat clean" -> "Hang Clean"
   - "burpee over bar" -> "Burpee Over Bar"
   - "toes to bar" -> "Toes to Bar"
3. For compound descriptions, extract each movement separately
4. Return ONLY movement names, no explanations

OUTPUT FORMAT: JSON array of strings
Example: ["Wall Walks", "Hang Clean", "Burpee Over Bar", "V-Up"]
"""

    try:
        response = await _maybe_await(
            claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=500,
                system=system_prompt,
                messages=[{"role": "user", "content": f"Extract movements from: {log_message}"}],
            )
        )
        movements_text = _strip_json_fence(response.content[0].text)
        movements = json.loads(movements_text)
        if not isinstance(movements, list):
            raise ValueError("movement extraction did not return a JSON list")
        return [str(m).strip() for m in movements if str(m).strip()]
    except Exception as e:
        print(f"Movement extraction error: {e}")
        return [log_message.strip()]


def _normalise_movement_for_match(name: str) -> str:
    """Return a comparison-friendly movement name for fuzzy matching."""
    normalised = re.sub(r"\([^)]*\)", " ", str(name or ""))
    normalised = re.sub(r"[^a-zA-Z0-9]+", " ", normalised).strip().lower()
    tokens = []
    for token in normalised.split():
        if len(token) > 3 and token.endswith("s"):
            token = token[:-1]
        tokens.append(token)
    return " ".join(tokens)


async def fuzzy_match_movements(
    extracted_movements: List[str],
    movements_db_cache: Dict[str, str],
    threshold: float = 0.70,
) -> List[Tuple[str, Optional[str], float]]:
    """
    Fuzzy match extracted movement names against the Movements DB cache.

    Args:
        extracted_movements: canonical names returned from NLP.
        movements_db_cache: mapping of movement name -> Notion page ID.
        threshold: documented decision threshold; all best scores are returned
            so callers can decide whether to auto-link, confirm, or create.

    Returns:
        Tuples of (extracted_name, matched_name, score). Scores are 0.0-1.0.
    """
    del threshold  # callers use score bands; retain arg for API clarity.
    matched_results: List[Tuple[str, Optional[str], float]] = []
    movement_names = list(movements_db_cache.keys())

    for movement in extracted_movements:
        movement = (movement or "").strip()
        if not movement:
            continue
        if not movement_names:
            matched_results.append((movement, None, 0.0))
            continue

        normalised_movement = _normalise_movement_for_match(movement)
        best_name: Optional[str] = None
        best_score = 0.0
        for candidate in movement_names:
            raw_score = fuzz.token_sort_ratio(movement, candidate) / 100.0
            normalised_score = fuzz.token_sort_ratio(
                normalised_movement,
                _normalise_movement_for_match(candidate),
            ) / 100.0
            score = max(raw_score, normalised_score)
            if score > best_score:
                best_name = candidate
                best_score = score

        matched_results.append((movement, best_name, best_score))

    return matched_results


async def load_movements_cache(notion_client, movements_db_id: Optional[str] = None) -> Dict[str, str]:
    """
    Load all movements from the Movements DB into an in-memory cache.

    Returns a mapping of movement name -> Notion page ID. Page IDs are used
    instead of page URLs because Notion relation properties require IDs.
    """
    movements_db_id = movements_db_id or os.getenv("NOTION_MOVEMENTS_DB", "")
    if not movements_db_id or notion_client is None:
        return {}

    results = await _maybe_await(
        notion_call(notion_client.databases.query, database_id=movements_db_id, page_size=100)
    )
    cache: Dict[str, str] = {}
    for page in results.get("results", []):
        title = page.get("properties", {}).get("Name", {}).get("title", [])
        if not title:
            continue
        name = title[0].get("plain_text", "").strip()
        if name:
            cache[name] = page.get("id") or page.get("url", "")
    return cache


# TESTING CHECKLIST — Phase 1 Movement Extraction
# [ ] Test extract_movements_from_log with:
#     - "4xHang squat and clean, did 6 sets at 115lb" -> ["Hang Clean"]
#     - "Wall Walks, Hang Cleans, Burpees" -> ["Wall Walks", "Hang Clean", "Burpee Over Bar"]
# [ ] Test fuzzy_match_movements with:
#     - "Hang Clean" -> score >0.90 (auto-link)
#     - "Wall Walk" -> score 0.70-0.90 (confirm "Wall Walks")
# [ ] Test load_movements_cache returns Dict with >20 movements
# [ ] Verify movements_db_cache persists in bot startup (global variable)
