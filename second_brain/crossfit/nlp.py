"""
Movement extraction and fuzzy matching for CrossFit logs.

This module turns raw workout text into canonical movement names and maps
those names to pages in the Movements database.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import anthropic

from utils.alert_handlers import alert_claude_auth_failure
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

        @staticmethod
        def token_set_ratio(a, b):
            a_tokens = set(str(a).lower().split())
            b_tokens = set(str(b).lower().split())
            if not a_tokens and not b_tokens:
                return 100.0
            if not a_tokens or not b_tokens:
                return 0.0
            common = len(a_tokens & b_tokens)
            return (2 * common / (len(a_tokens) + len(b_tokens))) * 100

from second_brain.ai.client import strip_json_fences
from second_brain.crossfit.utils import _app_tz
from second_brain.notion import notion_call

log = logging.getLogger(__name__)

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
EXPECTED_MOVEMENTS_DB_ID = "ecf5ac8381ce41a98fa804a1694977bb"
BENCHMARK_WODS = [
    "Fran", "Grace", "Helen", "Diane", "Elizabeth", "Karen", "Nancy", "Annie",
    "Cindy", "Mary", "Chelsea", "Jackie", "Kelly", "Linda", "Eva", "Amanda",
    "Barbara", "Murph", "DT", "Angie", "Fight Gone Bad", "The Seven", "Nicole",
]


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value



def _empty_workout_data(movements: Optional[List[str]] = None) -> Dict:
    """Return the canonical workout extraction shape with optional movements."""
    return {
        "movements": movements or [],
        "date": None,
        "sets": None,
        "reps": None,
        "weight_lbs": None,
        "weight_kg": None,
        "scheme": None,
        "notes": None,
        "workout_structure": None,
        "raw_input": None,
        "wod_name": None,
    }


def _detect_benchmark_wod(log_message: str) -> Optional[str]:
    """Detect named benchmark WODs in the user's original text."""
    text = log_message or ""
    for wod_name in BENCHMARK_WODS:
        pattern = r"(?<![A-Za-z0-9])" + re.escape(wod_name.lower()) + r"(?![A-Za-z0-9])"
        if re.search(pattern, text.lower()):
            return wod_name
    return None


def _coerce_number(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _normalise_workout_data(parsed, fallback_message: str) -> Dict:
    """Validate Claude output and keep a stable schema for callers."""
    if isinstance(parsed, list):
        data = _empty_workout_data([str(m).strip() for m in parsed if str(m).strip()])
    elif isinstance(parsed, dict):
        movements = parsed.get("movements") or []
        if isinstance(movements, str):
            movements = [movements]
        data = _empty_workout_data([str(m).strip() for m in movements if str(m).strip()])
        data.update({k: parsed.get(k) for k in data.keys() if k != "movements"})
    else:
        raise ValueError("workout extraction did not return a JSON object")

    if not data["movements"] and fallback_message:
        data["movements"] = [fallback_message.strip()]
    for key in ("sets", "reps"):
        number = _coerce_number(data.get(key))
        data[key] = int(number) if number is not None else None
    for key in ("weight_lbs", "weight_kg"):
        number = _coerce_number(data.get(key))
        data[key] = round(float(number), 1) if number is not None else None
    for key in ("date", "scheme", "notes", "workout_structure", "raw_input", "wod_name"):
        value = data.get(key)
        data[key] = str(value).strip() if value not in (None, "") else None
    data["raw_input"] = data.get("raw_input") or (fallback_message.strip() if fallback_message else None)
    data["workout_structure"] = data.get("workout_structure") or data["raw_input"]
    data["wod_name"] = data.get("wod_name") or _detect_benchmark_wod(fallback_message)
    return data


def _fallback_extract_workout_data(log_message: str, current_date: datetime) -> Dict:
    """Small deterministic fallback for common metadata when Claude is unavailable."""
    text = (log_message or "").strip()
    data = _empty_workout_data([text] if text else [])
    lower = text.lower()

    sets_reps_pattern = re.search(
        r"\b(\d+)\s+sets?\s+(?:of\s+)?(\d+)\s*(?:x|reps?)?\b",
        lower,
        re.IGNORECASE,
    )
    scheme_pattern = re.search(r"\b(\d+)\s*[x×]\s*(\d+)\b", lower, re.IGNORECASE)
    if sets_reps_pattern:
        data["sets"] = int(sets_reps_pattern.group(1))
        data["reps"] = int(sets_reps_pattern.group(2))
        data["scheme"] = f"{data['sets']}x{data['reps']}"
        log.debug("Fallback sets/reps: %sx%s", data['sets'], data['reps'])
    elif scheme_pattern:
        data["sets"] = int(scheme_pattern.group(1))
        data["reps"] = int(scheme_pattern.group(2))
        data["scheme"] = f"{data['sets']}x{data['reps']}"
        log.debug("Fallback sets/reps: %sx%s", data['sets'], data['reps'])
    else:
        rounds = re.search(r"\b(\d+)\s+rounds?\b", lower, re.IGNORECASE)
        if rounds:
            data["sets"] = int(rounds.group(1))
            data["scheme"] = f"{data['sets']} rounds"
            log.debug("Fallback rounds: %s", data['sets'])

    weight_pattern = re.search(
        r"\b(\d+(?:\.\d+)?)\s*(lbs?|pounds?|kg|#)(?:\b|\s|$)",
        lower,
        re.IGNORECASE,
    )
    if weight_pattern:
        amount = float(weight_pattern.group(1))
        unit = weight_pattern.group(2).lower()
        if unit == "kg":
            data["weight_kg"] = round(amount, 1)
            data["weight_lbs"] = round(amount / 0.453592, 1)
        else:
            data["weight_lbs"] = round(amount, 1)
            data["weight_kg"] = round(amount * 0.453592, 1)
        log.debug("Fallback weight: %slbs", data['weight_lbs'])

    if re.search(r"\byesterday\b", lower, re.IGNORECASE):
        data["date"] = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
        log.debug("Fallback date: %s", data['date'])
    else:
        date_pattern = re.search(
            r"\bon\s+(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b",
            lower,
            re.IGNORECASE,
        )
        if date_pattern:
            month = int(date_pattern.group(1))
            day = int(date_pattern.group(2))
            year = int(date_pattern.group(3)) if date_pattern.group(3) else current_date.year
            if year < 100:
                year += 2000
            try:
                data["date"] = datetime(year, month, day).strftime("%Y-%m-%d")
                log.debug("Fallback date: %s", data['date'])
            except ValueError:
                data["date"] = None

    data["raw_input"] = text or None
    data["workout_structure"] = text or None
    data["wod_name"] = _detect_benchmark_wod(text)
    if text and "," in text:
        movements = []
        for part in text.split(","):
            cleaned = re.sub(r"^\s*\d+\s*(?:[x×]|reps?|cal(?:ories)?s?)?\s*", "", part, flags=re.I)
            cleaned = cleaned.strip(" -")
            if cleaned:
                movements.append(cleaned)
        if movements:
            data["movements"] = movements
    return data


def _apply_fallback_workout_metadata(data: Dict, fallback: Dict) -> Dict:
    """Fill missing Claude metadata with deterministic regex results."""
    for key in ("date", "sets", "reps", "scheme"):
        if data.get(key) is None and fallback.get(key) is not None:
            data[key] = fallback[key]
    for key in ("weight_lbs", "weight_kg"):
        if data.get(key) in (None, 0) and fallback.get(key) is not None:
            data[key] = fallback[key]
    return data


async def extract_workout_data(
    log_message: str,
    claude_client: anthropic.Anthropic,
    current_date: Optional[datetime] = None,
) -> Dict:
    """
    Extract complete workout data from a natural-language CrossFit log.

    Returns movements plus optional date, sets, reps, weight, scheme, and notes.
    If Claude is unavailable or malformed, a deterministic fallback extracts the
    most common date/scheme/load patterns and preserves the raw text as movement.
    """
    if current_date is None:
        current_date = datetime.now(_app_tz())
    if not log_message:
        return _empty_workout_data()
    if claude_client is None:
        return _fallback_extract_workout_data(log_message, current_date)

    system_prompt = f"""You are a CrossFit workout log extraction expert. Extract ALL workout details from natural language messages.

TODAY'S DATE: {current_date.strftime('%Y-%m-%d')} ({current_date.strftime('%A, %B %d, %Y')})

EXTRACTION RULES:

1. MOVEMENTS: Extract canonical movement names only; remove sets/reps/weight/date words.
   - "4x hang clean squat at 115lb" -> "Hang Clean"
   - "6 sets of 4x hang clean squat" -> "Hang Clean"
   - Standardize "hang squat clean" and "hang clean squat" -> "Hang Squat Clean"
   - When user says "hang clean" or "hang cleans" without specifying variation, default to "Hang Power Clean".
   - Only return "Hang Squat Clean" if the user explicitly says "squat".

2. DATE: Parse date references relative to TODAY.
   - "on 5/6" -> use the current year unless another year is stated
   - "yesterday" -> subtract 1 day from TODAY
   - "last Monday" -> most recent Monday before TODAY
   - "Tuesday" -> most recent Tuesday on or before TODAY
   - If no date is mentioned -> null

3. SETS/REPS/SCHEME:
   - "6 sets of 4x" -> sets 6, reps 4, scheme "6x4"
   - "5x5" -> sets 5, reps 5, scheme "5x5"
   - "3 rounds" -> sets 3, reps null, scheme "3 rounds"

4. WEIGHT: Extract load and convert both directions using 1 lb = 0.453592 kg.
   - "115lbs" or "225#" are pounds
   - "100kg" is kilograms
   - Round converted weights to 1 decimal.

5. NOTES: Additional context that is not movement/date/scheme/load.

OUTPUT FORMAT: Valid JSON object only, no explanation:
{{
  "movements": ["Movement 1"],
  "date": "YYYY-MM-DD" or null,
  "sets": integer or null,
  "reps": integer or null,
  "weight_lbs": float or null,
  "weight_kg": float or null,
  "scheme": "string" or null,
  "notes": "string" or null,
  "workout_structure": "original user input with rep scheme preserved" or null,
  "raw_input": "original user input" or null,
  "wod_name": "benchmark WOD name such as Fran" or null
}}

WORKOUT STRUCTURE PRESERVATION:
- Preserve the user's original movement/rep scheme exactly in workout_structure.
- Do not strip rep counts from workout_structure; keep text such as "3x Wall walks, 6 hang cleans, 9 burpee over bar, 12 v-ups".
- Keep movements canonical and separate from workout_structure.
- Only populate wod_name when the user mentions a benchmark WOD name (for example Fran, Grace, Cindy, Murph, DT, Fight Gone Bad). Otherwise use null.

EXAMPLE for TODAY {current_date.strftime('%Y-%m-%d')}:
Input: "Did 6 sets of 4x hang squat clean at 115lbs on 5/6"
Output: {{"movements":["Hang Squat Clean"],"date":"{current_date.year}-05-06","sets":6,"reps":4,"weight_lbs":115.0,"weight_kg":52.2,"scheme":"6x4","notes":null,"workout_structure":"Did 6 sets of 4x hang squat clean at 115lbs on 5/6","raw_input":"Did 6 sets of 4x hang squat clean at 115lbs on 5/6","wod_name":null}}

Input: "3x Wall walks, 6 hang cleans, 9 burpees, 12 v-ups"
Output: {{"movements":["Wall Walks","Hang Power Clean","Burpee","V-Up"],"date":null,"sets":null,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":null,"notes":null,"workout_structure":"3x Wall walks, 6 hang cleans, 9 burpees, 12 v-ups","raw_input":"3x Wall walks, 6 hang cleans, 9 burpees, 12 v-ups","wod_name":null}}
"""

    try:
        response = await _maybe_await(
            claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1000,
                system=system_prompt,
                messages=[{"role": "user", "content": f"Extract workout data from: {log_message}"}],
            )
        )
        workout_data_text = strip_json_fences(response.content[0].text)
        parsed = json.loads(workout_data_text)
        workout_data = _normalise_workout_data(parsed, log_message)
        fallback_data = _fallback_extract_workout_data(log_message, current_date)
        workout_data = _apply_fallback_workout_metadata(workout_data, fallback_data)
        log.debug("Extracted workout data: %r", workout_data)
        return workout_data
    except Exception as e:
        alert_claude_auth_failure(str(e))
        log.error("Workout data extraction failed: %s", e)
        return _fallback_extract_workout_data(log_message, current_date)


async def extract_movements_from_log(
    log_message: str,
    claude_client: anthropic.Anthropic,
) -> List[str]:
    """
    DEPRECATED: Use extract_workout_data() instead.

    This wrapper preserves the old movements-only API while using the enhanced
    extractor so dates, sets/reps, and loads are stripped before fuzzy matching.
    """
    workout_data = await extract_workout_data(log_message, claude_client)
    return workout_data.get("movements", [])

def normalize_movement_name(name: str) -> str:
    """Normalize movement name for fuzzy matching.

    Removes parenthetical weights, punctuation, duplicate whitespace, and simple
    plural suffixes while keeping the token order intact for matching.
    """
    original = str(name or "")
    normalized = re.sub(r"\([^)]*\)", " ", original)
    normalized = re.sub(r"[^a-zA-Z0-9]+", " ", normalized).strip().lower()
    words = []
    for word in normalized.split():
        if word.endswith("s") and len(word) > 3:
            word = word[:-1]
        words.append(word)
    normalized = " ".join(words)
    log.debug("Normalized %r → %r", original, normalized)
    return normalized


def _normalise_movement_for_match(name: str) -> str:
    """Return a comparison-friendly movement name for fuzzy matching."""
    return normalize_movement_name(name)


def _movement_match_score(normalized_input: str, normalized_candidate: str) -> float:
    """Score a normalized movement candidate with token-overlap safeguards."""
    if not normalized_input or not normalized_candidate:
        return 0.0
    base_score = fuzz.token_set_ratio(normalized_input, normalized_candidate) / 100.0
    input_tokens = set(normalized_input.split())
    candidate_tokens = set(normalized_candidate.split())
    if not input_tokens:
        return base_score

    # Reward candidates that preserve more of the extracted movement. This makes
    # "Hang Clean" prefer "Hang Squat Clean" over "Sandbag Clean" because the
    # former contains both key input tokens while the latter only contains Clean.
    input_coverage = len(input_tokens & candidate_tokens) / len(input_tokens)
    score = (base_score * 0.70) + (input_coverage * 0.30)
    if normalized_candidate.startswith(normalized_input):
        score += 0.03
    return min(score, 1.0)


async def fuzzy_match_movements(
    extracted_movements: List[str],
    movements_db_cache: Dict[str, str],
    threshold: float = 0.70,
) -> List[Tuple[str, Optional[str], float]]:
    """
    Fuzzy match extracted movement names against the Movements DB cache.

    When several candidates score effectively the same, prefer the shortest
    normalized movement name. For ambiguous "Hang Clean" inputs with no exact
    alias row, prefer "Hang Power Clean" over "Hang Squat Clean" unless the
    input explicitly includes "squat".
    """
    matched_results: List[Tuple[str, Optional[str], float]] = []
    normalized_cache: Dict[str, Tuple[str, str]] = {}
    for name, page_id in movements_db_cache.items():
        normalized = normalize_movement_name(name)
        if normalized and normalized not in normalized_cache:
            normalized_cache[normalized] = (name, page_id)

    log.debug("Normalized cache has %d entries", len(normalized_cache))
    log.debug("Sample normalized movements: %s", list(normalized_cache.keys())[:5])

    for movement in extracted_movements:
        movement = (movement or "").strip()
        if not movement:
            continue
        if not normalized_cache:
            matched_results.append((movement, None, 0.0))
            continue

        normalized_input = normalize_movement_name(movement)
        log.debug("Matching %r against cache...", normalized_input)
        scored_candidates = []
        for normalized_candidate in normalized_cache:
            score = _movement_match_score(normalized_input, normalized_candidate)
            if score >= threshold:
                scored_candidates.append((normalized_candidate, score))

        if not scored_candidates:
            matched_results.append((movement, None, 0.0))
            continue

        scored_candidates.sort(key=lambda item: item[1], reverse=True)
        best_score = scored_candidates[0][1]
        tied_candidates = [item for item in scored_candidates if abs(item[1] - best_score) < 0.05]

        if len(tied_candidates) > 1:
            def tie_break_key(item):
                normalized_candidate, _score = item
                hang_power_default = (
                    normalized_input == "hang clean"
                    and normalized_candidate == "hang power clean"
                    and "squat" not in normalized_input.split()
                )
                return (0 if hang_power_default else 1, len(normalized_candidate), normalized_candidate)

            tied_candidates.sort(key=tie_break_key)
            best_normalized_name, best_score = tied_candidates[0]
            log.debug("Multiple tied matches for %r, preferring: %r", movement, best_normalized_name)
        else:
            best_normalized_name, best_score = scored_candidates[0]

        original_name, _url = normalized_cache[best_normalized_name]
        log.debug("Best match: %r (score: %.2f)", original_name, best_score)
        matched_results.append((movement, original_name, best_score))

    return matched_results


async def load_movements_cache(notion_client, movements_db_id: Optional[str] = None) -> Dict[str, str]:
    """
    Load all movements from the Movements DB into an in-memory cache.

    Returns a mapping of movement name -> Notion page ID. Page IDs are used
    instead of page URLs because Notion relation properties require IDs.
    """
    movements_db_id = (movements_db_id or os.getenv("NOTION_MOVEMENTS_DB") or EXPECTED_MOVEMENTS_DB_ID).strip()
    log.debug("Loading movements from DB: %s", movements_db_id)
    if movements_db_id != EXPECTED_MOVEMENTS_DB_ID:
        log.error("Wrong movements DB! Expected %s, got %s", EXPECTED_MOVEMENTS_DB_ID, movements_db_id)
    if not movements_db_id or notion_client is None:
        return {}

    cache: Dict[str, str] = {}
    start_cursor = None
    while True:
        query_kwargs = {"database_id": movements_db_id, "page_size": 100}
        if start_cursor:
            query_kwargs["start_cursor"] = start_cursor
        results = await _maybe_await(notion_call(notion_client.databases.query, **query_kwargs))
        for page in results.get("results", []):
            title = page.get("properties", {}).get("Name", {}).get("title", [])
            if not title:
                continue
            name = title[0].get("plain_text", "").strip()
            if name:
                cache[name] = page.get("id") or page.get("url", "")
        if not results.get("has_more"):
            break
        start_cursor = results.get("next_cursor")
        if not start_cursor:
            break

    log.debug("Loaded %d movements into cache", len(cache))
    log.debug("Sample movements: %s", list(cache.keys())[:5])
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
