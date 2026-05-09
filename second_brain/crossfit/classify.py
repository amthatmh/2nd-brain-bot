from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone

from utils.alert_handlers import alert_claude_auth_failure

log = logging.getLogger(__name__)


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _monday_str() -> str:
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()


def _extract_json(raw: str) -> dict:
    """Parse JSON from Claude response, with truncation recovery."""
    text = re.sub(r"```(?:json)?|```", "", raw).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    try:
        last_complete = text.rfind('}\n    ]')
        if last_complete == -1:
            last_complete = text.rfind('},\n      {')
        if last_complete == -1:
            last_complete = text.rfind('"}')

        if last_complete > 0:
            truncated = text[:last_complete + 2]
            open_braces = truncated.count('{') - truncated.count('}')
            open_brackets = truncated.count('[') - truncated.count(']')
            truncated += '}' * max(0, open_braces)
            truncated += ']' * max(0, open_brackets)
            try:
                result = json.loads(truncated)
                log.warning("_extract_json: recovered partial JSON (%d chars truncated)", len(text) - len(truncated))
                return result
            except json.JSONDecodeError:
                pass
    except Exception:
        pass

    raise json.JSONDecodeError(
        f"Could not parse JSON response (length={len(text)}). This likely means max_tokens is too low — increase CLAUDE_PARSE_MAX_TOKENS.",
        text,
        0,
    )


DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
]

TRACK_NAMES = {"PERFORMANCE": "Performance", "FITNESS": "Fitness", "HYROX": "Hyrox"}
DAY_CANONICAL = {
    "MONDAY": "Monday",
    "TUESDAY": "Tuesday",
    "WEDNESDAY": "Wednesday",
    "THURSDAY": "Thursday",
    "FRIDAY": "Friday",
    "SATURDAY": "Saturday",
    "SUNDAY": "Sunday",
}
DAY_HEADER_RE = re.compile(r"(?im)^[ \t]*(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)[ \t]*:?.*$")
TRACK_HEADER_RE = re.compile(r"(?im)^[ \t]*(PERFORMANCE|FITNESS|HYROX)[ \t]*:?.*$")
SECTION_HEADER_RE = re.compile(r"(?im)^[ \t]*(?:SECTION[ \t]*)?([BC])[ \t]*[\.:\)-]?[ \t]*(.*)$")


def _split_by_headers(text: str, header_re: re.Pattern) -> list[tuple[str, str]]:
    matches = list(header_re.finditer(text or ""))
    sections: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        sections.append((match.group(1).upper(), text[start:end].strip()))
    return sections


def _extract_section_text(block: str, section_letter: str) -> str:
    matches = list(SECTION_HEADER_RE.finditer(block or ""))
    for idx, match in enumerate(matches):
        if match.group(1).upper() != section_letter:
            continue
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(block)
        prefix = (match.group(2) or "").strip()
        rest = block[start:end].strip() if start < end else ""
        return "\n".join(part for part in [prefix, rest] if part).strip()
    return ""


def _infer_section_c_format(description: str) -> str | None:
    text = (description or "").lower()
    if "amrap" in text:
        return "AMRAP"
    if "for time" in text or "time cap" in text:
        return "For Time"
    if "emom" in text or "every minute" in text:
        return "EMOM"
    if "interval" in text or re.search(r"\bevery\s+\d+", text):
        return "Intervals"
    if "chipper" in text:
        return "Chipper"
    return None


def _extract_time_cap(description: str) -> int | None:
    match = re.search(r"\b(\d+)\s*(?:min|mins|minute|minutes)\b", description or "", re.I)
    return int(match.group(1)) if match else None


def _extract_candidate_movements(description: str) -> list[str]:
    """Small deterministic movement-name fallback for programme parsing.

    The save path augments these names from the Movements DB cache, so this only
    needs to preserve obvious phrases when Claude is skipped or truncates output.
    """
    text = re.sub(r"\([^)]*\)", " ", description or "")
    text = re.sub(r"\b(?:amrap|emom|for time|time cap|every|rounds?|reps?|cal(?:ories)?|minutes?|mins?)\b", " ", text, flags=re.I)
    pieces = re.split(r"[,;/\n]|\d+\s*(?:x|×|-|to)\s*\d+|\d+", text)
    out: list[str] = []
    seen: set[str] = set()
    for piece in pieces:
        cleaned = re.sub(r"[^A-Za-z\- ]+", " ", piece).strip(" -")
        cleaned = re.sub(r"\s+", " ", cleaned)
        words = [w for w in cleaned.split() if len(w) > 1]
        if not words or len(" ".join(words)) < 4:
            continue
        name = " ".join(words).title()
        key = name.lower()
        if key not in seen:
            seen.add(key)
            out.append(name)
    return out[:12]


def parse_programme_text(text: str) -> dict | None:
    """Deterministically parse programme text split by day and track headers."""
    day_sections = _split_by_headers(text or "", DAY_HEADER_RE)
    if not day_sections:
        return None

    tracks: dict[str, list[dict]] = {}
    for raw_day, day_block in day_sections:
        day = DAY_CANONICAL[raw_day]
        track_sections = _split_by_headers(day_block, TRACK_HEADER_RE)
        if not track_sections:
            # Preserve older single-track programmes while still honoring days.
            track_sections = [("PERFORMANCE", day_block)]
        for raw_track, track_block in track_sections:
            track = TRACK_NAMES[raw_track]
            section_b_text = _extract_section_text(track_block, "B")
            section_c_text = _extract_section_text(track_block, "C")
            if not section_b_text and not section_c_text:
                continue
            section_b = None
            if section_b_text:
                section_b = {
                    "description": section_b_text,
                    "movements": _extract_candidate_movements(section_b_text),
                    "is_strength_test": bool(re.search(r"\b(?:1rm|3rm|5rm|max|test)\b", section_b_text, re.I)),
                    "rep_scheme": None,
                }
            section_c = None
            if section_c_text:
                section_c = {
                    "description": section_c_text,
                    "format": _infer_section_c_format(section_c_text),
                    "duration_mins": _extract_time_cap(section_c_text) if _infer_section_c_format(section_c_text) in {"AMRAP", "EMOM", "Intervals"} else None,
                    "time_cap_mins": _extract_time_cap(section_c_text) if _infer_section_c_format(section_c_text) == "For Time" else None,
                    "movements": _extract_candidate_movements(section_c_text),
                    "is_partner": bool(re.search(r"\bpartner\b", section_c_text, re.I)),
                    "wod_name": None,
                }
            tracks.setdefault(track, []).append({
                "day": day,
                "section_b": section_b,
                "section_c": section_c,
                "training_notes": "",
            })

    if not tracks:
        return None
    monday = _monday_str()
    return {
        "week_label": f"Week of {monday}",
        "tracks": [{"track": track, "days": days} for track, days in tracks.items()],
    }


def classify_workout_message(text: str, claude_client, model: str, max_tokens: int) -> dict:
    # Fast-path: long text with day headings is always a programme
    if len(text) > 400 and any(day in text for day in DAY_NAMES):
        return {
            "type": "programme",
            "confidence": "high",
            "movement": None,
            "movements": [],
            "load_lbs": None,
            "load_kg": None,
            "sets": None,
            "reps": None,
            "is_max_attempt": False,
            "wod_name": None,
            "format": None,
            "duration_mins": None,
            "partner": False,
            "raw_text": text,
        }

    prompt = f'''You are a CrossFit workout classifier for a personal tracking bot.
Today is {_today_str()}.

Message: "{text}"

Classify into exactly ONE type:
STRENGTH — strength lifting, skill, or accessory work. Return a single "movement" when identifiable.
CONDITIONING — for WODs, metcons, AMRAPs, For Time workouts.
  Extract ALL movements mentioned (e.g. burpees, wall balls, pull-ups).
  Return as "movements" array. Empty array if none identifiable.
PROGRAMME — user pasting a full weekly gym programme.
  Signals: contains day headings (MONDAY, TUESDAY, WEDNESDAY etc. in any case),
  multiple sections (B., C., or Section B / Section C), structured workout
  descriptions. Also matches if text contains 2+ of: track headers (PERFORMANCE,
  FITNESS, HYROX), time blocks (00:00-15:00 format), rep schemes with sets×reps.
  A message over 300 characters containing day names and movement descriptions
  should be classified as PROGRAMME even if day count is unclear.
NONE ...

Return ONLY valid JSON with fields exactly as requested:
{{
  "type": "strength|conditioning|programme|none",
  "confidence": "low|medium|high",
  "movement": "single strength movement name or null",
  "movements": ["list", "of", "movement", "names"],
  "load_lbs": null,
  "load_kg": null,
  "sets": null,
  "reps": null,
  "is_max_attempt": false,
  "wod_name": null,
  "format": "For Time|AMRAP|EMOM|Chipper|Intervals|null",
  "duration_mins": null,
  "partner": false
}}'''
    try:
        resp = claude_client.messages.create(model=model, max_tokens=max_tokens, messages=[{"role": "user", "content": prompt}])
    except Exception as e:
        alert_claude_auth_failure(str(e))
        raise
    result = _extract_json(resp.content[0].text)
    movements = result.get("movements") or []
    if isinstance(movements, str):
        movements = [movements]
    if not movements and result.get("movement"):
        movements = [result["movement"]]
    result["movements"] = movements
    result["raw_text"] = text
    return result


def parse_programme(text: str, claude_client, model: str, max_tokens: int) -> dict:
    deterministic = parse_programme_text(text)
    if deterministic:
        return deterministic

    today = _today_str()
    monday = _monday_str()

    prompt = f"""You are parsing a CrossFit gym's weekly programme.
Today: {today}. Week starts: {monday}.

Programme:
---
{text}
---

Extract ALL tracks (Performance, Fitness, Hyrox). For each track, each day with Section B or C.

Return ONLY valid JSON:
{{
  "week_label": "Week of {monday}",
  "tracks": [
    {{
      "track": "Performance",
      "days": [
        {{
          "day": "Monday",
          "section_b": {{"description": "...", "movements": ["..."], "is_strength_test": true/false, "rep_scheme": "..."}},
          "section_c": {{"description": "...", "format": "For Time|AMRAP|EMOM|Chipper|Intervals|Partner AMRAP", "duration_mins": null, "time_cap_mins": 15, "movements": ["..."], "is_partner": false, "wod_name": null}},
          "training_notes": "..."
        }}
      ]
    }}
  ]
}}

Rules:
- section_b or section_c can be null if not present for that day
- Omit tracks not in the programme
- Use null for unknown/missing number fields
- Thursday and Saturday are often partner WODs (is_partner: true)
- Keep descriptions concise — movement names + rep counts only, skip coaching notes
"""
    try:
        resp = claude_client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        alert_claude_auth_failure(str(e))
        raise
    result = _extract_json(resp.content[0].text)
    result["week_label"] = f"Week of {monday}"
    return result
