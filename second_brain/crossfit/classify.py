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


def classify_workout_message(text: str, claude_client, model: str, max_tokens: int) -> dict:
    # Fast-path: long text with day headings is always a programme
    if len(text) > 400 and any(day in text for day in DAY_NAMES):
        return {
            "type": "programme",
            "confidence": "high",
            "movement": None,
            "load_lbs": None,
            "load_kg": None,
            "sets": None,
            "reps": None,
            "is_max_attempt": False,
            "wod_name": None,
            "format": None,
            "duration_mins": None,
            "partner": False,
        }

    prompt = f'''You are a CrossFit workout classifier for a personal tracking bot.
Today is {_today_str()}.

Message: "{text}"

Classify into exactly ONE type:
STRENGTH ...
CONDITIONING ...
PROGRAMME — user pasting a full weekly gym programme.
  Signals: contains day headings (MONDAY, TUESDAY, WEDNESDAY etc. in any case),
  multiple sections (B., C., or Section B / Section C), structured workout
  descriptions. Also matches if text contains 2+ of: track headers (PERFORMANCE,
  FITNESS, HYROX), time blocks (00:00-15:00 format), rep schemes with sets×reps.
  A message over 300 characters containing day names and movement descriptions
  should be classified as PROGRAMME even if day count is unclear.
NONE ...
Return ONLY valid JSON with fields exactly as requested.'''
    try:
        resp = claude_client.messages.create(model=model, max_tokens=max_tokens, messages=[{"role": "user", "content": prompt}])
    except Exception as e:
        alert_claude_auth_failure(str(e))
        raise
    return _extract_json(resp.content[0].text)


def parse_programme(text: str, claude_client, model: str, max_tokens: int) -> dict:
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
