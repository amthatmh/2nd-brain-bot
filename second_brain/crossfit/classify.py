from __future__ import annotations

import json
import re
from datetime import datetime, timedelta


def _today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _monday_str() -> str:
    today = datetime.utcnow().date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()


def _extract_json(raw: str) -> dict:
    text = re.sub(r"```(?:json)?|```", "", raw).strip()
    return json.loads(text)


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
    resp = claude_client.messages.create(model=model, max_tokens=max_tokens, messages=[{"role": "user", "content": prompt}])
    return _extract_json(resp.content[0].text)


def parse_programme(text: str, claude_client, model: str, max_tokens: int) -> dict:
    schema = {
        "week_label": "Week of 2026-05-05",
        "tracks": [{
            "track": "Performance",
            "days": [{
                "day": "Monday",
                "section_b": {
                    "description": "full text",
                    "movements": ["Back Squat"],
                    "is_strength_test": True,
                    "rep_scheme": "1RM"
                },
                "section_c": {
                    "description": "full text",
                    "format": "For Time",
                    "duration_mins": None,
                    "time_cap_mins": 15,
                    "movements": ["Deadlift", "Power Clean", "Squat Clean"],
                    "is_partner": False,
                    "wod_name": None
                },
                "training_notes": "optional coaching notes text"
            }]
        }]
    }
    prompt = f"""You are parsing a CrossFit gym's weekly programme for a tracking bot.
Today is {_today_str()}. Week starts: {_monday_str()}.

Programme text:
---
{text}
---

Extract ALL tracks present (Performance, Fitness, Hyrox). Never discard a track.
For each track, extract every day that has a Section B or Section C.

Section B = strength/skill block (lifts, EMOM with load, max effort)
Section C = conditioning/metcon (For Time, AMRAP, chipper, partner WOD)
Training notes = coaching cues after the workout description

Return ONLY valid JSON matching this schema exactly. No markdown, no explanation.
Use null for missing fields. Omit tracks not present in the text.

{json.dumps(schema)}
"""
    resp = claude_client.messages.create(model=model, max_tokens=max_tokens, messages=[{"role": "user", "content": prompt}])
    return _extract_json(resp.content[0].text)
