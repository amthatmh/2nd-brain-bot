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


def classify_workout_message(text: str, claude_client, model: str, max_tokens: int) -> dict:
    # Fast-path: long text with day headings is always a programme
    day_names = [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
        "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
    ]
    if len(text) > 500 and any(day in text for day in day_names):
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
    prompt = f'''You are parsing a CrossFit gym's weekly programme for a tracking bot.
Today is {_today_str()}.
Programme text:\n---\n{text}\n---
Return ONLY valid JSON in the requested schema, with week_label as Week of {_monday_str()}.'''
    resp = claude_client.messages.create(model=model, max_tokens=max_tokens, messages=[{"role": "user", "content": prompt}])
    return _extract_json(resp.content[0].text)
