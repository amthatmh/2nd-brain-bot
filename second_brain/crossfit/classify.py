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
    prompt = f'''You are a CrossFit workout classifier for a personal tracking bot.
Today is {_today_str()}.

Message: "{text}"

Classify into exactly ONE type:
STRENGTH ...
CONDITIONING ...
PROGRAMME ...
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
