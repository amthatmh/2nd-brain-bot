from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import anthropic

from utils.alert_handlers import alert_claude_auth_failure


_JSON_FENCE_RE = re.compile(r"^```(?:json)?|```$", re.MULTILINE)


def _strip_markdown_json(text: str) -> str:
    return _JSON_FENCE_RE.sub("", text).strip()


async def claude_classify(
    client: anthropic.Anthropic,
    model: str,
    prompt: str,
    max_tokens: int,
    retries: int = 3,
) -> dict[str, Any]:
    delay = 0.75
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            msg = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                ),
            )
            text = msg.content[0].text if msg.content else "{}"
            return json.loads(_strip_markdown_json(text) or "{}")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            transient = any(token in str(exc).lower() for token in ("rate", "529", "timeout", "tempor"))
            if not transient or attempt == retries:
                alert_claude_auth_failure(str(exc))
                break
            await asyncio.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Claude classification failed after {retries} attempts: {last_error}")


def classify_message(
    claude: anthropic.Anthropic,
    claude_model: str,
    text: str,
    habit_names: list[str],
    watchlist_enabled: bool,
    wantslist_enabled: bool,
    photo_enabled: bool,
    notes_enabled: bool,
    today_local,
) -> dict[str, Any]:
    enabled_intents = ["habit", "entertainment_log", "task"]
    if watchlist_enabled:
        enabled_intents.append("watchlist")
    if wantslist_enabled:
        enabled_intents.append("wantslist")
    if photo_enabled:
        enabled_intents.append("photo")
    if notes_enabled:
        enabled_intents.append("note")

    prompt = f"""You are a personal assistant classifier for a second brain system.
Today is {today_local.strftime("%A, %B %-d, %Y")}.

Message: "{text}"

Active habits: {habit_names}
Workout types that count as 💪 Workout: soccer, snowboard, skiing, gym, run, jog, trained

Enabled intent types: {enabled_intents}

Classify this message into EXACTLY ONE intent. Rules:

WATCHLIST — user wants to watch a TV series, film, anime, or documentary in the future.
WANTSLIST — user wants to buy or acquire a physical product/item.
PHOTO — user wants to capture a photography scene/subject/location.
NOTE — user wants to save information/reference/thought without an action.
HABIT — user saying they completed a recurring habit RIGHT NOW.
ENTERTAINMENT_LOG — user logged media/event they watched or attended.
TASK — something to be done in the future (default if nothing else matches).

If confidence is low on watchlist/wantslist/photo, return task instead.
"Watch:" prefix = always watchlist, high confidence.
"want:" prefix = always wantslist, high confidence.
"photo:" prefix = always photo, high confidence.
"note:" prefix = always note, high confidence.
"idea:" prefix = always note, high confidence.
"code:" prefix = always note, high confidence.

Return ONLY valid JSON, no markdown:
If WATCHLIST: {{"type":"watchlist","title":"clean title only, no year","media_type":"Series|Film|Anime|Documentary","confidence":"high|low"}}
If WANTSLIST: {{"type":"wantslist","item":"clean item name","category":"Tech|Home|Clothes|Health|Other","confidence":"high|low"}}
If PHOTO: {{"type":"photo","subject":"clean scene/subject description","confidence":"high|low"}}
If NOTE: {{"type":"note","content":"clean note content","confidence":"high|low"}}
If HABIT: {{"type":"habit","habit_name":"exact name from {habit_names} or null","confidence":"high|low"}}
If ENTERTAINMENT_LOG: {{"type":"entertainment_log","log_type":"cinema|performance|sport","title":"extracted name of film/show/event","venue":"venue if mentioned, else null","date":"{today_local.isoformat()}","notes":"extra detail if mentioned, else null","favourite":false,"confidence":"high|low"}}
If TASK: {{"type":"task","task_name":"clean concise action","deadline_days":<integer or null>,"context":"one of: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab","confidence":"high|low","recurring":"None|🔁 Daily|📅 Weekly|🗓️ Monthly|📆 Quarterly","repeat_day":"Mon|Tue|Wed|Thu|Fri|Sat|Sun|1st..31st|Last or null"}}"""
    try:
        resp = claude.messages.create(
            model=claude_model,
            max_tokens=250,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        alert_claude_auth_failure(str(exc))
        raise
    raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
    return json.loads(raw)


def classify_note(
    claude: anthropic.Anthropic,
    claude_model: str,
    title: str,
    description: str,
    url: str,
    raw_text: str,
    topic_options: list[str],
) -> dict[str, Any]:
    context = title or description or raw_text or url
    prompt = f"""You are classifying a saved note/link for a second brain system.
Note context: "{context}"
URL: {url}
Available topics: {topic_options}
Return ONLY valid JSON, no markdown:
{{"title":"short descriptive title (max 80 chars, use the page title if good)","topics":["pick 1-2 most relevant topics from the list above"]}}"""
    try:
        resp = claude.messages.create(
            model=claude_model,
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        result = json.loads(raw)
        valid_topics = [t for t in result.get("topics", []) if t in topic_options]
        return {"title": result.get("title", title or url)[:200], "topics": valid_topics or ["💡 Ideas"]}
    except Exception as exc:
        alert_claude_auth_failure(str(exc))
        return {"title": title or url[:200], "topics": ["💡 Ideas"]}
