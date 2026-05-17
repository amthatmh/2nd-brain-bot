from __future__ import annotations

import asyncio
import json
from typing import Any

import anthropic

from second_brain.ai.client import strip_json_fences
from utils.alert_handlers import alert_claude_auth_failure


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
            return json.loads(strip_json_fences(text) or "{}")
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

CRITICAL SPLITTING RULES:
- Do NOT split on periods, commas, or newlines.
- ONLY split into multiple intents if you see explicit delimiters:
  * Uppercase AND keyword: "Task A AND Task B" → 2 tasks
  * Numbered list: "1. Task 2. Task" → 2 tasks
  * Bullet list: "• Task • Task" → 2 tasks
- If there are NO explicit delimiters, treat as ONE task (even if multiple sentences).
- For single tasks, extract metadata (due date, labels) from the full text.

Examples:
- "Add work task: Send Stephen Door drop information. Due today" → 1 TASK (due_date: today)
- "Send Stephen door drop info AND schedule meeting AND review proposal" → 3 TASKS (warn user to confirm)
- "Task 1. Send report 2. Schedule call" → 2 TASKS

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
If ENTERTAINMENT_LOG: {{"type":"entertainment_log","log_type":"cinema|performance|sport","title":"extracted name of film/show/event","venue":"venue if mentioned, else null","date":"{today_local.isoformat()}","notes":"extra detail if mentioned, else null","favourite":<true or false>,"confidence":"high|low"}}
For TASK, also detect recurring task patterns and extract:
- recurring_type: "daily", "weekly", "monthly", or null if not recurring
- repeat_day: Specific day/date if mentioned (e.g., "Mon", "1st", "15th", "Last")

Examples:
- "Review LEED docs every Monday" → recurring_type="weekly", repeat_day="Mon"
- "Pay rent on the 1st" → recurring_type="monthly", repeat_day="1st"
- "Water plants daily" → recurring_type="daily", repeat_day=null
- "Check email" → recurring_type=null, repeat_day=null

Context rules (pick the BEST match):
- 💼 Work: meetings, reports, clients, projects, emails, deadlines, invoices, work-related
- 🏠 Personal: home, errands, family, finances, hobbies, personal admin
- 🏃 Health: exercise, gym, run, weigh, diet, doctor, medication, fitness, health, workout, steps, water intake
- 🤝 Collab: shared tasks with others, collaborations, group projects

If TASK: {{"type":"task","task_name":"clean concise action","deadline_days":<integer or null>,"context":"one of: 💼 Work | 🏠 Personal | 🏃 Health | 🤝 Collab","confidence":"high|low","recurring_type":"daily|weekly|monthly or null","repeat_day":"Mon|Tue|Wed|Thu|Fri|Sat|Sun|1st..31st|Last or null"}}"""
    try:
        resp = claude.messages.create(
            model=claude_model,
            max_tokens=250,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        alert_claude_auth_failure(str(exc))
        raise
    raw = strip_json_fences(resp.content[0].text.strip())
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
        raw = strip_json_fences(resp.content[0].text.strip())
        result = json.loads(raw)
        valid_topics = [t for t in result.get("topics", []) if t in topic_options]
        return {"title": result.get("title", title or url)[:200], "topics": valid_topics or ["💡 Ideas"]}
    except Exception as exc:
        alert_claude_auth_failure(str(exc))
        return {"title": title or url[:200], "topics": ["💡 Ideas"]}
