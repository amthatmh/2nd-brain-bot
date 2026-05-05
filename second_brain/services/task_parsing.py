"""Pure task parsing helpers extracted from main."""

from __future__ import annotations

import re


def split_tasks(text: str, bullet_re: re.Pattern[str]) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(bullet_re.match(l) for l in lines):
        tasks = [bullet_re.sub("", l).strip() for l in lines if bullet_re.match(l)]
        return tasks if len(tasks) > 1 else [text]
    if len(lines) > 1:
        lower = text.lower()
        if re.search(r"\bschedule\b.*\brecurring\b", lower) and re.search(r"\bevery\b", lower):
            return [text]
        return lines
    return [text]


def looks_like_crossfit_programme(text: str) -> bool:
    lower = text.lower()
    day_hits = len(re.findall(r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)\b", lower))
    section_hits = len(re.findall(r"(?:^|\n)\s*[bc]\.", lower))
    workout_hits = len(re.findall(r"\b(amrap|emom|for time|rounds?|reps?|wod|snatch|clean|jerk|burpee|row|sit ups?|pushups?)\b", lower))
    if day_hits >= 2 and (section_hits >= 2 or workout_hits >= 3):
        return True
    return day_hits >= 1 and (section_hits >= 1 or workout_hits >= 4)


def looks_like_task_batch(text: str, bullet_re: re.Pattern[str]) -> bool:
    if looks_like_crossfit_programme(text):
        return False
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) <= 1:
        return False
    numbered_or_bulleted = sum(1 for l in lines if bullet_re.match(l))
    if numbered_or_bulleted >= 2:
        return True
    lead = lines[0].lower()
    if lead in {"add", "todo", "to-do", "tasks"}:
        return True
    return False


def infer_deadline_override(text: str) -> int | None:
    lower = text.lower()
    if re.search(r"\btomorrow\b", lower):
        return 1
    if re.search(r"\b(?:today|tonight)\b", lower):
        return 0
    if re.search(r"\bthis week\b", lower):
        return 5
    if re.search(r"\bthis month\b", lower):
        return 20
    return None


def infer_batch_overrides(text: str) -> dict:
    lower = text.lower()
    context = None
    context_aliases = [
        ("💼 Work", ["work", "💼"]),
        ("🏠 Personal", ["personal", "🏠"]),
        ("🏃 Health", ["health", "🏃"]),
        ("🤝 Collab", ["collab", "🤝"]),
    ]

    explicit_scope = re.search(r"\b(?:under|for|in)\s+([^\n,.;:]+)", lower)
    scoped_text = explicit_scope.group(1) if explicit_scope else ""
    haystacks = [scoped_text, lower] if scoped_text else [lower]

    for hay in haystacks:
        for notion_context, aliases in context_aliases:
            if any((a in hay) if not a.isalpha() else re.search(rf"\b{re.escape(a)}\b", hay) for a in aliases):
                context = notion_context
                break
        if context:
            break

    return {"context": context, "deadline_days": infer_deadline_override(text)}
