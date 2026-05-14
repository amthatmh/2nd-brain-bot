"""Pure task parsing helpers extracted from main."""

from __future__ import annotations

import re


def split_tasks(text: str, bullet_re: re.Pattern[str]) -> list[str]:
    """Split tasks only on explicit multi-task delimiters.

    Periods, commas, and plain newlines often carry task metadata (for
    example, "Send report. Due today"), so they are intentionally kept as
    a single task unless the user uses numbered items, bullet items, or AND.
    """
    original = text.strip()
    if not original:
        return [text]

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Numbered lists, either one item per line or inline: "1. A 2) B".
    numbered_line_pattern = r"^\s*\d+[.)]\s+"
    numbered_lines = [l for l in lines if re.match(numbered_line_pattern, l)]
    if len(numbered_lines) >= 2:
        return [re.sub(numbered_line_pattern, "", l).strip() for l in numbered_lines]

    inline_numbered_pattern = r"(?:^|\s)\d+[.)]\s+"
    inline_numbered_matches = list(re.finditer(inline_numbered_pattern, text))
    if len(inline_numbered_matches) >= 2:
        tasks: list[str] = []
        for idx, match in enumerate(inline_numbered_matches):
            start = match.end()
            end = inline_numbered_matches[idx + 1].start() if idx + 1 < len(inline_numbered_matches) else len(text)
            task = text[start:end].strip(" \t\n-•*")
            if task:
                tasks.append(task)
        if len(tasks) >= 2:
            return tasks

    # Bullets must be explicit list markers; do not treat plain newlines as a batch.
    bulleted_lines = [l for l in lines if bullet_re.match(l)]
    if len(bulleted_lines) >= 2:
        return [bullet_re.sub("", l).strip() for l in bulleted_lines]

    # Inline bullet characters (•/*) can also denote an explicit list.
    inline_bullet_pattern = r"(?:^|\s)[•*]\s+"
    inline_bullet_matches = list(re.finditer(inline_bullet_pattern, text))
    if len(inline_bullet_matches) >= 2:
        tasks = []
        for idx, match in enumerate(inline_bullet_matches):
            start = match.end()
            end = inline_bullet_matches[idx + 1].start() if idx + 1 < len(inline_bullet_matches) else len(text)
            task = text[start:end].strip(" \t\n-•*")
            if task:
                tasks.append(task)
        if len(tasks) >= 2:
            return tasks

    # AND is the only prose delimiter that intentionally creates a batch.
    if re.search(r"\s+AND\s+", text):
        tasks = [t.strip() for t in re.split(r"\s+AND\s+", text) if t.strip()]
        if len(tasks) >= 2:
            return tasks

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
    """Return True only when explicit multi-task delimiters are present."""
    if looks_like_crossfit_programme(text):
        return False

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    if re.search(r"\s+AND\s+", text):
        if len([t for t in re.split(r"\s+AND\s+", text) if t.strip()]) >= 2:
            return True

    numbered_line_pattern = r"^\d+[.)]\s+"
    if sum(1 for l in lines if re.match(numbered_line_pattern, l)) >= 2:
        return True

    if len(list(re.finditer(r"(?:^|\s)\d+[.)]\s+", text))) >= 2:
        return True

    if sum(1 for l in lines if bullet_re.match(l)) >= 2:
        return True

    if len(list(re.finditer(r"(?:^|\s)[•*]\s+", text))) >= 2:
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
