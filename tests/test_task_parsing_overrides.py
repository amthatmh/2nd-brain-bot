import re
from datetime import date

from second_brain.services import task_parsing


def test_infer_deadline_override_prefers_tomorrow():
    assert task_parsing.infer_deadline_override('Add personal task for tomorrow "Fix steps count"') == 1


def test_infer_deadline_override_weekday_resolves_to_next_occurrence():
    # 2026-07-18 is a Saturday, so Monday is 2 days out.
    assert task_parsing.infer_deadline_override("Buy Flowers on Monday", today=date(2026, 7, 18)) == 2


def test_infer_deadline_override_weekday_same_day_rolls_a_week():
    assert task_parsing.infer_deadline_override("Buy flowers on Saturday", today=date(2026, 7, 18)) == 7


def test_infer_deadline_override_relative_words_beat_weekday():
    assert task_parsing.infer_deadline_override("Prep Monday standup today", today=date(2026, 7, 18)) == 0


def test_infer_batch_overrides_passes_today_to_weekday_parser():
    parsed = task_parsing.infer_batch_overrides("Personal tasks for Sunday: water plants", today=date(2026, 7, 18))
    assert parsed["deadline_days"] == 1
    assert parsed["context"] == "🏠 Personal"


def test_infer_batch_overrides_uses_deadline_parser():
    parsed = task_parsing.infer_batch_overrides('Work tasks for this week: send report')
    assert parsed['deadline_days'] == 5
    assert parsed['context'] == '💼 Work'


_BULLET_RE = re.compile(r"^[\s]*(?:[-•*]|\d+[.):])\s+", re.MULTILINE)


def test_split_tasks_keeps_multi_sentence_task_together():
    text = "Add work task: Send Stephen Door drop information. Due today"
    assert task_parsing.split_tasks(text, _BULLET_RE) == [text]
    assert not task_parsing.looks_like_task_batch(text, _BULLET_RE)


def test_split_tasks_uses_and_as_explicit_delimiter():
    assert task_parsing.split_tasks(
        "Send report AND schedule meeting AND review proposal",
        _BULLET_RE,
    ) == ["Send report", "schedule meeting", "review proposal"]


def test_split_tasks_uses_numbered_lines_as_explicit_delimiter():
    assert task_parsing.split_tasks(
        "1. Finish report\n2. Schedule call\n3. Review docs",
        _BULLET_RE,
    ) == ["Finish report", "Schedule call", "Review docs"]


def test_split_tasks_keeps_plain_multiline_message_together():
    text = "Send Stephen Door drop information.\nDue today"
    assert task_parsing.split_tasks(text, _BULLET_RE) == [text]


def test_split_tasks_keeps_comma_separated_task_together():
    text = "Pick up groceries, milk, and bread tomorrow"
    assert task_parsing.split_tasks(text, _BULLET_RE) == [text]
