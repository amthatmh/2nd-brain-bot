from second_brain.services import task_parsing


def test_infer_deadline_override_prefers_tomorrow():
    assert task_parsing.infer_deadline_override('Add personal task for tomorrow "Fix steps count"') == 1


def test_infer_batch_overrides_uses_deadline_parser():
    parsed = task_parsing.infer_batch_overrides('Work tasks for this week: send report')
    assert parsed['deadline_days'] == 5
    assert parsed['context'] == '💼 Work'
