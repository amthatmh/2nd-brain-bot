"""Tests for CrossFit module — classifier and notion helpers."""

from types import SimpleNamespace

from second_brain.crossfit.classify import classify_workout_message
from second_brain.crossfit.handlers import parse_rounds_reps, parse_time_to_seconds


class _FakeClaude:
    def __init__(self, payload: str):
        self.payload = payload
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        del kwargs
        return SimpleNamespace(content=[SimpleNamespace(text=self.payload)])


def test_classify_strength_message():
    """classify_workout_message should detect 'back squat 225 5x3' as strength."""
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Back Squat","load_lbs":225,"load_kg":102.1,"sets":5,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    out = classify_workout_message("back squat 225 5x3", c, "test-model", 1000)
    assert out["type"] == "strength"
    assert out["movement"] == "Back Squat"


def test_classify_conditioning_message():
    """classify_workout_message should detect '8 rounds + 5 reps' as conditioning."""
    c = _FakeClaude('{"type":"conditioning","confidence":"high","movement":null,"load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":"AMRAP","duration_mins":15,"partner":false}')
    out = classify_workout_message("8 rounds + 5 reps", c, "test-model", 1000)
    assert out["type"] == "conditioning"
    assert out["format"] == "AMRAP"


def test_parse_rounds_reps():
    """'8+5' and '8 rounds 5 reps' should both parse to rounds=8, reps=5."""
    assert parse_rounds_reps("8+5") == (8, 5)
    assert parse_rounds_reps("8 rounds 5 reps") == (8, 5)


def test_parse_time_result():
    """'14:32' should parse to result_seconds=872."""
    assert parse_time_to_seconds("14:32") == 872
