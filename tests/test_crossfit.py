"""Tests for CrossFit module — classifier and notion helpers."""

from second_brain.crossfit.handlers import parse_rounds_reps, parse_time_to_seconds


def test_parse_rounds_reps():
    assert parse_rounds_reps("8+5") == (8, 5)
    assert parse_rounds_reps("8 rounds 5 reps") == (8, 5)


def test_parse_time_result():
    assert parse_time_to_seconds("14:32") == 872
