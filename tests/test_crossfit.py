"""Tests for CrossFit module — classifier and notion helpers."""

from types import SimpleNamespace

import asyncio

from second_brain.crossfit.classify import classify_workout_message
from second_brain.crossfit.handlers import handle_cf_strength_flow, handle_gymnastics_level_check, parse_rounds_reps, parse_time_to_seconds
from second_brain.crossfit.notion import get_progressions_for_movement, set_current_level


class _FakeClaude:
    def __init__(self, payload: str):
        self.payload = payload
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        del kwargs
        return SimpleNamespace(content=[SimpleNamespace(text=self.payload)])


class _DummyMessage:
    def __init__(self):
        self.chat_id = 123
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace()


def test_classify_strength_message():
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Back Squat","load_lbs":225,"load_kg":102.1,"sets":5,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    out = classify_workout_message("back squat 225 5x3", c, "test-model", 1000)
    assert out["type"] == "strength"


def test_classify_conditioning_message():
    c = _FakeClaude('{"type":"conditioning","confidence":"high","movement":null,"load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":"AMRAP","duration_mins":15,"partner":false}')
    out = classify_workout_message("8 rounds + 5 reps", c, "test-model", 1000)
    assert out["type"] == "conditioning"


def test_classify_programme_text():
    c = _FakeClaude('{"type":"programme","confidence":"high","movement":null,"load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    out = classify_workout_message("MONDAY\nB. Squat\nC. AMRAP\nTUESDAY\nB. Bench\nC. For Time\nWEDNESDAY\nB. Deadlift\nC. EMOM", c, "test-model", 1000)
    assert out["type"] == "programme"


def test_parse_rounds_reps():
    assert parse_rounds_reps("8+5") == (8, 5)
    assert parse_rounds_reps("8 rounds 5 reps") == (8, 5)


def test_parse_time_result():
    assert parse_time_to_seconds("14:32") == 872


def test_graceful_degradation_missing_env():
    message = _DummyMessage()
    asyncio.run(handle_cf_strength_flow(message, {}, None, None, {}, {}))
    assert "isn't configured yet" in message.replies[0][0]


def test_get_progressions_for_movement_sorted_by_order():
    notion = SimpleNamespace(
        databases=SimpleNamespace(
            query=lambda **kwargs: {
                "results": [
                    {"id": "2", "properties": {"Name": {"title": [{"plain_text": "Step 2"}]}, "Target Movement": {"relation": [{"id": "mov1"}]}, "Order": {"number": 2}, "Is My Current Level": {"checkbox": False}, "Notes": {"rich_text": []}}},
                    {"id": "1", "properties": {"Name": {"title": [{"plain_text": "Step 1"}]}, "Target Movement": {"relation": [{"id": "mov1"}]}, "Order": {"number": 1}, "Is My Current Level": {"checkbox": True}, "Notes": {"rich_text": []}}},
                ]
            }
        )
    )
    rows = get_progressions_for_movement(notion, "db", "mov1")
    assert [r["page_id"] for r in rows] == ["1", "2"]


def test_set_current_level_toggles_only_selected():
    updates = []
    notion = SimpleNamespace(
        databases=SimpleNamespace(query=lambda **kwargs: {"results": [
            {"id": "p1", "properties": {"Name": {"title": [{"plain_text": "A"}]}, "Target Movement": {"relation": [{"id": "mov1"}]}, "Order": {"number": 1}, "Is My Current Level": {"checkbox": True}, "Notes": {"rich_text": []}}},
            {"id": "p2", "properties": {"Name": {"title": [{"plain_text": "B"}]}, "Target Movement": {"relation": [{"id": "mov1"}]}, "Order": {"number": 2}, "Is My Current Level": {"checkbox": False}, "Notes": {"rich_text": []}}},
        ]}),
        pages=SimpleNamespace(update=lambda **kwargs: updates.append(kwargs)),
    )
    set_current_level(notion, "db", "mov1", "p2")
    assert any(u["page_id"] == "p2" and u["properties"]["Is My Current Level"]["checkbox"] for u in updates)
    assert any(u["page_id"] == "p1" and not u["properties"]["Is My Current Level"]["checkbox"] for u in updates)


def test_handle_gymnastics_level_check_false_for_compound():
    notion = SimpleNamespace(pages=SimpleNamespace(retrieve=lambda **kwargs: {"properties": {"Category": {"select": {"name": "Compound"}}}}))
    out = asyncio.run(handle_gymnastics_level_check(_DummyMessage(), "mov1", "Back Squat", notion, {"NOTION_PROGRESSIONS_DB": "p", "NOTION_MOVEMENTS_DB": "m"}, {}, "k"))
    assert out is False
