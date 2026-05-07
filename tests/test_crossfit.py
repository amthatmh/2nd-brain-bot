"""Tests for CrossFit module — classifier and notion helpers."""

from types import SimpleNamespace

import asyncio

from second_brain.crossfit.classify import classify_workout_message
from second_brain.crossfit.handlers import handle_cf_strength_flow, handle_gymnastics_level_check, parse_rounds_reps, parse_time_to_seconds
from second_brain.crossfit.notion import get_progressions_for_movement, save_programme, set_current_level


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


def test_classify_workout_message_fast_path_long_text_with_days():
    """Long text with day headings should return programme without calling Claude."""
    long_text = "MONDAY\nPERFORMANCE\nB. Back Squat\n" * 20
    c = _FakeClaude('{}')
    result = classify_workout_message(long_text, c, "model", 1000)
    assert result["type"] == "programme"
    assert result["confidence"] == "high"


def test_classify_workout_message_short_text_not_programme():
    """Short message without day headings should not fast-path to programme."""
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Back Squat","load_lbs":225,"load_kg":102.1,"sets":5,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    result = classify_workout_message("back squat 225 5x3", c, "model", 1000)
    assert result["type"] == "strength"


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


def test_classify_programme_fast_path():
    text = "MONDAY\nPERFORMANCE\nB. Back Squat\nC. For Time\n" * 15
    c = _FakeClaude("{}")
    result = classify_workout_message(text, c, "model", 1000)
    assert result["type"] == "programme"
    assert result["confidence"] == "high"


def test_classify_short_text_not_fast_path():
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Back Squat","load_lbs":225,"load_kg":102,"sets":5,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    result = classify_workout_message("back squat 225 5x3", c, "model", 1000)
    assert result["type"] == "strength"


def test_save_programme_flat_fallback():
    calls = []
    updates = []
    notion = SimpleNamespace(
        pages=SimpleNamespace(
            create=lambda **kwargs: calls.append(kwargs) or {"id": "parent"},
            update=lambda **kwargs: updates.append(kwargs) or {"id": "parent"},
        ),
        databases=SimpleNamespace(query=lambda **kwargs: {"results": []}),
    )
    parsed = {"week_label": "Week of 2026-05-04", "days": [{"day": "Monday", "section_b": None, "section_c": None}]}
    save_programme(notion, "program", "days", "", parsed, "raw")
    assert len(calls) >= 2
    assert updates == []


def test_save_programme_updates_parent_movements_relation():
    calls = []
    updates = []
    notion = SimpleNamespace(
        pages=SimpleNamespace(
            create=lambda **kwargs: calls.append(kwargs) or {"id": "parent"},
            update=lambda **kwargs: updates.append(kwargs) or {"id": "parent"},
        ),
        databases=SimpleNamespace(query=lambda **kwargs: {"results": [{"id": "mov-page", "properties": {"Name": {"title": [{"plain_text": "Back Squat"}]}}}]}),
    )
    parsed = {
        "week_label": "Week of 2026-05-04",
        "tracks": [{"track": "Performance", "days": [{"day": "Monday", "section_b": {"description": "5x5", "movements": ["Back Squat"]}, "section_c": None}]}],
    }
    save_programme(notion, "program", "days", "movements", parsed, "raw")
    assert any("Movements" in u.get("properties", {}) for u in updates)


def test_rich_text_chunks_splits_long_text():
    """_rich_text_chunks should split text > 1900 chars into multiple blocks."""
    from second_brain.crossfit.notion import _rich_text_chunks

    long = "A" * 4000
    chunks = _rich_text_chunks(long)
    assert len(chunks) == 3
    assert all(len(c["text"]["content"]) <= 1900 for c in chunks)


def test_rich_text_chunks_short_text():
    """Short text should produce a single block."""
    from second_brain.crossfit.notion import _rich_text_chunks

    chunks = _rich_text_chunks("hello")
    assert len(chunks) == 1
    assert chunks[0]["text"]["content"] == "hello"

def test_infer_primary_patterns_olympic_for_hang_clean():
    from second_brain.crossfit.notion import infer_primary_patterns

    assert infer_primary_patterns("Hang Clean") == ["Olympic"]


def test_get_or_create_movement_sets_primary_pattern_on_create():
    from second_brain.crossfit.notion import get_or_create_movement

    calls = []
    notion = SimpleNamespace(
        databases=SimpleNamespace(query=lambda **kwargs: {"results": []}),
        pages=SimpleNamespace(create=lambda **kwargs: calls.append(kwargs) or {"id": "new-movement"}),
    )

    page_id = get_or_create_movement(notion, "movements", "Hang Clean")

    assert page_id == "new-movement"
    props = calls[0]["properties"]
    assert props["Category"] == {"select": {"name": "Compound"}}
    assert props["Primary Pattern"] == {"multi_select": [{"name": "Olympic"}]}


def test_wod_flow_prompts_rx_scaled_before_result_notes():
    from second_brain.crossfit.handlers import MOVEMENTS_CACHE, handle_cf_text_reply, handle_cf_wod_flow

    MOVEMENTS_CACHE.clear()
    MOVEMENTS_CACHE["Wall Walks"] = "mov-wall-walks"
    message = _DummyMessage()
    cf_pending = {}
    notion = SimpleNamespace(databases=SimpleNamespace(query=lambda **kwargs: {"results": []}))

    asyncio.run(handle_cf_wod_flow(message, {"format": "AMRAP"}, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))
    asyncio.run(handle_cf_text_reply(message, "Wall Walks", str(message.chat_id), None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[str(message.chat_id)]
    assert state["stage"] == "rx_scaled"
    assert "Rx or Scaled?" in message.replies[-1][0]


def test_wod_rx_callback_moves_to_result_notes_prompt():
    from second_brain.crossfit.handlers import handle_cf_callback

    class _DummyQuery:
        def __init__(self):
            self.message = _DummyMessage()
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    q = _DummyQuery()
    key = str(q.message.chat_id)
    cf_pending = {key: {"mode": "wod", "stage": "rx_scaled", "format": "for_time"}}
    notion = SimpleNamespace()

    asyncio.run(handle_cf_callback(q, ["cf", "rx", key, "scaled"], None, notion, {}, cf_pending))

    assert cf_pending[key]["stage"] == "notes"
    assert cf_pending[key]["rx_scaled"] == "Scaled"
    assert "time" in q.message.replies[-1][0].lower()


def test_fuzzy_match_movements_uses_extracted_canonical_name_for_weighted_db_entry():
    from second_brain.crossfit.nlp import fuzzy_match_movements

    matches = asyncio.run(fuzzy_match_movements(["Hang Clean"], {"Hang Cleans (115/85)": "mov-hang-clean"}))

    assert matches == [("Hang Clean", "Hang Cleans (115/85)", 1.0)]



def test_fuzzy_match_movements_prefers_hang_squat_clean_over_sandbag_clean():
    from second_brain.crossfit.nlp import fuzzy_match_movements

    cache = {
        "Hang Squat Clean": "mov-hang-squat-clean",
        "Squat Clean": "mov-squat-clean",
        "Sandbag Clean": "mov-sandbag-clean",
    }

    matches = asyncio.run(fuzzy_match_movements(["Hang Clean"], cache))

    assert matches[0][1] == "Hang Squat Clean"
    assert matches[0][2] > 0.80


def test_resolve_movement_ids_extracts_before_fuzzy_matching():
    from second_brain.crossfit.handlers import MOVEMENTS_CACHE, _resolve_movement_ids

    MOVEMENTS_CACHE.clear()
    MOVEMENTS_CACHE["Hang Cleans (115/85)"] = "mov-hang-clean"
    claude = _FakeClaude('["Hang Clean"]')

    movement_ids, names = asyncio.run(
        _resolve_movement_ids(
            "4xHang squat and clean, did 6 sets at 115lb",
            claude,
            None,
            {"NOTION_MOVEMENTS_DB": "movements"},
        )
    )

    assert movement_ids == ["mov-hang-clean"]
    assert names == ["Hang Cleans (115/85)"]


def test_wod_callback_starts_wod_flow_prompt():
    from second_brain.crossfit.handlers import handle_cf_callback

    class _DummyQuery:
        def __init__(self):
            self.message = _DummyMessage()

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, *args, **kwargs):
            pass

    q = _DummyQuery()
    cf_pending = {}
    notion = SimpleNamespace(databases=SimpleNamespace(query=lambda **kwargs: {"results": []}))

    asyncio.run(handle_cf_callback(q, ["cf", "log_wod"], None, notion, {"NOTION_WOD_LOG_DB": "wod"}, cf_pending))

    assert cf_pending[str(q.message.chat_id)]["mode"] == "wod"
    assert "Which movement(s) were in the WOD?" in q.message.replies[-1][0]
    assert "configured yet" not in q.message.replies[-1][0]


def test_readiness_final_score_writes_and_clears_pending(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    class _DummyQuery:
        def __init__(self):
            self.message = _DummyMessage()
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    calls = []

    async def fake_log_daily_readiness(notion, **kwargs):
        calls.append((notion, kwargs))
        return {"id": "readiness-page"}

    monkeypatch.setattr(handlers, "log_daily_readiness", fake_log_daily_readiness)
    q = _DummyQuery()
    key = str(q.message.chat_id)
    cf_pending = {
        key: {
            "mode": "readiness",
            "stage": "soreness",
            "readiness": {"sleep_quality": "4", "energy": "5", "mood": "4", "stress": "2"},
        }
    }
    notion = SimpleNamespace()

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "ready", key, "soreness", "3"], None, notion, {"NOTION_DAILY_READINESS_DB": "ready-db"}, cf_pending))

    assert key not in cf_pending
    assert q.edits[-1][0] == "✅ Readiness logged!"
    assert calls[0][1] == {
        "sleep_quality": "4",
        "energy": "5",
        "mood": "4",
        "stress": "2",
        "soreness": "3",
        "daily_readiness_db_id": "ready-db",
    }
