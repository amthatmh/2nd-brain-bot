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


def test_wod_flow_prompts_result_before_rx_scaled():
    from second_brain.crossfit.handlers import MOVEMENTS_CACHE, handle_cf_callback, handle_cf_text_reply, handle_cf_wod_flow

    MOVEMENTS_CACHE.clear()
    MOVEMENTS_CACHE["Wall Walks"] = "mov-wall-walks"
    message = _DummyMessage()
    cf_pending = {}
    notion = SimpleNamespace(databases=SimpleNamespace(query=lambda **kwargs: {"results": []}))

    asyncio.run(handle_cf_wod_flow(message, {"format": "AMRAP"}, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))
    key = str(message.chat_id)
    assert cf_pending[key]["stage"] == "format"
    assert "What format was the WOD?" in message.replies[-1][0]

    class _DummyQuery:
        def __init__(self, message):
            self.message = message
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    q = _DummyQuery(message)
    asyncio.run(handle_cf_callback(q, ["cf", "fmt", key, "amrap"], None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))
    assert cf_pending[key]["format"] == "amrap"
    assert cf_pending[key]["stage"] == "movement"
    assert "Which movement(s)" in message.replies[-1][0]

    asyncio.run(handle_cf_text_reply(message, "Wall Walks", key, None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[key]
    assert state["stage"] == "time_cap"
    assert state["workout_structure"] == "Wall Walks"
    assert "How long was the AMRAP" in message.replies[-1][0]
    assert "Rx or Scaled?" not in message.replies[-1][0]

    asyncio.run(handle_cf_text_reply(message, "14 minutes", str(message.chat_id), None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[str(message.chat_id)]
    assert state["stage"] == "result"
    assert state["time_cap_mins"] == 14
    assert "rounds + reps" in message.replies[-1][0]

    asyncio.run(handle_cf_text_reply(message, "5 rounds + 12 reps", str(message.chat_id), None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[str(message.chat_id)]
    assert state["stage"] == "rx_scaled"
    assert state["result_notes"] == "5 rounds + 12 reps"
    assert "Rx or Scaled?" in message.replies[-1][0]


def test_wod_rx_callback_without_result_keeps_result_prompt_defensive():
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

    assert cf_pending[key]["stage"] == "result"
    assert cf_pending[key]["rx_scaled"] == "Scaled"
    assert "time" in q.message.replies[-1][0].lower()


def test_fuzzy_match_movements_uses_extracted_canonical_name_for_weighted_db_entry():
    from second_brain.crossfit.nlp import fuzzy_match_movements

    matches = asyncio.run(fuzzy_match_movements(["Hang Clean"], {"Hang Cleans (115/85)": "mov-hang-clean"}))

    assert matches == [("Hang Clean", "Hang Cleans (115/85)", 1.0)]



def test_fuzzy_match_movements_prefers_hang_power_clean_for_ambiguous_hang_clean():
    from second_brain.crossfit.nlp import fuzzy_match_movements

    cache = {
        "Hang Squat Clean": "mov-hang-squat-clean",
        "Hang Power Clean": "mov-hang-power-clean",
        "Sandbag Clean": "mov-sandbag-clean",
    }

    matches = asyncio.run(fuzzy_match_movements(["Hang Clean"], cache))

    assert matches[0][1] == "Hang Power Clean"
    assert matches[0][2] > 0.80


def test_fuzzy_match_movements_still_respects_explicit_hang_squat_clean():
    from second_brain.crossfit.nlp import fuzzy_match_movements

    cache = {
        "Hang Squat Clean": "mov-hang-squat-clean",
        "Hang Power Clean": "mov-hang-power-clean",
    }

    matches = asyncio.run(fuzzy_match_movements(["Hang Squat Clean"], cache))

    assert matches[0][1] == "Hang Squat Clean"


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
    assert cf_pending[str(q.message.chat_id)]["stage"] == "format"
    assert "What format was the WOD?" in q.message.replies[-1][0]
    assert q.message.replies[-1][1].get("reply_markup") is not None
    assert "Which movement(s) were in the WOD?" not in q.message.replies[-1][0]
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


def test_extract_workout_data_parses_complete_claude_payload():
    from datetime import datetime
    from second_brain.crossfit.nlp import extract_workout_data

    payload = '{"movements":["Hang Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    out = asyncio.run(
        extract_workout_data(
            "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
            _FakeClaude(payload),
            datetime(2026, 5, 8),
        )
    )

    assert out == {
        "movements": ["Hang Clean"],
        "date": "2026-05-06",
        "sets": 6,
        "reps": 4,
        "weight_lbs": 115.0,
        "weight_kg": 52.2,
        "scheme": "6x4",
        "notes": None,
        "workout_structure": "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
        "raw_input": "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
        "wod_name": None,
    }


def test_extract_workout_data_fallback_parses_common_metadata_without_claude():
    from datetime import datetime
    from second_brain.crossfit.nlp import extract_workout_data

    out = asyncio.run(
        extract_workout_data(
            "5x5 back squat at 225# yesterday",
            None,
            datetime(2026, 5, 8),
        )
    )

    assert out["date"] == "2026-05-07"
    assert out["sets"] == 5
    assert out["reps"] == 5
    assert out["weight_lbs"] == 225.0
    assert out["weight_kg"] == 102.1
    assert out["scheme"] == "5x5"


def test_extract_workout_data_fallback_parses_sets_weight_and_slash_date():
    from datetime import datetime
    from second_brain.crossfit.nlp import extract_workout_data

    out = asyncio.run(
        extract_workout_data(
            "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
            None,
            datetime(2026, 5, 8),
        )
    )

    assert out["date"] == "2026-05-06"
    assert out["sets"] == 6
    assert out["reps"] == 4
    assert out["weight_lbs"] == 115.0
    assert out["weight_kg"] == 52.2
    assert out["scheme"] == "6x4"


def test_extract_workout_data_uses_fallback_metadata_when_claude_omits_it():
    from datetime import datetime
    from second_brain.crossfit.nlp import extract_workout_data

    payload = '{"movements":["Hang Squat Clean"],"date":null,"sets":null,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":null,"notes":null}'
    out = asyncio.run(
        extract_workout_data(
            "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
            _FakeClaude(payload),
            datetime(2026, 5, 8),
        )
    )

    assert out["movements"] == ["Hang Squat Clean"]
    assert out["date"] == "2026-05-06"
    assert out["sets"] == 6
    assert out["reps"] == 4
    assert out["weight_lbs"] == 115.0
    assert out["weight_kg"] == 52.2
    assert out["scheme"] == "6x4"


def test_create_strength_log_accepts_extracted_date_and_scheme():
    from second_brain.crossfit.notion import create_strength_log

    calls = []
    notion = SimpleNamespace(pages=SimpleNamespace(create=lambda **kwargs: calls.append(kwargs) or {"id": "log"}))

    page_id = create_strength_log(
        notion,
        "workout-log",
        ["mov-hang-clean"],
        "Hang Squat Clean",
        115,
        6,
        4,
        False,
        "week-1",
        None,
        None,
        "2026-05-06",
        "6x4",
        52.2,
    )

    assert page_id == "log"
    props = calls[0]["properties"]
    assert props["Date"] == {"date": {"start": "2026-05-06"}}
    assert props["effort_sets"] == {"number": 6}
    assert props["effort_reps"] == {"number": 4}
    assert props["effort_scheme"] == {"rich_text": [{"text": {"content": "6x4"}}]}
    assert props["load_lbs"] == {"number": 115}
    assert props["load_kg"] == {"number": 52.2}
    assert props["weekly_program_ref"] == {"relation": [{"id": "week-1"}]}


def test_strength_flow_auto_logs_complete_extracted_metadata(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Hang Clean"
        return ["mov-hang-clean"], ["Hang Squat Clean"]

    async def fake_level_check(message, movement_id, movement_name, notion, config, cf_pending, key):
        del message, movement_id, movement_name, notion, config, cf_pending, key
        return False

    async def fake_week(notion):
        del notion
        return "week-1"

    def fake_create_strength_log(notion, workout_log_db_id, movement_ids, movement_name, load_lbs, sets, reps, is_max_attempt, weekly_program_id, cycle_id, readiness, workout_date=None, effort_scheme=None, load_kg=None):
        del notion, is_max_attempt, cycle_id, readiness
        created.update(
            workout_log_db_id=workout_log_db_id,
            movement_ids=movement_ids,
            movement_name=movement_name,
            load_lbs=load_lbs,
            sets=sets,
            reps=reps,
            weekly_program_id=weekly_program_id,
            workout_date=workout_date,
            effort_scheme=effort_scheme,
            load_kg=load_kg,
        )
        return "log-1"

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)
    monkeypatch.setattr(handlers, "handle_gymnastics_level_check", fake_level_check)
    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "create_strength_log", fake_create_strength_log)

    payload = '{"movements":["Hang Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    message = _DummyMessage()

    asyncio.run(
        handlers.handle_cf_strength_flow(
            message,
            {"raw_text": "Did 6 sets of 4x hang clean squat at 115lbs on 5/6"},
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            {},
        )
    )

    assert created == {
        "workout_log_db_id": "workout-log",
        "movement_ids": ["mov-hang-clean"],
        "movement_name": "Hang Squat Clean",
        "load_lbs": 115.0,
        "sets": 6,
        "reps": 4,
        "weekly_program_id": "week-1",
        "workout_date": "2026-05-06",
        "effort_scheme": "6x4",
        "load_kg": 52.2,
    }
    assert "Strength logged" in message.replies[-1][0]
    assert "Date: 2026-05-06" in message.replies[-1][0]
    assert "Scheme: 6x4" in message.replies[-1][0]
    assert "115.0lbs (52.2kg)" in message.replies[-1][0]
    assert not any("Any notes" in reply[0] for reply in message.replies)


def test_wod_amrap_time_cap_and_workout_structure_logged(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    def fake_create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_id, readiness, workout_structure=None):
        del notion, duration_mins, result_seconds, result_reps, scaling_notes, is_partner, wod_name, readiness
        created.update(
            wod_log_db_id=wod_log_db_id,
            wod_format=wod_format,
            time_cap_mins=time_cap_mins,
            result_type=result_type,
            result_rounds=result_rounds,
            rx_scaled=rx_scaled,
            movement_page_ids=movement_page_ids,
            weekly_program_id=weekly_program_id,
            workout_structure=workout_structure,
        )
        return "wod-log"

    async def fake_week(notion):
        del notion
        return "week-1"

    monkeypatch.setattr(handlers, "create_wod_log", fake_create_wod_log)
    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    handlers.MOVEMENTS_CACHE.clear()
    handlers.MOVEMENTS_CACHE.update({
        "Wall Walks": "mov-wall-walks",
        "Hang Cleans": "mov-hang-cleans",
        "Burpee Over Bar": "mov-burpee-over-bar",
        "V-Ups": "mov-v-ups",
    })

    message = _DummyMessage()
    key = str(message.chat_id)
    cf_pending = {key: {"mode": "wod", "stage": "movement", "format": "amrap"}}
    notion = SimpleNamespace(databases=SimpleNamespace(query=lambda **kwargs: {"results": []}))

    raw_structure = "3x Wall walks, 6 hang cleans, 9 burpee over bar, 12 v-ups"
    asyncio.run(handlers.handle_cf_text_reply(message, raw_structure, key, None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    assert cf_pending[key]["stage"] == "time_cap"
    assert cf_pending[key]["workout_structure"] == raw_structure
    assert "How long was the AMRAP" in message.replies[-1][0]

    asyncio.run(handlers.handle_cf_text_reply(message, "14 minutes", key, None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    assert cf_pending[key]["stage"] == "result"
    assert cf_pending[key]["time_cap_mins"] == 14
    assert "rounds + reps" in message.replies[-1][0]

    asyncio.run(handlers.handle_cf_text_reply(message, "6 rounds", key, None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))
    assert cf_pending[key]["stage"] == "rx_scaled"

    class _DummyQuery:
        def __init__(self, message):
            self.message = message
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    asyncio.run(handlers.handle_cf_callback(_DummyQuery(message), ["cf", "rx", key, "scaled"], None, notion, {"NOTION_WOD_LOG_DB": "wod"}, cf_pending))

    assert created == {
        "wod_log_db_id": "wod",
        "wod_format": "AMRAP",
        "time_cap_mins": 14,
        "result_type": "Rounds",
        "result_rounds": 6,
        "rx_scaled": "Scaled",
        "movement_page_ids": ["mov-wall-walks", "mov-hang-cleans", "mov-burpee-over-bar", "mov-v-ups"],
        "weekly_program_id": "week-1",
        "workout_structure": raw_structure,
    }
    assert key not in cf_pending


def test_wod_for_time_result_is_captured_before_rx_and_logged(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    def fake_create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_id, readiness):
        del notion, duration_mins, time_cap_mins, result_rounds, result_reps, is_partner, wod_name, readiness
        created.update(
            wod_log_db_id=wod_log_db_id,
            wod_format=wod_format,
            result_type=result_type,
            result_seconds=result_seconds,
            rx_scaled=rx_scaled,
            scaling_notes=scaling_notes,
            movement_page_ids=movement_page_ids,
            weekly_program_id=weekly_program_id,
        )
        return "wod-log"

    async def fake_week(notion):
        del notion
        return "week-1"

    monkeypatch.setattr(handlers, "create_wod_log", fake_create_wod_log)
    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)

    class _DummyQuery:
        def __init__(self, message):
            self.message = message
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    message = _DummyMessage()
    key = str(message.chat_id)
    cf_pending = {
        key: {
            "mode": "wod",
            "stage": "result",
            "format": "for_time",
            "movement_page_ids": ["mov-wall-walks", "mov-hang-power-clean"],
        }
    }

    asyncio.run(handlers.handle_cf_text_reply(message, "12:34", key, None, SimpleNamespace(), {"NOTION_WOD_LOG_DB": "wod"}, cf_pending))

    assert cf_pending[key]["stage"] == "rx_scaled"
    assert cf_pending[key]["result_notes"] == "12:34"
    assert "Rx or Scaled?" in message.replies[-1][0]

    q = _DummyQuery(message)
    asyncio.run(handlers.handle_cf_callback(q, ["cf", "rx", key, "rx"], None, SimpleNamespace(), {"NOTION_WOD_LOG_DB": "wod"}, cf_pending))

    assert created == {
        "wod_log_db_id": "wod",
        "wod_format": "For Time",
        "result_type": "Time",
        "result_seconds": 754,
        "rx_scaled": "Rx",
        "scaling_notes": "12:34",
        "movement_page_ids": ["mov-wall-walks", "mov-hang-power-clean"],
        "weekly_program_id": "week-1",
    }
    assert key not in cf_pending
    assert "WOD logged" in message.replies[-1][0]


def test_wod_format_callback_prompts_result_before_rx_when_movements_exist():
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
    cf_pending = {key: {"mode": "wod", "stage": "format", "movement_page_ids": ["mov-hang-power-clean"]}}

    asyncio.run(handle_cf_callback(q, ["cf", "fmt", key, "for_time"], None, SimpleNamespace(), {}, cf_pending))

    assert cf_pending[key]["stage"] == "result"
    assert "What was your time" in q.message.replies[-1][0]
    assert "Rx or Scaled?" not in q.message.replies[-1][0]
