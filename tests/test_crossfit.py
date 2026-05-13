"""Tests for CrossFit module — classifier and notion helpers."""

from types import SimpleNamespace

import asyncio

from second_brain.crossfit.classify import classify_workout_message
from second_brain.crossfit.handlers import handle_cf_strength_flow, handle_gymnastics_level_check, parse_rounds_reps, parse_time_to_seconds
from second_brain.crossfit.keyboards import crossfit_submenu_keyboard
from second_brain.crossfit.notion import get_progressions_for_movement, save_programme, set_current_level


class _FakeClaude:
    def __init__(self, payload: str):
        self.payload = payload
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        del kwargs
        return SimpleNamespace(content=[SimpleNamespace(text=self.payload)])


class _FakeBot:
    def __init__(self):
        self.edits = []

    async def edit_message_text(self, **kwargs):
        self.edits.append(kwargs)
        return SimpleNamespace()


class _DummyMessage:
    def __init__(self):
        self.chat_id = 123
        self.replies = []
        self.bot = _FakeBot()

    def get_bot(self):
        return self.bot

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(message_id=len(self.replies))


def test_crossfit_submenu_uses_conversational_log_labels():
    keyboard = crossfit_submenu_keyboard()

    assert [button.text for button in keyboard.inline_keyboard[0]] == [
        "📊 Readiness (A)",
        "🏋️ Strength (B)",
    ]
    assert [button.text for button in keyboard.inline_keyboard[1]] == [
        "🏆 WOD (C)",
        "💬 Workout Feel (D)",
    ]
    assert [button.callback_data for button in keyboard.inline_keyboard[0]] == [
        "cf:log_readiness",
        "cf:log_strength",
    ]
    assert [button.callback_data for button in keyboard.inline_keyboard[1]] == [
        "cf:log_wod",
        "cf:log_feel",
    ]


def test_classify_strength_message():
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Back Squat","load_lbs":225,"load_kg":102.1,"sets":5,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')
    out = classify_workout_message("back squat 225 5x3", c, "test-model", 1000)
    assert out["type"] == "strength"


def test_classify_conditioning_message():
    c = _FakeClaude('{"type":"conditioning","confidence":"high","movement":null,"load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":"AMRAP","duration_mins":15,"partner":false}')
    out = classify_workout_message("8 rounds + 5 reps", c, "test-model", 1000)
    assert out["type"] == "conditioning"




def test_classify_conditioning_message_normalizes_legacy_singular_movement():
    c = _FakeClaude('{"type":"conditioning","confidence":"high","movement":"Thruster","load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":"For Time","duration_mins":null,"partner":false}')

    out = classify_workout_message("21-15-9 thrusters", c, "test-model", 1000)

    assert out["type"] == "conditioning"
    assert out["movements"] == ["Thruster"]


def test_classify_conditioning_message_keeps_movements_array():
    c = _FakeClaude('{"type":"conditioning","confidence":"high","movement":null,"movements":["Thruster","Pull-Up"],"load_lbs":null,"load_kg":null,"sets":null,"reps":null,"is_max_attempt":false,"wod_name":null,"format":"For Time","duration_mins":null,"partner":false}')

    out = classify_workout_message("21-15-9 thrusters and pull-ups", c, "test-model", 1000)

    assert out["movements"] == ["Thruster", "Pull-Up"]

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



def test_classify_strength_message_preserves_per_movement_fields():
    c = _FakeClaude('{"type":"strength","confidence":"high","movement":"Push Press","movements":[{"movement":"Push Press","sets":4,"reps":3,"load_lbs":105},{"movement":"Push Jerk","sets":4,"reps":5,"load_lbs":105}],"load_lbs":105,"load_kg":null,"sets":4,"reps":3,"is_max_attempt":false,"wod_name":null,"format":null,"duration_mins":null,"partner":false}')

    out = classify_workout_message("4 sets of 3x105 lb push press + 4 sets of 5x105lb push jerk", c, "model", 1000)

    assert out["movement"] == "Push Press"
    assert out["sets"] == 4
    assert out["reps"] == 3
    assert out["load_lbs"] == 105
    assert out["movements"] == [
        {"movement": "Push Press", "sets": 4, "reps": 3, "load_lbs": 105},
        {"movement": "Push Jerk", "sets": 4, "reps": 5, "load_lbs": 105},
    ]


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


def test_handle_gymnastics_level_check_false_for_compound_multi_select():
    notion = SimpleNamespace(pages=SimpleNamespace(retrieve=lambda **kwargs: {"properties": {"Category": {"multi_select": [{"name": "Compound"}]}}}))
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
    assert props["Category"] == {"multi_select": [{"name": "Compound"}]}
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

    asyncio.run(handle_cf_text_reply(message, "14", str(message.chat_id), None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[str(message.chat_id)]
    assert state["stage"] == "result"
    assert state["time_cap_mins"] == 14
    assert message.bot.edits[-1]["text"] == "⏱️ How long was the AMRAP? *14mins*"
    assert message.bot.edits[-1]["reply_markup"] is None
    assert "rounds + reps" in message.replies[-1][0]

    asyncio.run(handle_cf_text_reply(message, "5 rounds + 12 reps", str(message.chat_id), None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    state = cf_pending[str(message.chat_id)]
    assert state["stage"] == "rx_scaled"
    assert state["result_notes"] == "5 rounds + 12 reps"
    assert message.bot.edits[-1]["text"] == "🔄 How many rounds + reps did you complete? *5 rounds + 12 reps*"
    assert message.bot.edits[-1]["reply_markup"] is None
    assert "Rx or Scaled?" in message.replies[-1][0]



def test_wod_time_cap_skip_edits_prompt_and_removes_button():
    from second_brain.crossfit.handlers import handle_cf_callback

    message = _DummyMessage()
    key = str(message.chat_id)
    cf_pending = {key: {"mode": "wod", "stage": "time_cap", "format": "amrap"}}
    notion = SimpleNamespace(databases=SimpleNamespace(query=lambda **kwargs: {"results": []}))

    class _DummyQuery:
        def __init__(self, message):
            self.message = message
            self.markup_edits = []
            self.text_edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_reply_markup(self, **kwargs):
            self.markup_edits.append(kwargs)

        async def edit_message_text(self, text, **kwargs):
            self.text_edits.append((text, kwargs))

    q = _DummyQuery(message)
    asyncio.run(handle_cf_callback(q, ["cf", "skip", key], None, notion, {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"}, cf_pending))

    assert cf_pending[key]["stage"] == "result"
    assert cf_pending[key]["time_cap_mins"] is None
    assert q.text_edits[-1][0] == "⏱️ How long was the AMRAP? *Skipped*"
    assert q.text_edits[-1][1]["reply_markup"] is None
    assert "rounds + reps" in message.replies[-1][0]


def test_wod_movement_stage_named_month_date_resolves_without_buttons(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Wall Walks"
        return ["mov-wall-walks"], ["Wall Walks"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    raw_text = "This log is for May 6th, 3x wall walks"
    payload = '{"movements":["Wall Walks"],"date":"2026-05-06","sets":3,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":"3x","notes":null,"workout_structure":"3x wall walks","raw_input":"This log is for May 6th, 3x wall walks","wod_name":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "wod", "stage": "movement", "format": "amrap"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            raw_text,
            key,
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending[key]
    assert state["workout_date"] == "2026-05-06"
    assert state["raw_workout_date"] == "May 6th"
    assert state["stage"] == "time_cap"
    assert "How long was the AMRAP" in message.replies[-1][0]
    assert not any("Which date did you mean" in reply[0] for reply in message.replies)


def test_wod_movement_stage_ambiguous_slash_date_prompts_buttons(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Wall Walks"
        return ["mov-wall-walks"], ["Wall Walks"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    raw_text = "WOD on 5/6, 3x wall walks"
    payload = '{"movements":["Wall Walks"],"date":"2026-05-06","sets":3,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":"3x","notes":null,"workout_structure":"3x wall walks","raw_input":"WOD on 5/6, 3x wall walks","wod_name":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "wod", "stage": "movement", "format": "amrap"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            raw_text,
            key,
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending[key]
    assert state["workout_date"] == "2026-05-06"
    assert state["raw_workout_date"] == "5/6"
    assert state["stage"] == "date_pick"
    assert state["stage_before_date_pick"] == "movement"
    assert state["raw_date_a"] == "2026-05-06"
    assert state["raw_date_b"] == "2026-06-05"
    assert "Which date did you mean?" in message.replies[-1][0]
    buttons = message.replies[-1][1]["reply_markup"].inline_keyboard[0]
    assert [button.text for button in buttons] == ["May 6", "Jun 5"]
    assert [button.callback_data for button in buttons] == [f"cf:date_pick:a:{key}", f"cf:date_pick:b:{key}"]


def test_wod_movement_stage_unambiguous_slash_date_resolves_without_buttons(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Wall Walks"
        return ["mov-wall-walks"], ["Wall Walks"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    raw_text = "WOD on 5/13, 3x wall walks"
    payload = '{"movements":["Wall Walks"],"date":"2026-05-13","sets":3,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":"3x","notes":null,"workout_structure":"3x wall walks","raw_input":"WOD on 5/13, 3x wall walks","wod_name":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "wod", "stage": "movement", "format": "amrap"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            raw_text,
            key,
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending[key]
    assert state["workout_date"] == "2026-05-13"
    assert state["raw_workout_date"] == "5/13"
    assert state["stage"] == "time_cap"
    assert "How long was the AMRAP" in message.replies[-1][0]
    assert not any("Which date did you mean" in reply[0] for reply in message.replies)

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
    assert q.edits[-1][0].startswith("✅ *Readiness logged!*")
    assert "Sleep Quality: 4" in q.edits[-1][0]
    assert "Soreness: 3" in q.edits[-1][0]
    assert q.edits[-1][1]["reply_markup"] is None
    assert calls[0][1] == {
        "sleep_quality": "4",
        "energy": "5",
        "mood": "4",
        "stress": "2",
        "soreness": "3",
        "daily_readiness_db_id": "ready-db",
    }


def test_readiness_progressive_callback_edits_single_message(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    class _DummyQuery:
        def __init__(self):
            self.message = _DummyMessage()
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    q = _DummyQuery()
    cf_pending = {}

    asyncio.run(
        handlers.handle_cf_callback(
            q,
            ["cf", "sleep", "4", "99"],
            None,
            SimpleNamespace(),
            {"NOTION_DAILY_READINESS_DB": "ready-db"},
            cf_pending,
        )
    )

    assert len(q.edits) == 1
    text, kwargs = q.edits[0]
    assert "Sleep Quality: 4" in text
    assert "Energy (1-5)?" in text
    assert kwargs["reply_markup"] is not None
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "cf:energy:4:1:99"


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
    assert props["Name"] == {"title": [{"text": {"content": "2026-05-06 — Strength"}}]}
    assert props["Date"] == {"date": {"start": "2026-05-06"}}
    assert props["effort_sets"] == {"number": 6}
    assert props["effort_reps"] == {"number": 4}
    assert props["load_lbs"] == {"number": 115}
    assert props["Movement"] == {"relation": [{"id": "mov-hang-clean"}]}
    assert props["weekly_program_ref"] == {"relation": [{"id": "week-1"}]}
    assert "effort_scheme" not in props
    assert "load_kg" not in props
    assert "calc_1rm_brzycki" not in props
    assert "calc_1rm_epley" not in props
    assert "is_max_attempt" not in props


def test_strength_flow_auto_logs_complete_extracted_metadata(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}
    pending = {}

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

    def fake_create_strength_log(**kwargs):
        created.update(
            workout_log_db_id=kwargs["workout_log_db_id"],
            movement_ids=kwargs["movement_page_id"],
            movement_name=kwargs["movement_name"],
            load_lbs=kwargs["load_lbs"],
            sets=kwargs["effort_sets"],
            reps=kwargs["effort_reps"],
            weekly_program_id=kwargs["weekly_program_page_id"],
            workout_date=kwargs["workout_date"],
            effort_scheme=kwargs["effort_scheme"],
            load_kg=kwargs["load_kg"],
            pending_state=dict(pending["123"]),
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
            {"raw_text": "Did 6 sets of 4x hang clean squat at 115lbs on May 6"},
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = created.pop("pending_state")
    assert state["sets"] == 6
    assert state["reps"] == 4
    assert state["weight_lbs"] == 115.0
    assert state["workout_date"] == "2026-05-06"
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
    assert "Strength logged" in message.replies[-2][0]
    assert "Date: 2026-05-06" in message.replies[-2][0]
    assert "Scheme: 6x4" in message.replies[-2][0]
    assert "Weight: 115lbs" in message.replies[-2][0]
    assert pending[str(message.chat_id)]["stage"] == "awaiting_feel"
    assert pending[str(message.chat_id)]["last_workout_page_id"] == "log-1"
    assert "How did that session feel?" in message.replies[-1][0]
    assert not any("Any notes" in reply[0] for reply in message.replies)




def test_finalize_flow_creates_one_strength_log_per_movement(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = []
    created_movements = []

    async def fake_week(notion):
        del notion
        return "week-1"

    def fake_get_or_create(notion, db_id, name):
        del notion
        created_movements.append((db_id, name))
        return f"mov-{name.lower().replace(' ', '-')}"

    def fake_create_strength_log(**kwargs):
        created.append(kwargs)
        return f"log-{len(created)}"

    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "get_or_create_movement", fake_get_or_create)
    monkeypatch.setattr(handlers, "create_strength_log", fake_create_strength_log)

    message = _DummyMessage()
    pending = {
        "123": {
            "mode": "strength",
            "movement_page_id": "mov-push-press",
            "movement_name": "Push Press",
            "workout_date": "2026-05-06",
            "raw_log": "raw session",
            "movements": [
                {"movement": "Push Press", "sets": 4, "reps": 3, "load_lbs": 105},
                {"movement": "Push Jerk", "sets": 4, "reps": 5, "load_lbs": 105},
            ],
        }
    }

    asyncio.run(handlers._finalize_flow(message, "123", SimpleNamespace(), {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"}, pending, "felt crisp"))

    assert len(created) == 2
    assert created[0]["movement_page_id"] == "mov-push-press"
    assert created[0]["movement_name"] == "Push Press"
    assert created[0]["effort_sets"] == 4
    assert created[0]["effort_reps"] == 3
    assert created[0]["load_lbs"] == 105.0
    assert created[1]["movement_page_id"] == "mov-push-jerk"
    assert created[1]["movement_name"] == "Push Jerk"
    assert created[1]["effort_sets"] == 4
    assert created[1]["effort_reps"] == 5
    assert created[1]["load_lbs"] == 105.0
    assert created[0]["workout_date"] == created[1]["workout_date"] == "2026-05-06"
    assert created[0]["raw_log"] == created[1]["raw_log"] == "raw session\n\nNotes: felt crisp"
    assert created_movements == [("movements", "Push Jerk")]
    assert pending["123"]["last_workout_page_ids"] == ["log-1", "log-2"]
    assert "Push Press, Push Jerk" in message.replies[-2][0]
    assert "4x3 105 lbs | 4x5 105 lbs" in message.replies[-2][0]


def test_strength_flow_disambiguates_slash_date_before_notes(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        return ["mov-hang-clean"], [text]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    pending = {}
    payload = '{"movements":["Hang Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    message = _DummyMessage()

    asyncio.run(
        handlers.handle_cf_strength_flow(
            message,
            {"raw_text": "Did 6 sets of 4x hang clean squat at 115lbs on 5/6"},
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    key = str(message.chat_id)
    assert pending[key]["stage"] == "awaiting_date"
    assert pending[key]["workout_date"] == "5/6"
    assert pending[key]["_date_option_a"] == "2026-05-06"
    assert pending[key]["_date_option_b"] == "2026-06-05"
    assert "Which date did you mean" in message.replies[-1][0]

    class _DummyQuery:
        def __init__(self, message):
            self.message = message

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, *args, **kwargs):
            pass

    asyncio.run(
        handlers.handle_cf_callback(
            _DummyQuery(message),
            ["cf", "date_pick", "a", key],
            None,
            SimpleNamespace(),
            {},
            pending,
        )
    )

    assert pending[key]["workout_date"] == "2026-05-06"
    assert pending[key]["stage"] == "notes"
    assert "_date_option_a" not in pending[key]
    assert "Any notes" in message.replies[-1][0]

def test_finalize_flow_uses_nlp_pending_state_keys(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    async def fake_week(notion):
        del notion
        return "week-1"

    def fake_create_strength_log(**kwargs):
        created.update(kwargs)
        return "log-1"

    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "create_strength_log", fake_create_strength_log)

    message = _DummyMessage()
    pending = {
        "123": {
            "mode": "strength",
            "movement_page_id": "mov-hang-clean",
            "movement_name": "Hang Squat Clean",
            "workout_date": "2026-05-06",
            "sets": 6,
            "reps": 4,
            "weight_lbs": 115.0,
            "is_max_attempt": False,
        }
    }

    asyncio.run(
        handlers._finalize_flow(
            message,
            "123",
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    assert created["workout_log_db_id"] == "workout-log"
    assert created["movement_page_id"] == ["mov-hang-clean"]
    assert created["movement_name"] == "Hang Squat Clean"
    assert created["load_lbs"] == 115.0
    assert created["effort_sets"] == 6
    assert created["effort_reps"] == 4
    assert created["workout_date"] == "2026-05-06"
    assert created["effort_scheme"] == "6x4"
    assert "Date: 2026-05-06" in message.replies[-2][0]
    assert "Scheme: 6x4" in message.replies[-2][0]
    assert "Weight: 115lbs" in message.replies[-2][0]
    assert pending[str(message.chat_id)]["stage"] == "awaiting_feel"
    assert pending[str(message.chat_id)]["last_workout_page_id"] == "log-1"
    assert "How did that session feel?" in message.replies[-1][0]


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
    assert cf_pending[key]["stage"] == "awaiting_feel"
    assert cf_pending[key]["last_wod_page_id"] == "wod-log"
    assert "How did that session feel?" in message.replies[-1][0]




def test_wod_finalize_resolves_state_movements_and_continues_on_failure(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}
    resolved = []

    async def fake_week(notion):
        del notion
        return "week-1"

    async def fake_query(notion, wod_log_db_id, workout_date, wod_format=None):
        del notion, wod_log_db_id, workout_date, wod_format
        return []

    def fake_get_or_create(notion, movements_db_id, name):
        del notion
        assert movements_db_id == "movements"
        if name == "Bad Match":
            raise RuntimeError("boom")
        page_id = f"mov-{name.lower().replace(' ', '-')}"
        resolved.append((name, page_id))
        return page_id

    def fake_create_wod_log(*args, **kwargs):
        created["args"] = args
        created["kwargs"] = kwargs
        return "wod-log"

    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "query_wod_log_by_date", fake_query)
    monkeypatch.setattr(handlers, "get_or_create_movement", fake_get_or_create)
    monkeypatch.setattr(handlers, "create_wod_log", fake_create_wod_log)

    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {
        key: {
            "mode": "wod",
            "stage": "rx_scaled",
            "format": "amrap",
            "movements": ["Row", "Bad Match", "Wall Ball"],
            "rx_scaled": "rx",
            "result_notes": "5 rounds",
            "workout_date": "2026-05-06",
        }
    }

    asyncio.run(
        handlers._finalize_flow(
            message,
            key,
            SimpleNamespace(),
            {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
            "5 rounds",
        )
    )

    assert resolved == [("Row", "mov-row"), ("Wall Ball", "mov-wall-ball")]
    assert created["args"][13] == ["mov-row", "mov-wall-ball"]
    assert pending[key]["last_wod_page_id"] == "wod-log"
    assert pending[key]["stage"] == "awaiting_feel"
    assert "🏋️ Row, Bad Match, Wall Ball" in message.replies[-2][0]

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
    assert cf_pending[key]["stage"] == "awaiting_feel"
    assert cf_pending[key]["last_wod_page_id"] == "wod-log"
    assert "WOD logged" in message.replies[-2][0]
    assert "How did that session feel?" in message.replies[-1][0]

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "feel", "4", key], None, SimpleNamespace(), {"NOTION_WOD_LOG_DB": "wod"}, cf_pending))

    assert key not in cf_pending
    assert q.edits[-1][0] == "✅ Session feel logged: 4/5"


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


def test_strength_pending_movement_stage_extracts_full_input_before_resolving(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    resolved = {}

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        resolved["text"] = text
        return ["mov-hang-squat-clean"], ["Hang Squat Clean"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    payload = '{"movements":["Hang Squat Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115.0,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "strength", "stage": "movement"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            "Did 6 sets of 4x hang clean squat at 115lbs on 5/6",
            key,
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending[key]
    assert resolved["text"] == "Hang Squat Clean"
    assert state["sets"] == 6
    assert state["reps"] == 4
    assert state["weight_lbs"] == 115.0
    assert state["weight_kg"] == 52.2
    assert state["workout_date"] == "5/6"
    assert state["raw_workout_date"] == "5/6"
    assert state["_date_option_a"] == "2026-05-06"
    assert state["_date_option_b"] == "2026-06-05"
    assert state["effort_scheme"] == "6x4"
    assert state["movement"] == "Hang Squat Clean"
    assert state["movement_page_id"] == "mov-hang-squat-clean"
    assert state["stage"] == "awaiting_date"
    assert "Which date did you mean?" in message.replies[-1][0]
    buttons = message.replies[-1][1]["reply_markup"].inline_keyboard[0]
    assert [button.text for button in buttons] == ["May 6", "Jun 5"]


def test_strength_pending_movement_stage_unambiguous_dates_go_to_notes(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        return ["mov-hang-squat-clean"], ["Hang Squat Clean"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    cases = [
        ("6 sets of 4x hang squat clean 115lbs on May 6", "2026-05-06"),
        ("6 sets of 4x hang squat clean 115lbs on 5/13", "2026-05-13"),
    ]
    payload = '{"movements":["Hang Squat Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115.0,"weight_kg":52.2,"scheme":"6x4","notes":null}'

    for raw_text, expected_date in cases:
        message = _DummyMessage()
        key = str(message.chat_id)
        pending = {key: {"mode": "strength", "stage": "movement"}}

        asyncio.run(
            handlers.handle_cf_text_reply(
                message,
                raw_text,
                key,
                _FakeClaude(payload),
                SimpleNamespace(),
                {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
                pending,
            )
        )

        state = pending[key]
        assert state["workout_date"] == expected_date
        assert state["stage"] == "notes"
        assert "_date_option_a" not in state
        assert "Any notes" in message.replies[-1][0]


def test_strength_date_pick_button_sets_option_and_prompts_for_notes():
    import second_brain.crossfit.handlers as handlers

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
    pending = {
        key: {
            "mode": "strength",
            "stage": "awaiting_date",
            "movement": "Hang Squat Clean",
            "sets": 6,
            "reps": 4,
            "weight_lbs": 115.0,
            "_date_option_a": "2026-05-06",
            "_date_option_b": "2026-06-05",
        }
    }

    asyncio.run(
        handlers.handle_cf_callback(
            _DummyQuery(message),
            ["cf", "date_pick", "a", key],
            None,
            SimpleNamespace(),
            {},
            pending,
        )
    )

    assert pending[key]["workout_date"] == "2026-05-06"
    assert pending[key]["stage"] == "notes"
    assert "_date_option_a" not in pending[key]
    assert "_date_option_b" not in pending[key]
    assert "Any notes" in message.replies[-1][0]


def test_strength_pending_movement_stage_handles_movement_only(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    resolved = {}

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        resolved["text"] = text
        return ["mov-hang-squat-clean"], ["Hang Squat Clean"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    payload = '{"movements":["Hang Squat Clean"],"date":null,"sets":null,"reps":null,"weight_lbs":null,"weight_kg":null,"scheme":null,"notes":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "strength", "stage": "movement"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            "hang squat clean",
            key,
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending[key]
    assert resolved["text"] == "Hang Squat Clean"
    assert state["sets"] is None
    assert state["reps"] is None
    assert state["weight_lbs"] is None
    assert state["weight_kg"] is None
    assert state["workout_date"] == "2026-05-08"
    assert state["effort_scheme"] is None
    assert state["movement"] == "Hang Squat Clean"
    assert state["stage"] == "notes"
    assert "Any notes" in message.replies[-1][0]


def test_strength_flow_prompts_for_ambiguous_raw_date_and_preserves_metadata(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Hang Squat Clean"
        return ["mov-hang-clean"], ["Hang Squat Clean"]

    async def fake_level_check(message, movement_id, movement_name, notion, config, cf_pending, key):
        del message, movement_id, movement_name, notion, config, cf_pending, key
        return False

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)
    monkeypatch.setattr(handlers, "handle_gymnastics_level_check", fake_level_check)

    payload = '{"movements":["Hang Squat Clean"],"date":"2026-05-06","sets":6,"reps":4,"weight_lbs":115,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    message = _DummyMessage()
    pending = {}

    asyncio.run(
        handlers.handle_cf_strength_flow(
            message,
            {"raw_text": "Did 6 sets of 4x hang clean squat at 115lbs on 5/6"},
            _FakeClaude(payload),
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    state = pending["123"]
    assert state["stage"] == "awaiting_date"
    assert state["sets"] == 6
    assert state["reps"] == 4
    assert state["weight_lbs"] == 115.0
    assert state["workout_date"] == "5/6"
    assert state["raw_workout_date"] == "5/6"
    assert state["_date_option_a"] == "2026-05-06"
    assert state["_date_option_b"] == "2026-06-05"
    assert "Which date did you mean?" in message.replies[-1][0]


def test_strength_date_pick_resolves_date_and_prompts_for_notes(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Hang Squat Clean"
        return ["mov-hang-clean"], ["Hang Squat Clean"]

    async def fake_level_check(message, movement_id, movement_name, notion, config, cf_pending, key):
        del message, movement_id, movement_name, notion, config, cf_pending, key
        return False

    async def fake_week(notion):
        del notion
        return "week-1"

    def fake_create_strength_log(**kwargs):
        created.update(kwargs)
        return "log-1"

    class _DummyQuery:
        def __init__(self, message):
            self.message = message
            self.edits = []

        async def answer(self, *args, **kwargs):
            pass

        async def edit_message_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)
    monkeypatch.setattr(handlers, "handle_gymnastics_level_check", fake_level_check)
    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "create_strength_log", fake_create_strength_log)

    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {
        key: {
            "mode": "strength",
            "stage": "date_pick",
            "stage_before_date_pick": "notes",
            "movement": "Hang Squat Clean",
            "movement_name": "Hang Squat Clean",
            "sets": 6,
            "reps": 4,
            "weight_lbs": 115.0,
            "weight_kg": 52.2,
            "effort_scheme": "6x4",
            "workout_date": "2026-05-06",
            "raw_date_a": "2026-05-06",
            "raw_date_b": "2026-06-05",
        }
    }

    q = _DummyQuery(message)
    asyncio.run(
        handlers.handle_cf_callback(
            q,
            ["cf", "date_pick", "a", key],
            None,
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    assert created == {}
    assert pending[key]["workout_date"] == "2026-05-06"
    assert pending[key]["stage"] == "notes"
    assert "raw_date_a" not in pending[key]
    assert "raw_date_b" not in pending[key]
    assert "Any notes" in message.replies[-1][0]


def test_crossfit_callback_collapses_skip_before_routing(monkeypatch):
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    async def fake_finalize(message, key, notion, config, cf_pending, notes):
        cf_pending.pop(key, None)
        await message.reply_text(f"finalized {key} notes={notes}")

    monkeypatch.setattr(handlers, "_finalize_flow", fake_finalize)
    message = _DummyMessage()
    key = str(message.chat_id)
    q = SimpleNamespace(
        message=message,
        edit_message_reply_markup=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    cf_pending = {key: {"mode": "strength", "stage": "notes"}}

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "skip", key], None, SimpleNamespace(), {}, cf_pending))

    q.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)
    assert key not in cf_pending
    assert message.replies[-1][0] == f"finalized {key} notes=None"


def test_crossfit_callback_collapses_date_pick_before_routing():
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    message = _DummyMessage()
    key = str(message.chat_id)
    q = SimpleNamespace(
        message=message,
        edit_message_reply_markup=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    cf_pending = {
        key: {
            "mode": "strength",
            "stage": "date_pick",
            "_date_option_a": "2026-05-06",
            "_date_option_b": "2026-06-05",
        }
    }

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "date_pick", "a", key], None, SimpleNamespace(), {}, cf_pending))

    q.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)
    assert cf_pending[key]["workout_date"] == "2026-05-06"
    assert cf_pending[key]["stage"] == "notes"
    assert "Any notes" in message.replies[-1][0]


def test_crossfit_callback_collapses_feel_before_routing():
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    message = _DummyMessage()
    key = str(message.chat_id)
    q = SimpleNamespace(
        message=message,
        edit_message_reply_markup=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    cf_pending = {key: {"mode": "wod", "stage": "awaiting_feel"}}

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "feel", "5", key], None, SimpleNamespace(), {}, cf_pending))

    q.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)
    q.edit_message_text.assert_awaited_once_with("✅ Session feel logged: 5/5", parse_mode="Markdown")
    assert key not in cf_pending


def test_crossfit_callback_collapses_menu_button_before_routing(monkeypatch):
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    async def fake_strength_flow(message, parsed, claude, notion, config, cf_pending):
        await message.reply_text("strength flow started")

    monkeypatch.setattr(handlers, "handle_cf_strength_flow", fake_strength_flow)
    message = _DummyMessage()
    q = SimpleNamespace(
        message=message,
        edit_message_reply_markup=AsyncMock(),
        edit_message_text=AsyncMock(),
    )

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "log_strength"], None, SimpleNamespace(), {}, {}))

    q.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)
    assert message.replies[-1][0] == "strength flow started"



def test_wod_movement_stage_extracts_spelled_date_from_initial_text(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        return ["mov-wall-walks"], ["Wall Walks"]

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)

    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "wod", "stage": "movement", "format": "amrap"}}

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            "This log is for May 6th, 3x wall walks",
            key,
            None,
            SimpleNamespace(),
            {"NOTION_WOD_LOG_DB": "wod", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    assert pending[key]["workout_date"] == "2026-05-06"
    assert pending[key]["raw_workout_date"] == "May 6th"
    assert pending[key]["stage"] == "time_cap"
    assert "Which date did you mean?" not in [reply[0] for reply in message.replies]

def test_wod_duplicate_guard_warns_and_still_logs(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    created = {}

    async def fake_week(notion):
        del notion
        return "week-1"

    async def fake_query(notion, wod_log_db_id, workout_date, wod_format=None):
        del notion
        assert wod_log_db_id == "wod"
        assert workout_date == "2026-05-06"
        assert wod_format == "AMRAP"
        return [{"id": "existing-wod"}]

    def fake_create_wod_log(*args, **kwargs):
        created["args"] = args
        created["kwargs"] = kwargs
        return "new-wod"

    monkeypatch.setattr(handlers, "get_current_week_program_url", fake_week)
    monkeypatch.setattr(handlers, "query_wod_log_by_date", fake_query)
    monkeypatch.setattr(handlers, "create_wod_log", fake_create_wod_log)

    message = _DummyMessage()
    key = str(message.chat_id)
    assert cf_pending[key]["mode"] == "feel_only"
    assert cf_pending[key]["stage"] == "awaiting_feel"
    assert cf_pending[key]["workout_date"] == handlers.date.today().isoformat()
    assert message.replies[-1][0] == "💬 How did that session feel?"
    feel_keyboard = message.replies[-1][1]["reply_markup"].inline_keyboard
    assert [button.callback_data for button in feel_keyboard[0]] == [
        f"cf:feel:1:{key}",
        f"cf:feel:2:{key}",
        f"cf:feel:3:{key}",
        f"cf:feel:4:{key}",
        f"cf:feel:5:{key}",
    ]


def test_crossfit_strength_feel_updates_workout_log_and_training_log():
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    page_updates = []
    training_queries = []
    notion = SimpleNamespace(
        databases=SimpleNamespace(
            query=lambda **kwargs: training_queries.append(kwargs) or {"results": [{"id": "training-row"}]}
        ),
        pages=SimpleNamespace(
            update=lambda **kwargs: page_updates.append(kwargs),
            create=lambda **kwargs: None,
        ),
    )
    message = _DummyMessage()
    q = SimpleNamespace(message=message, edit_message_reply_markup=AsyncMock(), edit_message_text=AsyncMock())
    key = str(message.chat_id)
    pending = {
        key: {
            "mode": "strength",
            "stage": "awaiting_feel",
            "workout_date": "2026-05-06",
            "last_workout_page_id": "workout-page",
        }
    }

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "feel", "4", key], None, notion, {"NOTION_DAILY_READINESS_DB": "training-db"}, pending))

    assert page_updates == [
        {"page_id": "workout-page", "properties": {"Strength Feel": {"select": {"name": "4"}}}},
        {"page_id": "training-row", "properties": {"Strength Feel": {"select": {"name": "4"}}}},
    ]
    assert training_queries == [{"database_id": "training-db", "filter": {"property": "Date", "date": {"equals": "2026-05-06"}}, "page_size": 1}]
    assert key not in pending
    q.edit_message_text.assert_awaited_once_with("✅ Session feel logged: 4/5", parse_mode="Markdown")


def test_crossfit_wod_feel_updates_wod_log_and_creates_training_log_row():
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    page_updates = []
    page_creates = []
    notion = SimpleNamespace(
        databases=SimpleNamespace(query=lambda **kwargs: {"results": []}),
        pages=SimpleNamespace(
            update=lambda **kwargs: page_updates.append(kwargs),
            create=lambda **kwargs: page_creates.append(kwargs) or {"id": "new-training-row"},
        ),
    )
    message = _DummyMessage()
    q = SimpleNamespace(message=message, edit_message_reply_markup=AsyncMock(), edit_message_text=AsyncMock())
    key = str(message.chat_id)
    pending = {
        key: {
            "mode": "wod",
            "stage": "awaiting_feel",
            "workout_date": "2026-05-07",
            "last_wod_page_id": "wod-page",
        }
    }

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "feel", "5", key], None, notion, {"NOTION_DAILY_READINESS_DB": "training-db"}, pending))

    assert page_updates == [
        {"page_id": "wod-page", "properties": {"WOD Feel": {"select": {"name": "5"}}}},
    ]
    assert page_creates == [{
        "parent": {"database_id": "training-db"},
        "properties": {
            "Name": {"title": [{"text": {"content": "2026-05-07 — Training"}}]},
            "Date": {"date": {"start": "2026-05-07"}},
            "WOD Feel": {"select": {"name": "5"}},
        },
    }]
    assert key not in pending
    q.edit_message_text.assert_awaited_once_with("✅ Session feel logged: 5/5", parse_mode="Markdown")


def test_crossfit_standalone_feel_updates_training_log_only():
    import second_brain.crossfit.handlers as handlers
    from unittest.mock import AsyncMock

    page_updates = []
    notion = SimpleNamespace(
        databases=SimpleNamespace(query=lambda **kwargs: {"results": [{"id": "training-row"}]}),
        pages=SimpleNamespace(
            update=lambda **kwargs: page_updates.append(kwargs),
            create=lambda **kwargs: None,
        ),
    )
    message = _DummyMessage()
    q = SimpleNamespace(message=message, edit_message_reply_markup=AsyncMock(), edit_message_text=AsyncMock())
    key = str(message.chat_id)
    pending = {key: {"mode": "feel_only", "stage": "awaiting_feel", "workout_date": "2026-05-08"}}

    asyncio.run(handlers.handle_cf_callback(q, ["cf", "feel", "3", key], None, notion, {"NOTION_DAILY_READINESS_DB": "training-db"}, pending))

    assert page_updates == [
        {"page_id": "training-row", "properties": {"Workout Feel": {"select": {"name": "3"}}}},
    ]
    assert key not in pending
    q.edit_message_text.assert_awaited_once_with("✅ Session feel logged: 3/5", parse_mode="Markdown")

def test_parse_programme_text_splits_days_and_tracks_without_claude():
    from second_brain.crossfit.classify import parse_programme

    text = """
MONDAY
PERFORMANCE
B. 5x5 Back Squat
C. AMRAP 12
10 Wall Balls
FITNESS
B. Skill Handstand Push-Up
C. For Time
21-15-9 Burpees
WEDNESDAY
PERFORMANCE
B. EMOM 10 Hang Squat Clean
C. Chipper
Run 400m
FITNESS
B. 4x8 Front Squat
C. Intervals
Every 3 minutes Row
"""
    parsed = parse_programme(text, _FakeClaude('{"should":"not call"}'), "model", 1000)
    perf = next(t for t in parsed["tracks"] if t["track"] == "Performance")
    fit = next(t for t in parsed["tracks"] if t["track"] == "Fitness")
    assert [d["day"] for d in perf["days"]] == ["Monday", "Wednesday"]
    assert [d["day"] for d in fit["days"]] == ["Monday", "Wednesday"]
    wed_perf = next(d for d in perf["days"] if d["day"] == "Wednesday")
    assert wed_perf["section_b"]["description"].startswith("EMOM 10")
    assert wed_perf["section_c"]["format"] == "Chipper"


def test_save_programme_links_week_cycle_and_section_movements_from_text():
    from second_brain.crossfit.notion import save_programme

    created = []
    updated = []

    def query(**kwargs):
        db = kwargs["database_id"]
        if db == "movements":
            return {"results": [
                {"id": "mov-back-squat", "properties": {"Name": {"title": [{"plain_text": "Back Squat"}]}}},
                {"id": "mov-burpee", "properties": {"Name": {"title": [{"plain_text": "Burpee"}]}}},
            ]}
        if db == "cycles" and kwargs.get("filter", {}).get("property") == "End Date":
            return {"results": [{"id": "cycle-open", "properties": {"Name": {"title": [{"plain_text": "Cycle 3"}]}}}]}
        if db == "program" and kwargs.get("filter", {}).get("property") == "Cycle":
            return {"results": [{"id": "week-old"}, {"id": "week-old-2"}]}
        return {"results": []}

    notion = SimpleNamespace(
        pages=SimpleNamespace(
            create=lambda **kwargs: created.append(kwargs) or {"id": "parent" if kwargs["parent"]["database_id"] == "program" else f"row-{len(created)}"},
            update=lambda **kwargs: updated.append(kwargs) or {"id": kwargs.get("page_id")},
        ),
        databases=SimpleNamespace(query=query),
    )
    parsed = {
        "week_label": "Week of 2026-05-04",
        "tracks": [{"track": "Performance", "days": [{"day": "Wednesday", "section_b": {"description": "5x5 Back Squat", "movements": []}, "section_c": {"description": "AMRAP 8 Burpees", "format": "AMRAP", "movements": []}}]}],
    }
    save_programme(notion, "program", "days", "movements", parsed, "raw", "cycles")
    parent_props = created[0]["properties"]
    assert parent_props["Cycle"] == {"relation": [{"id": "cycle-open"}]}
    assert parent_props["Week"] == {"number": 3}
    day_props = created[1]["properties"]
    assert day_props["Day"] == {"select": {"name": "Wednesday"}}
    assert day_props["Track"] == {"select": {"name": "Performance"}}
    assert day_props["Week Of"] == {"date": {"start": "2026-05-04"}}
    assert day_props["Section B Movements"] == {"relation": [{"id": "mov-back-squat"}]}
    assert day_props["Section C Movements"] == {"relation": [{"id": "mov-burpee"}]}

def test_save_programme_from_notion_row_returns_days_and_section_movement_ids():
    from second_brain.crossfit.notion import save_programme_from_notion_row

    created = []
    updated = []

    def query(**kwargs):
        if kwargs["database_id"] == "movements":
            return {"results": [
                {"id": "mov-back-squat", "properties": {"Name": {"title": [{"plain_text": "Back Squat"}]}}},
                {"id": "mov-burpee", "properties": {"Name": {"title": [{"plain_text": "Burpee"}]}}},
            ]}
        return {"results": []}

    notion = SimpleNamespace(
        pages=SimpleNamespace(
            create=lambda **kwargs: created.append(kwargs) or {"id": f"row-{len(created)}"},
            update=lambda **kwargs: updated.append(kwargs) or {"id": kwargs.get("page_id")},
        ),
        databases=SimpleNamespace(query=query),
    )
    parsed = {
        "week_label": "Week of 2026-05-04",
        "tracks": [{"track": "Performance", "days": [{
            "day": "Monday",
            "section_b": {"description": "5x5 Back Squat", "movements": []},
            "section_c": {"description": "AMRAP 8 Burpees", "format": "AMRAP", "movements": []},
        }]}],
    }

    result = save_programme_from_notion_row(notion, "week-page", "days", "movements", parsed, "program", "")

    assert result == {"days_created": 1, "movement_ids": ["mov-back-squat", "mov-burpee"]}
    day_props = created[0]["properties"]
    assert day_props["Section B Movements"] == {"relation": [{"id": "mov-back-squat"}]}
    assert day_props["Section C Movements"] == {"relation": [{"id": "mov-burpee"}]}


def _workout_row(load, sets, reps, est_1rm, workout_date):
    return {
        "properties": {
            "load_lbs": {"number": load},
            "effort_sets": {"number": sets},
            "effort_reps": {"number": reps},
            "calc_1rm_brzycki": {"formula": {"type": "number", "number": est_1rm}},
            "Date": {"date": {"start": workout_date}},
        }
    }


def test_my_prs_target_reps_uses_workout_log_v2_and_clears_state(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_match(notion, config, movement_text, threshold=0.70):
        del notion, config
        assert movement_text == "back squat"
        assert threshold == 0.80
        return "Back Squat", "mov-back-squat"

    captured = {}

    def query(**kwargs):
        captured.update(kwargs)
        return {"results": [
            _workout_row(245, 1, 3, 266, "2026-05-06"),
            _workout_row(235, 1, 4, 261, "2026-04-28"),
            _workout_row(225, 1, 5, 253, "2026-04-14"),
        ]}

    monkeypatch.setattr(handlers, "_match_movement_from_cache", fake_match)
    message = _DummyMessage()
    pending = {str(message.chat_id): {"mode": "prs", "stage": "movement"}}
    notion = SimpleNamespace(databases=SimpleNamespace(query=query))

    asyncio.run(
        handlers.handle_cf_prs_reply(
            message,
            "6x back squat",
            notion,
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    assert captured["database_id"] == "workout-log"
    assert captured["filter"] == {"property": "Movement", "relation": {"contains": "mov-back-squat"}}
    assert captured["sorts"] == [{"property": "load_lbs", "direction": "descending"}]
    assert captured["page_size"] == 10
    assert str(message.chat_id) not in pending
    reply = message.replies[-1][0]
    assert "Back Squat — 6 Rep Target" in reply
    assert "Suggested for 6 reps:* 225 lbs (85% of 1RM)" in reply
    assert "Best logged:* 245 lbs × 3 reps" in reply


def test_my_prs_no_match_keeps_state(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_match(notion, config, movement_text, threshold=0.70):
        del notion, config, movement_text, threshold
        return None

    monkeypatch.setattr(handlers, "_match_movement_from_cache", fake_match)
    message = _DummyMessage()
    pending = {str(message.chat_id): {"mode": "prs", "stage": "movement"}}

    asyncio.run(
        handlers.handle_cf_prs_reply(
            message,
            "xyz",
            SimpleNamespace(),
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    assert pending[str(message.chat_id)] == {"mode": "prs", "stage": "movement"}
    assert "Couldn't find that movement" in message.replies[-1][0]


def test_strength_movement_reply_prepends_recent_pr_context(monkeypatch):
    import second_brain.crossfit.handlers as handlers

    async def fake_resolve(text, claude, notion, config, message=None):
        del claude, notion, config, message
        assert text == "Hang Squat Clean"
        return ["mov-hsc"], ["Hang Squat Clean"]

    rows = [
        _workout_row(115, 6, 4, 130, "2026-05-06"),
        _workout_row(110, 5, 5, 128, "2026-04-28"),
        _workout_row(105, 4, 6, 126, "2026-04-14"),
    ]

    def query(**kwargs):
        assert kwargs["database_id"] == "workout-log"
        assert kwargs["sorts"] == [{"property": "Date", "direction": "descending"}]
        assert kwargs["page_size"] == 3
        return {"results": rows}

    monkeypatch.setattr(handlers, "_resolve_movement_ids", fake_resolve)
    payload = '{"movements":["Hang Squat Clean"],"date":null,"sets":6,"reps":4,"weight_lbs":115,"weight_kg":52.2,"scheme":"6x4","notes":null}'
    message = _DummyMessage()
    key = str(message.chat_id)
    pending = {key: {"mode": "strength", "stage": "movement"}}
    notion = SimpleNamespace(databases=SimpleNamespace(query=query))

    asyncio.run(
        handlers.handle_cf_text_reply(
            message,
            "6 sets of 4x hang squat clean 115lbs",
            key,
            _FakeClaude(payload),
            notion,
            {"NOTION_WORKOUT_LOG_DB": "workout-log", "NOTION_MOVEMENTS_DB": "movements"},
            pending,
        )
    )

    reply = message.replies[-1][0]
    assert reply.startswith("📊 *Hang Squat Clean — recent*")
    assert "• 2026-05-06 — 115 lbs × 6×4" in reply
    assert "🧮 Est. 1RM: 130 lbs" in reply
    assert "Any notes about this session" in reply
