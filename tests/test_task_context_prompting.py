import importlib
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


REQUIRED_ENV = {
    "TELEGRAM_TOKEN": "x",
    "TELEGRAM_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "x",
    "NOTION_TOKEN": "x",
    "NOTION_DB_ID": "x",
    "NOTION_HABIT_DB": "x",
    "NOTION_LOG_DB": "x",
    "NOTION_STREAK_DB": "x",
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
}


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


def test_classify_task_prompt_includes_ambiguity_guidance():
    main = load_main_module()

    captured_prompt = {}

    def _fake_create(**kwargs):
        captured_prompt["text"] = kwargs["messages"][0]["content"]
        payload = {"task_name": "x", "deadline_days": None, "context": "🏠 Personal", "confidence": "low", "recurring": "None", "repeat_day": None}
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(payload))])

    main.claude.messages.create = _fake_create
    main.classify_task("remind me about this")

    assert "If context is ambiguous between categories, set confidence to low." in captured_prompt["text"]


def test_task_context_keyboard_uses_context_callback_prefix():
    main = load_main_module()
    keyboard = main.task_context_keyboard("42")
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]

    assert callbacks == ["nc:42:p", "nc:42:w", "nc:42:h", "nc:42:c"]
