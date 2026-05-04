import importlib
import os
import sys
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


def test_restore_pid_handles_compact_and_dashed_inputs():
    main = load_main_module()
    compact = "34b302e9131d81f781d7d6029141dd47"
    dashed = "34b302e9-131d-81f7-81d7-d6029141dd47"
    assert main._restore_pid(compact) == dashed
    assert main._restore_pid(dashed) == dashed
    assert main._restore_pid("manual") == "manual"
