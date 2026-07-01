"""Global pytest configuration.

``second_brain.config`` reads required env vars at import time and fails hard
if any are missing. This provides a baseline so that importing config never
KeyErrors during local collection (CI supplies its own values via the workflow
``env:`` block). ``setdefault`` is used so it never overrides an env that CI or
a developer has already set — tests that need a specific value pin it on the
module under test (e.g. via ``monkeypatch.setattr``) rather than relying on
these defaults.

(Two related isolation hazards are fixed at their source, not here: a stale
``second_brain.notion.tasks`` reference held by ``second_brain.digest`` after a
reload is handled in ``tests/test_habits.py.load_main_module`` (which reloads
``digest`` alongside ``main``); and ``tests/test_trips.py`` pins
``trips.NOTION_DB_ID`` directly so its assertion is independent of the frozen
config value.)
"""

import os

_CANONICAL_ENV = {
    "TELEGRAM_TOKEN": "test-token",
    "TELEGRAM_CHAT_ID": "1",
    "MY_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "test-key",
    "NOTION_TOKEN": "test-token",
    "NOTION_DB_ID": "test-db",
    "NOTION_HABIT_DB": "test-db",
    "NOTION_LOG_DB": "test-db",
    "NOTION_STREAK_DB": "test-db",
    "NOTION_CINEMA_LOG_DB": "test-db",
    "NOTION_NOTES_DB": "test-db",
    "NOTION_DIGEST_SELECTOR_DB": "test-db",
}
for _key, _value in _CANONICAL_ENV.items():
    os.environ.setdefault(_key, _value)
