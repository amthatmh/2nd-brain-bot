"""Global pytest configuration that makes the suite deterministic and
order-independent.

Hazard addressed here: frozen config values. ``second_brain.config`` reads
required env vars at import time and freezes them into module-level constants.
Whichever test imported config first — under its own patched environment —
decided those values for the entire session (e.g. ``NOTION_DB_ID`` ending up
as ``"x"`` instead of ``"test-db"``, which made test_trips fail only when it
ran after a reload-based test). We set canonical env vars here, at conftest
import time (pytest imports conftest before any test module), so config always
freezes to the same values regardless of collection order.

(The other isolation hazard — a stale ``second_brain.notion.tasks`` reference
held by ``second_brain.digest`` after a reload — is handled at its source in
``tests/test_habits.py.load_main_module``, which reloads ``digest`` alongside
``main`` so both re-bind the current module object.)
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
