"""The habit multi-select selector must not collapse to a single habit.

Regression: after a process restart the in-memory selection session is lost, so
tapping a habit fell back to recovering the list from the message markup. That
recovery dropped any button not present in the (possibly stale) habit cache,
leaving only the tapped habit visible — so multiple habits could never be
selected. Recovery must instead preserve every habit button regardless of cache
state, and read prior selections from the visible checkmarks.
"""

from types import SimpleNamespace
from unittest.mock import patch

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import second_brain.routers as routers


def _message(buttons):
    rows = [[InlineKeyboardButton(text, callback_data=cb)] for text, cb in buttons]
    return SimpleNamespace(reply_markup=InlineKeyboardMarkup(rows))


def test_recovery_preserves_all_habits_when_cache_is_empty():
    msg = _message([
        ("✅ 💊 Allergy Meds", "h:toggle:aaaa"),
        ("💧 Water", "h:toggle:bbbb"),
        ("🏃 Run", "h:toggle:cccc"),
    ])
    fake_main = SimpleNamespace(_restore_pid=lambda p: p)

    # Empty cache simulates a post-restart / mismatched habit cache.
    with patch.object(routers, "_main", return_value=fake_main), \
        patch.object(routers, "_habit_cache", return_value={}):
        habits = routers._habits_from_message_markup(msg)
        selected = routers._selected_pids_from_message_markup(msg)

    assert [h["page_id"] for h in habits] == ["aaaa", "bbbb", "cccc"]
    # Names are reconstructed from the button labels, ✅ marker stripped.
    assert [h["name"] for h in habits] == ["💊 Allergy Meds", "💧 Water", "🏃 Run"]
    # Prior selection is recovered from the visible checkmark.
    assert selected == {"aaaa"}


def test_recovery_prefers_cache_metadata_when_available():
    msg = _message([
        ("💧 Water", "h:toggle:bbbb"),
        ("🏃 Run", "h:toggle:cccc"),
    ])
    cached = {
        "b": {"page_id": "bbbb", "name": "💧 Water", "sort": 2, "auto_only": False},
        "c": {"page_id": "cccc", "name": "🏃 Run", "sort": 1, "auto_only": False},
    }
    fake_main = SimpleNamespace(_restore_pid=lambda p: p)

    with patch.object(routers, "_main", return_value=fake_main), \
        patch.object(routers, "_habit_cache", return_value=cached):
        habits = routers._habits_from_message_markup(msg)

    # Full metadata dicts from the cache are used (order follows the markup).
    assert habits[0]["sort"] == 2 and habits[1]["sort"] == 1
    assert [h["page_id"] for h in habits] == ["bbbb", "cccc"]
