import os

import asyncio

os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("NOTION_DB_ID", "test-db")
os.environ.setdefault("MY_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_CHAT_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault("NOTION_HABIT_DB", "test-db")
os.environ.setdefault("NOTION_LOG_DB", "test-db")
os.environ.setdefault("NOTION_NOTES_DB", "test-db")
os.environ.setdefault("NOTION_DIGEST_SELECTOR_DB", "test-db")

from second_brain import trips


class _Message:
    def __init__(self):
        self.sent = []

    async def reply_text(self, text):
        self.sent.append(text)


class _Query:
    def __init__(self):
        self.message = _Message()


def _trip_map():
    return {
        "0": {
            "destinations": ["Nashville TN"],
            "departure_date": "2026-05-14",
            "return_date": "2026-05-17",
            "duration_label": "2-3 Days",
            "purpose": "Work",
            "field_work_types": ["Site Survey"],
            "multiple_sites": True,
            "checked_luggage": False,
        }
    }


def test_execute_trip_saves_to_notion(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edbf406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)

    notion = type("Notion", (), {"pages": _NotionPages()})()
    flag = {"value": False}

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: flag.update(value=value),
        fetch_weather=lambda _: None,
    ))

    assert created["parent"]["database_id"] == "57f9edbf-406d-4368-b32d-23f0ea2a0c66"
    assert created["properties"]["Trip"]["title"][0]["text"]["content"].startswith("Nashville TN")
    assert flag["value"] is True
    assert any("Trip saved to Notion" in msg for msg in query.message.sent)


def test_parse_trip_message_falls_back_when_nlp_unavailable():
    parsed = trips.parse_trip_message("work and personal trip to Nashville TN, May 14-17", claude=None)

    assert parsed["destinations"] == ["Nashville TN"]
    assert parsed["purpose"] == "Both"
    assert parsed["departure_date"] is None
    assert parsed["return_date"] is None


def test_normalize_notion_database_id():
    assert trips._normalize_notion_database_id("c57f9edbf406d4368b32d23f0ea2a0c66") == "57f9edbf-406d-4368-b32d-23f0ea2a0c66"
    assert trips._normalize_notion_database_id("57f9edbf-406d-4368-b32d-23f0ea2a0c66") == "57f9edbf-406d-4368-b32d-23f0ea2a0c66"
    assert trips._normalize_notion_database_id("bad-id") == ""
