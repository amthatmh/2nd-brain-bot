import os

import asyncio

os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("NOTION_DB_ID", "test-db")
os.environ.setdefault("MY_CHAT_ID", "1")

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
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "abc123")
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

    assert created["parent"]["database_id"] == "abc123"
    assert created["properties"]["Trip"]["title"][0]["text"]["content"].startswith("Nashville TN")
    assert flag["value"] is True
    assert any("Trip saved to Notion" in msg for msg in query.message.sent)
