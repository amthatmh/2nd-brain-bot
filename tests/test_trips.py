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
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Duration": {"type": "select"},
                    "Purpose": {"type": "select"},
                    "Field Work": {"type": "rich_text"},
                    "Multiple Sites": {"type": "checkbox"},
                    "Checked Luggage": {"type": "checkbox"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()
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

    assert created["parent"]["database_id"] == "c57f9edb-406d-4368-b32d-23f0ea2a0c66"
    assert created["properties"]["Trip"]["title"][0]["text"]["content"].startswith("Nashville TN")
    assert created["properties"]["Field Work"]["rich_text"][0]["text"]["content"] == "Site Survey"
    assert flag["value"] is True
    assert any("Trip saved to Notion" in msg for msg in query.message.sent)


def test_parse_trip_message_falls_back_when_nlp_unavailable():
    parsed = trips.parse_trip_message("work and personal trip to Nashville TN, May 14-17", claude=None)

    assert parsed["destinations"] == ["Nashville TN"]
    assert parsed["purpose"] == "Both"
    assert parsed["departure_date"] is None
    assert parsed["return_date"] is None


def test_normalize_notion_database_id():
    assert trips._normalize_notion_database_id("c57f9edbf406d4368b32d23f0ea2a0c66") == ""
    assert trips._normalize_notion_database_id("c57f9edb406d4368b32d23f0ea2a0c66") == "c57f9edb-406d-4368-b32d-23f0ea2a0c66"
    assert trips._normalize_notion_database_id("bad-id") == ""


def test_execute_trip_uses_field_work_types_when_present(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Purpose": {"type": "select"},
                    "Field Work Types": {"type": "rich_text"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: None,
        fetch_weather=lambda _: None,
    ))

    assert "Field Work Types" in created["properties"]
    assert created["properties"]["Field Work Types"]["rich_text"][0]["text"]["content"] == "Site Survey"


def test_execute_trip_maps_weather_flags_to_multi_select(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Purpose": {"type": "select"},
                    "Weather Flags": {"type": "multi_select"},
                    "Weather Summary": {"type": "rich_text"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: None,
        fetch_weather=lambda bucket: {"condition": "Rain", "temp_high": 31, "temp_low": 4, "precip_chance": 80} if bucket == "today" else None,
    ))

    tags = [item["name"] for item in created["properties"]["Weather Flags"]["multi_select"]]
    assert set(tags) == {"Rain", "Hot", "Cold"}


def test_trip_weather_summary_uses_trip_date_range():
    summary, flags = trips._build_trip_weather_summary(
        "2026-05-14",
        "2026-05-17",
        "Nashville, TN",
        fetch_weather=lambda _: {"condition": "Rain", "temp_high": 31, "temp_low": 4, "precip_chance": 80},
        fetch_trip_weather_range=lambda dep, ret, dest: [
            {"label": "Thu May 14", "condition": "Clear", "temp_high": 24, "temp_low": 14, "precip_chance": 0},
            {"label": "Fri May 15", "condition": "Rain", "temp_high": 20, "temp_low": 10, "precip_chance": 70},
        ],
    )
    assert "Thu May 14" in summary
    assert "Fri May 15" in summary
    assert "Rain" in flags


def test_refresh_upcoming_trip_weather_updates_rows():
    updated_pages = []

    class _NotionDatabases:
        def query(self, **kwargs):
            return {
                "results": [
                    {
                        "id": "page-1",
                        "properties": {
                            "Departure Date": {"date": {"start": "2026-05-06"}},
                            "Return Date": {"date": {"start": "2026-05-08"}},
                            "Destination(s)": {"rich_text": [{"plain_text": "Nashville, TN"}]},
                            "Weather Flags": {"type": "multi_select"},
                            "Weather Summary": {"type": "rich_text"},
                        },
                    }
                ]
            }

        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Weather Flags": {"type": "multi_select"},
                    "Weather Summary": {"type": "rich_text"},
                }
            }

    class _NotionPages:
        def update(self, **kwargs):
            updated_pages.append(kwargs)

    notion = type("Notion", (), {"databases": _NotionDatabases(), "pages": _NotionPages()})()
    count = trips.refresh_upcoming_trip_weather(
        notion,
        "c57f9edb406d4368b32d23f0ea2a0c66",
        fetch_trip_weather_range=lambda *_: [{"label": "Thu May 6", "condition": "Rain", "temp_high": 30, "temp_low": 10, "precip_chance": 80}],
        lookahead_days=5,
    )
    assert count == 1
    assert updated_pages
