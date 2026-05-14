import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("NOTION_DB_ID", "test-db")
os.environ.setdefault("TELEGRAM_CHAT_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault("NOTION_HABIT_DB", "test-db")
os.environ.setdefault("NOTION_LOG_DB", "test-db")
os.environ.setdefault("NOTION_NOTES_DB", "test-db")
os.environ.setdefault("NOTION_DIGEST_SELECTOR_DB", "test-db")
os.environ.setdefault("NOTION_STREAK_DB", "test-db")
os.environ.setdefault("OPENWEATHER_KEY", "test-openweather-key")

from second_brain import formatters as fmt
from second_brain import weather as wx


def _daily_rows():
    return [
        {
            "date": (datetime.now(wx.TZ).date() + timedelta(days=i)).isoformat(),
            "temp_high": 20 + i,
            "temp_low": 10 + i,
            "condition": "Clouds",
            "description": "Clouds",
            "precip_chance": i * 10,
            "uvi": 4.0,
            "sunrise": datetime.now(wx.TZ).replace(hour=6, minute=0).isoformat(),
            "sunset": datetime.now(wx.TZ).replace(hour=20, minute=0).isoformat(),
        }
        for i in range(5)
    ]


def test_digest_weather_card_uses_shared_five_day_daily_pull(monkeypatch):
    calls = []

    def fake_daily(days=5, force_refresh=False):
        calls.append((days, force_refresh))
        return _daily_rows()[:days]

    monkeypatch.setattr(wx, "current_location", "Chicago, Illinois, US")
    monkeypatch.setattr(fmt, "digest_location_label", lambda: "Chicago")
    monkeypatch.setattr(wx, "fetch_daily_weather", fake_daily)
    monkeypatch.setattr(wx, "fetch_weather", lambda forecast_type="current": {"condition": "Clouds", "temp": 14})
    monkeypatch.setattr(fmt, "_should_show_uv_guidance", lambda *args, **kwargs: True)

    message = fmt.format_digest_weather_card()

    assert calls == [(5, False)]
    assert "📍 Chicago · ☁️ Clouds" in message
    assert "🌡️ 20°C / 10°C" in message


def test_weather_snapshot_next_five_days_are_celsius(monkeypatch):
    monkeypatch.setattr(wx, "current_location", "Chicago, Illinois, US")
    monkeypatch.setattr(wx, "fetch_weather", lambda forecast_type="current": {"condition": "Clouds", "temp": 14})
    monkeypatch.setattr(wx, "fetch_daily_weather", lambda days=5, force_refresh=False: _daily_rows()[:days])
    monkeypatch.setattr(fmt, "_should_show_uv_guidance", lambda *args, **kwargs: False)

    message = fmt.format_weather_snapshot()
    next_five_days = message.split("📆 Next 5 Days", 1)[1]

    assert "20°C/10°C" in next_five_days
    assert "68°/50°" not in next_five_days


def test_fetch_daily_weather_reuses_cached_one_call_data(monkeypatch):
    monkeypatch.setattr(wx, "OPENWEATHER_KEY", "test-openweather-key")
    wx.current_lat = 41.88
    wx.current_lon = -87.63
    wx.clear_weather_cache()
    calls = []
    base_dt = int(datetime(2026, 5, 5, tzinfo=timezone.utc).timestamp())

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "daily": [
                    {
                        "dt": base_dt + (86400 * i),
                        "temp": {"max": 20 + i, "min": 10 + i},
                        "weather": [{"main": "Clouds", "description": "clouds"}],
                        "pop": 0.1,
                        "uvi": 4,
                    }
                    for i in range(5)
                ]
            }

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse()

    monkeypatch.setattr(wx.httpx, "get", fake_get)

    digest_today = wx.fetch_daily_weather(days=1)
    weather_five_days = wx.fetch_daily_weather(days=5)

    assert len(calls) == 1
    assert len(digest_today) == 1
    assert len(weather_five_days) == 5
    assert weather_five_days[0] == digest_today[0]


def test_fetch_multi_day_forecast_buckets_three_hour_rows(monkeypatch):
    monkeypatch.setattr(wx, "OPENWEATHER_KEY", "test-openweather-key")
    wx.current_lat = 41.88
    wx.current_lon = -87.63
    base_day = datetime.now(wx.TZ).date()
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            rows = []
            for day_offset in range(2):
                for hour, temp, pop, condition in [
                    (9, 10 + day_offset, 0.1, "Clouds"),
                    (15, 20 + day_offset, 0.6, "Rain"),
                ]:
                    dt = datetime.combine(
                        base_day + timedelta(days=day_offset),
                        datetime.min.time(),
                        tzinfo=wx.TZ,
                    ).replace(hour=hour)
                    rows.append(
                        {
                            "dt": int(dt.astimezone(timezone.utc).timestamp()),
                            "main": {"temp_max": temp, "temp_min": temp - 2},
                            "pop": pop,
                            "weather": [{"main": condition, "description": condition.lower()}],
                        }
                    )
            return {"list": rows}

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse()

    monkeypatch.setattr(wx.httpx, "get", fake_get)

    rows = wx.fetch_multi_day_forecast(2)

    assert len(calls) == 1
    assert rows is not None
    assert len(rows) == 2
    assert rows[0]["temp_high"] == 20
    assert rows[0]["precip_chance"] == 60


def test_palette_digest_view_includes_weather_card():
    from second_brain import palette

    class FakeTasks:
        @staticmethod
        def get_all_active_tasks(notion, notion_db_id):
            return []

        @staticmethod
        def _parse_deadline(deadline):
            return None

        @staticmethod
        def _task_sort_key(task):
            return task.get("name", "")

        @staticmethod
        def _context_label(task):
            return ""

    message, _keyboard = palette.format_digest_view(
        notion_tasks=FakeTasks,
        notion=None,
        notion_db_id="test-db",
        local_today_fn=lambda: datetime.now(wx.TZ).date(),
        back_to_palette_keyboard=lambda: None,
        weather_card="📍 Chicago · ☁️ Clouds\n🌡️ 20°C / 10°C",
    )

    assert "📍 Chicago · ☁️ Clouds" in message
    assert message.index("📍 Chicago") < message.index("✅ Clear for next 7 days!")
