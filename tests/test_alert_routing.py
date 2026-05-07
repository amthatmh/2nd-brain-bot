from utils import alerts
from utils.alert_handlers import alert_cinema_sync_complete, alert_startup


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True, "result": {"message_id": 123}}


def test_send_alert_uses_alert_channel_id_not_owner_chat(monkeypatch):
    calls = []
    monkeypatch.setenv("TELEGRAM_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42582324")
    monkeypatch.setenv("ALERT_CHANNEL_ID", "-1003840996802")
    monkeypatch.delenv("TELEGRAM_ALERT_CHAT_ID", raising=False)

    def fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr(alerts.httpx, "post", fake_post)

    assert alerts.send_alert("hello") is True
    assert calls[0]["json"]["chat_id"] == "-1003840996802"


def test_send_alert_does_not_fallback_to_owner_chat(monkeypatch):
    calls = []
    monkeypatch.setenv("TELEGRAM_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42582324")
    monkeypatch.delenv("ALERT_CHANNEL_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_ALERT_CHAT_ID", raising=False)
    monkeypatch.setattr(alerts.httpx, "post", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert alerts.send_alert("hello") is False
    assert calls == []


def test_startup_handler_calls_send_alert(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "utils.alert_handlers.send_alert",
        lambda message, level="INFO", **kwargs: calls.append((message, level, kwargs)) or True,
    )

    assert alert_startup("v1", "abc123") is True
    assert calls
    assert calls[0][1] == "DEPLOY"
    assert "Deployment" in calls[0][0]


def test_cinema_handler_calls_send_alert(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "utils.alert_handlers.send_alert",
        lambda message, level="INFO", **kwargs: calls.append((message, level, kwargs)) or True,
    )

    assert alert_cinema_sync_complete(2, 1, 3.4, None) is True
    assert calls
    assert calls[0][1] == "INFO"
    assert "Cinema Sync Completed" in calls[0][0]
    assert "2 new favourites" in calls[0][0]
