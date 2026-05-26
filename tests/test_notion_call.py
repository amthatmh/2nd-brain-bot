from second_brain.notion import notion_call


def test_notion_call_retries_timeouts(monkeypatch):
    calls = {"count": 0}
    monkeypatch.setattr("second_brain.notion.time.sleep", lambda seconds: None)
    monkeypatch.setattr("second_brain.notion.random.uniform", lambda start, end: 0)

    def flaky():
        calls["count"] += 1
        if calls["count"] == 1:
            raise Exception("Request to Notion API has timed out")
        return {"ok": True}

    assert notion_call(flaky, retries=2) == {"ok": True}
    assert calls["count"] == 2
