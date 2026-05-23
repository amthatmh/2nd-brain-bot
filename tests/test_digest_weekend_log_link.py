import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import os


os.environ.setdefault("TELEGRAM_TOKEN", "x")
os.environ.setdefault("TELEGRAM_CHAT_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "x")
os.environ.setdefault("NOTION_TOKEN", "x")
os.environ.setdefault("NOTION_DB_ID", "x")
os.environ.setdefault("NOTION_HABIT_DB", "x")
os.environ.setdefault("NOTION_LOG_DB", "x")
os.environ.setdefault("NOTION_NOTES_DB", "x")
os.environ.setdefault("NOTION_DIGEST_SELECTOR_DB", "x")
os.environ.setdefault("NOTION_STREAK_DB", "x")


class TestDigestWeekendLogLink(unittest.IsolatedAsyncioTestCase):
    async def test_log_only_slot_generates_log_before_send(self):
        import second_brain.digest as digest

        now = digest.datetime.now(digest.TZ)
        weekday = now.weekday() < 5
        slot = {"time": "11:45", "is_weekday": weekday, "include_habits": False}

        digest._digest_slot_sent_today.clear()
        digest._last_daily_log_url = ""

        cfg = {
            "contexts": None,
            "include_habits": False,
            "include_weather": False,
            "include_feel": False,
            "include_log": True,
            "max_items": None,
        }
        events = []

        async def fake_generate(bot):
            del bot
            events.append("generate")
            digest._last_daily_log_url = "https://notion.so/log-page"
            return {"action": "generated", "has_url": True}

        async def fake_send(*args, **kwargs):
            del args, kwargs
            events.append("send")

        with patch.object(digest, "get_digest_config", AsyncMock(return_value=cfg)), \
            patch.object(digest, "generate_daily_log", AsyncMock(side_effect=fake_generate)) as mock_generate, \
            patch.object(digest, "send_daily_digest", AsyncMock(side_effect=fake_send)) as mock_send, \
            patch.object(digest, "alert_digest_sent") as mock_alert:
            await digest.send_digest_for_slot(MagicMock(), slot)

        self.assertEqual(events, ["generate", "send"])
        mock_generate.assert_called_once()
        mock_send.assert_called_once()
        mock_alert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
