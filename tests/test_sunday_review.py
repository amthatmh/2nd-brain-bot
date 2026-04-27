import importlib
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


REQUIRED_ENV = {
    "TELEGRAM_TOKEN": "x",
    "TELEGRAM_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "x",
    "NOTION_TOKEN": "x",
    "NOTION_DB_ID": "x",
    "NOTION_HABIT_DB": "x",
    "NOTION_LOG_DB": "x",
    "NOTION_STREAK_DB": "x",
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
}


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), \
        patch("notion_client.Client", return_value=MagicMock()), \
        patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


class TestSundayReviewFloodProtection(unittest.IsolatedAsyncioTestCase):
    async def test_sunday_review_limits_card_messages(self):
        main = load_main_module()
        bot = AsyncMock()
        tasks = [
            {"name": f"Task {i}", "context": "💼 Work", "auto_horizon": "🟠 This Week", "page_id": f"page-{i}"}
            for i in range(1, 10)
        ]

        with patch.object(main, "is_muted", return_value=False), \
            patch.object(main, "send_daily_digest", AsyncMock()), \
            patch.object(main, "query_tasks_by_auto_horizon", side_effect=[tasks, []]), \
            patch.object(main, "SUNDAY_REVIEW_CARD_LIMIT", 3):
            await main.send_sunday_review(bot)

        # 1 header + 3 task cards + 1 overflow notice
        self.assertEqual(bot.send_message.await_count, 5)
        overflow_call = bot.send_message.await_args_list[-1]
        self.assertIn("avoid flooding chat", overflow_call.kwargs["text"])


if __name__ == "__main__":
    unittest.main()
