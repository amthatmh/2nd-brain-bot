"""Integration tests for the steps sync HTTP webhook."""

from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

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

from second_brain.healthtrack.routes import register_health_routes


class TestStepsWebhook(unittest.IsolatedAsyncioTestCase):
    async def test_steps_sync_posts_payload_to_notion_upsert(self):
        app = web.Application()
        notion = MagicMock()
        bot = MagicMock()
        register_health_routes(
            app,
            notion=notion,
            habit_db_id="habit-db",
            log_db_id="log-db",
            env_db_id="env-db",
            tz=None,
            bot_getter=lambda: bot,
            chat_id=123,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            with patch("second_brain.healthtrack.routes.WEBHOOK_SECRET", "secret"), \
                 patch("second_brain.healthtrack.routes.handle_steps_sync", new_callable=AsyncMock) as sync_mock:
                sync_mock.return_value = {
                    "action": "created",
                    "steps": 10500,
                    "date": "2026-05-06",
                    "completed": True,
                    "page_id": "page-1",
                    "timestamp": "2026-05-06T00:00:00+00:00",
                }
                response = await client.post(
                    "/api/v1/steps-sync",
                    headers={"X-Health-Secret": "secret"},
                    json={"steps": 10500, "date": "2026-05-06"},
                )
                body = await response.text()
        finally:
            await client.close()

        self.assertEqual(response.status, 200, body)
        self.assertTrue(json.loads(body)["ok"])
        sync_mock.assert_awaited_once()
        kwargs = sync_mock.await_args.kwargs
        self.assertEqual(kwargs["steps"], 10500)
        self.assertEqual(kwargs["date_str"], "2026-05-06")
        self.assertIs(kwargs["notion"], notion)
        self.assertEqual(kwargs["habit_db_id"], "habit-db")
        self.assertEqual(kwargs["log_db_id"], "log-db")
        self.assertEqual(kwargs["env_db_id"], "env-db")
        self.assertIs(kwargs["bot"], bot)
        self.assertEqual(kwargs["chat_id"], 123)

    async def test_steps_sync_reports_notion_upsert_failure_to_system_log(self):
        app = web.Application()
        notion = MagicMock()
        bot = MagicMock()
        bot.send_message = AsyncMock()
        register_health_routes(
            app,
            notion=notion,
            habit_db_id="habit-db",
            log_db_id="log-db",
            env_db_id="env-db",
            tz=None,
            bot_getter=lambda: bot,
            chat_id=123,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            fake_config = SimpleNamespace(MY_CHAT_ID=123, ERROR_CHANNEL_ID=456)
            with patch("second_brain.healthtrack.routes.WEBHOOK_SECRET", "secret"), \
                 patch("second_brain.healthtrack.routes.handle_steps_sync", new_callable=AsyncMock) as sync_mock, \
                 patch.dict("sys.modules", {"second_brain.config": fake_config}):
                sync_mock.return_value = {
                    "action": "error",
                    "reason": "notion_create_failed",
                    "steps": 10500,
                    "date": "2026-05-06",
                    "completed": True,
                    "page_id": None,
                    "timestamp": "2026-05-06T00:00:00+00:00",
                }
                response = await client.post(
                    "/api/v1/steps-sync",
                    headers={"X-Health-Secret": "secret"},
                    json={"steps": 10500, "date": "2026-05-06"},
                )
                body = await response.text()
        finally:
            await client.close()

        self.assertEqual(response.status, 200, body)
        bot.send_message.assert_awaited_once()
        kwargs = bot.send_message.await_args.kwargs
        self.assertEqual(kwargs["chat_id"], 456)
        self.assertIn("Steps webhook Notion upsert failed", kwargs["text"])
        self.assertIn("notion_create_failed", kwargs["text"])


if __name__ == "__main__":
    unittest.main()
