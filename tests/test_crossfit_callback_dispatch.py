import os
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
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
    "NOTION_STREAK_DB": "x",
}


class TestCfCallbackDispatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, REQUIRED_ENV, clear=False)
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    def _query(self, data: str = "cf:cancel", chat_id: int = 123):
        q = MagicMock()
        q.data = data
        q.answer = AsyncMock()
        q.edit_message_reply_markup = AsyncMock()
        q.edit_message_text = AsyncMock()
        q.message = MagicMock()
        q.message.chat_id = chat_id
        q.message.reply_text = AsyncMock()
        return q

    async def test_cancel_clears_pending_and_replies(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:cancel")
        cf_pending = {"123": {"mode": "strength"}}
        await handle_cf_callback(q, ["cf", "cancel"], MagicMock(), MagicMock(), {}, cf_pending)
        self.assertNotIn("123", cf_pending)
        q.message.reply_text.assert_awaited_once_with("❌ Session cancelled.")

    async def test_chain_no_clears_pending_and_replies(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:chain_no")
        cf_pending = {"123": {"session_chain": ["b"]}}
        await handle_cf_callback(q, ["cf", "chain_no"], MagicMock(), MagicMock(), {}, cf_pending)
        self.assertNotIn("123", cf_pending)
        q.message.reply_text.assert_awaited_once_with("✅ Session complete.")

    async def test_log_strength_routes_to_strength_flow(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:log_strength")
        cf_pending = {}
        claude = MagicMock()
        notion = MagicMock()
        config = {}
        with patch("second_brain.crossfit.handlers.handle_cf_strength_flow", new_callable=AsyncMock) as mock_flow:
            await handle_cf_callback(q, ["cf", "log_strength"], claude, notion, config, cf_pending)
        mock_flow.assert_awaited_once_with(q.message, {}, claude, notion, config, cf_pending)

    async def test_log_wod_routes_to_wod_flow(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:log_wod")
        cf_pending = {}
        notion = MagicMock()
        config = {}
        with patch("second_brain.crossfit.handlers.handle_cf_wod_flow", new_callable=AsyncMock) as mock_flow:
            await handle_cf_callback(q, ["cf", "log_wod"], MagicMock(), notion, config, cf_pending)
        mock_flow.assert_awaited_once_with(q.message, {}, notion, config, cf_pending)

    async def test_log_readiness_already_logged_edits_message(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:log_readiness")
        with patch("second_brain.crossfit.handlers.check_readiness_logged_today", new_callable=AsyncMock, return_value=True):
            await handle_cf_callback(q, ["cf", "log_readiness"], MagicMock(), MagicMock(), {}, {})
        q.edit_message_text.assert_awaited_once_with("✅ Readiness is already logged for today.", reply_markup=None)

    async def test_log_readiness_not_logged_sets_pending_mode(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:log_readiness")
        cf_pending = {}
        with patch("second_brain.crossfit.handlers.check_readiness_logged_today", new_callable=AsyncMock, return_value=False), \
            patch("second_brain.crossfit.handlers._prompt_readiness_field", new_callable=AsyncMock) as mock_prompt:
            await handle_cf_callback(q, ["cf", "log_readiness"], MagicMock(), MagicMock(), {}, cf_pending)
        self.assertEqual(cf_pending["123"]["mode"], "readiness")
        self.assertEqual(cf_pending["123"]["stage"], "sleep_quality")
        mock_prompt.assert_awaited_once_with(q.message, "123", "sleep_quality")

    async def test_unknown_action_answers_without_exception(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf:zzz")
        await handle_cf_callback(q, ["cf", "zzz"], MagicMock(), MagicMock(), {}, {})
        q.answer.assert_awaited_once_with("Action unavailable.", show_alert=False)

    async def test_missing_action_answers_unavailable(self):
        from second_brain.crossfit.handlers import handle_cf_callback

        q = self._query("cf")
        await handle_cf_callback(q, ["cf"], MagicMock(), MagicMock(), {}, {})
        q.answer.assert_awaited_once_with("Action unavailable.", show_alert=False)
