import unittest
import os
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


class TestCallbackDispatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, REQUIRED_ENV, clear=False)
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    def _update(self, data: str):
        q = MagicMock()
        q.data = data
        q.answer = AsyncMock()
        q.edit_message_reply_markup = AsyncMock()
        update = MagicMock()
        update.callback_query = q
        return update, q

    async def _dispatch_with_patched_main(self, data: str):
        from second_brain.routers import handle_callback

        update, q = self._update(data)
        context = MagicMock()
        fake_main = MagicMock()
        fake_main.handle_v10_callback = AsyncMock(return_value=False)
        return update, q, context, fake_main, handle_callback

    async def test_dispatch_h_log(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("h:log:abc")
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch("second_brain.routers._cb_h_log", new_callable=AsyncMock) as mock_handler:
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["h", "log", "abc"], context)

    async def test_dispatch_h_toggle(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("h:toggle:abc")
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch("second_brain.routers._cb_h_toggle", new_callable=AsyncMock) as mock_handler:
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["h", "toggle", "abc"], context)

    async def test_dispatch_h_done(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("h:done")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_EXACT", {"h:done": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["h", "done"], context)

    async def test_dispatch_h_check_cancel(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("h:check:cancel")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_EXACT", {"h:check:cancel": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["h", "check", "cancel"], context)

    async def test_dispatch_d(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("d:page-id")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_PREFIX", {"d": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["d", "page-id"], context)

    async def test_dispatch_mq_unmute(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("mq:unmute")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_PREFIX", {"mq": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["mq", "unmute"], context)

    async def test_dispatch_qp_digest(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("qp:digest")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_PREFIX", {"qp": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["qp", "digest"], context)

    async def test_dispatch_noop(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("noop")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_PREFIX", {"noop": mock_handler}):
            await handle_callback(update, context)
        q.answer.assert_awaited_once()
        mock_handler.assert_awaited_once_with(q, ["noop"], context)

    async def test_dispatch_digest_today(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("digest:today")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_EXACT", {"digest:today": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["digest", "today"], context)

    async def test_unknown_prefix_logs_warning_without_raising(self):
        update, _q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("zzz:foo")
        with patch("second_brain.routers._main", return_value=fake_main), \
            self.assertLogs("second_brain.routers", level="WARNING") as logs:
            await handle_callback(update, context)
        self.assertTrue(any("Unhandled callback: zzz:foo" in msg for msg in logs.output))

    async def test_hl_normalizes_to_hc_before_v10_dispatch(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("hl:abc")
        with patch("second_brain.routers._main", return_value=fake_main):
            await handle_callback(update, context)
        fake_main.handle_v10_callback.assert_awaited_once_with(q, ["hc", "abc"])

    async def test_cf_A_normalizes_to_log_readiness_before_dispatch(self):
        update, q, context, fake_main, handle_callback = await self._dispatch_with_patched_main("cf_A")
        mock_handler = AsyncMock()
        with patch("second_brain.routers._main", return_value=fake_main), \
            patch.dict("second_brain.routers._CB_PREFIX", {"cf": mock_handler}):
            await handle_callback(update, context)
        mock_handler.assert_awaited_once_with(q, ["cf", "log_readiness"], context)
