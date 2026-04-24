import unittest

from asana_startup import resolve_asana_startup_status
from asana_sync import AsanaSyncError


class TestAsanaStartup(unittest.IsolatedAsyncioTestCase):
    async def test_off_when_pat_missing(self):
        async def send_alert(_text: str):
            raise AssertionError("send_alert should not be called")

        asana_status, smoke_status = await resolve_asana_startup_status(
            asana_pat="",
            source_mode="project",
            asana_workspace_gid="",
            asana_project_gid="123",
            notion=object(),
            notion_db_id="db",
            validate_notion_schema_fn=lambda *_: [],
            startup_smoke_enabled=True,
            startup_smoke_fn=lambda **_: {"sample_task_gid": "x"},
            asana_sync_error_cls=AsanaSyncError,
            send_alert=send_alert,
        )
        self.assertEqual(asana_status, "OFF")
        self.assertEqual(smoke_status, "SKIPPED")

    async def test_disabled_schema_sends_alert(self):
        alerts = []

        async def send_alert(text: str):
            alerts.append(text)

        asana_status, smoke_status = await resolve_asana_startup_status(
            asana_pat="token",
            source_mode="project",
            asana_workspace_gid="",
            asana_project_gid="123",
            notion=object(),
            notion_db_id="db",
            validate_notion_schema_fn=lambda *_: ["Missing property: Name"],
            startup_smoke_enabled=True,
            startup_smoke_fn=lambda **_: {"sample_task_gid": "x"},
            asana_sync_error_cls=AsanaSyncError,
            send_alert=send_alert,
        )
        self.assertEqual(asana_status, "DISABLED (schema)")
        self.assertEqual(smoke_status, "SKIPPED")
        self.assertEqual(len(alerts), 1)

    async def test_ready_when_smoke_disabled(self):
        async def send_alert(_text: str):
            return None

        asana_status, smoke_status = await resolve_asana_startup_status(
            asana_pat="token",
            source_mode="project",
            asana_workspace_gid="",
            asana_project_gid="123",
            notion=object(),
            notion_db_id="db",
            validate_notion_schema_fn=lambda *_: [],
            startup_smoke_enabled=False,
            startup_smoke_fn=lambda **_: {"sample_task_gid": "x"},
            asana_sync_error_cls=AsanaSyncError,
            send_alert=send_alert,
        )
        self.assertEqual(asana_status, "READY")
        self.assertTrue(smoke_status.startswith("SKIPPED"))

    async def test_ready_when_smoke_passes(self):
        async def send_alert(_text: str):
            raise AssertionError("send_alert should not be called")

        asana_status, smoke_status = await resolve_asana_startup_status(
            asana_pat="token",
            source_mode="project",
            asana_workspace_gid="",
            asana_project_gid="123",
            notion=object(),
            notion_db_id="db",
            validate_notion_schema_fn=lambda *_: [],
            startup_smoke_enabled=True,
            startup_smoke_fn=lambda **_: {"sample_task_gid": "gid-1"},
            asana_sync_error_cls=AsanaSyncError,
            send_alert=send_alert,
        )
        self.assertEqual(asana_status, "READY")
        self.assertIn("PASS (sample=gid-1)", smoke_status)


if __name__ == "__main__":
    unittest.main()
