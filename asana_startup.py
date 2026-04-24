"""Asana startup validation/smoke orchestration helpers."""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


async def resolve_asana_startup_status(
    *,
    asana_pat: str,
    source_mode: str,
    asana_workspace_gid: str,
    asana_project_gid: str,
    notion,
    notion_db_id: str,
    validate_notion_schema_fn,
    startup_smoke_enabled: bool,
    startup_smoke_fn,
    asana_sync_error_cls,
    send_alert,
) -> tuple[str, str]:
    """
    Return (asana_status, smoke_status) after startup checks.
    send_alert: async callable(text: str) used for operator notifications.
    """
    asana_status = "OFF"
    smoke_status = "SKIPPED"

    if not asana_pat:
        return asana_status, smoke_status

    problems = validate_notion_schema_fn(notion, notion_db_id)
    if source_mode == "my_tasks" and not asana_workspace_gid:
        problems.append("ASANA_WORKSPACE_GID env var is required when ASANA_SYNC_SOURCE=my_tasks")

    if problems:
        log.error("Asana sync DISABLED — startup checks failed:")
        for p in problems:
            log.error("  - %s", p)
        await send_alert(
            "🚨 *Asana sync DISABLED — Notion schema/startup checks failed*\n\n"
            + "\n".join(f"• {p}" for p in problems)
        )
        return "DISABLED (schema)", smoke_status

    if not startup_smoke_enabled:
        return "READY", "SKIPPED (disabled by ASANA_STARTUP_SMOKE)"

    try:
        loop = asyncio.get_event_loop()
        smoke = await loop.run_in_executor(
            None,
            lambda: startup_smoke_fn(
                notion=notion,
                notion_db_id=notion_db_id,
                asana_token=asana_pat,
                asana_project_gid=asana_project_gid,
                asana_workspace_gid=asana_workspace_gid,
                source_mode=source_mode,
            ),
        )
        smoke_status = f"PASS (sample={smoke.get('sample_task_gid')})"
        log.info("Asana startup smoke test passed ✓ %s", smoke)
        return "READY", smoke_status
    except asana_sync_error_cls as e:
        smoke_status = f"FAIL ({e})"
        await send_alert(
            "🚨 *Asana sync DISABLED — startup smoke test failed*\n\n"
            f"• {e}\n\n"
            "_Fix config/integration and redeploy. Scheduler was not started for Asana sync._"
        )
        return "DISABLED (smoke)", smoke_status
    except Exception as e:
        smoke_status = f"FAIL ({e})"
        log.exception("Asana sync DISABLED — unexpected smoke test error: %s", e)
        await send_alert(
            "🚨 *Asana sync DISABLED — startup smoke test crashed*\n\n"
            f"• {e}\n\n"
            "_Fix and redeploy._"
        )
        return "DISABLED (smoke)", smoke_status
