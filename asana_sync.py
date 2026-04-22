"""
Asana <-> Notion sync worker.

Runs as a separate Railway service from the Telegram bot.
Polls every ASANA_SYNC_EVERY_SECONDS (default 15s for near-real-time feel).

Key design choices:
- Sequential cycles (never overlapping). If a sync takes longer than the
  interval, the next one waits. Prevents doubled writes and rate-limit
  thrashing under load.
- Graceful shutdown via SIGTERM so Railway deploys don't interrupt
  a mid-flight sync.
- Exponential backoff on repeated failures so a broken Notion DB doesn't
  hammer the API 5760 times a day.
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from notion_client import Client

from asana_sync import AsanaSyncError, reconcile

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
log = logging.getLogger("sync_worker")

# ── Config ────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]

ASANA_PAT = os.environ["ASANA_PAT"]
ASANA_WORKSPACE_GID = os.environ.get("ASANA_WORKSPACE_GID", "")
ASANA_PROJECT_GID = os.environ.get("ASANA_PROJECT_GID", "")
ASANA_SYNC_SOURCE = os.environ.get("ASANA_SYNC_SOURCE", "project")
INTERVAL = int(os.environ.get("ASANA_SYNC_EVERY_SECONDS", "15"))

# Backoff ceiling: if we keep failing, wait up to this long between tries.
MAX_BACKOFF = 300  # 5 minutes

# ── Shutdown handling ─────────────────────────────────────────────────────
_shutdown_requested = False


def _handle_shutdown(signum, _frame):
    """Railway sends SIGTERM on deploy/restart. Finish current cycle cleanly."""
    global _shutdown_requested
    log.info("Received signal %s, will exit after current cycle", signum)
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ── Sync loop ─────────────────────────────────────────────────────────────
def main() -> None:
    notion = Client(auth=NOTION_TOKEN)

    log.info(
        "Sync worker started | interval=%ss source=%s project=%s",
        INTERVAL,
        ASANA_SYNC_SOURCE,
        ASANA_PROJECT_GID or "(my_tasks)",
    )

    consecutive_failures = 0

    while not _shutdown_requested:
        cycle_start = time.monotonic()
        try:
            stats = reconcile(
                notion=notion,
                notion_db_id=NOTION_DB_ID,
                asana_token=ASANA_PAT,
                asana_workspace_gid=ASANA_WORKSPACE_GID,
                asana_project_gid=ASANA_PROJECT_GID,
                source_mode=ASANA_SYNC_SOURCE,
            )
            elapsed = time.monotonic() - cycle_start

            # Only log when something happened — keeps logs readable at 15s polling.
            if any(v for k, v in stats.items() if k != "skipped"):
                log.info("Cycle OK in %.2fs: %s", elapsed, stats)
            else:
                log.debug("Cycle OK in %.2fs (no changes)", elapsed)

            consecutive_failures = 0

            # Warn if a cycle is approaching the interval — you'd want to know.
            if elapsed > INTERVAL * 0.8:
                log.warning(
                    "Cycle took %.2fs (interval=%ss). Consider raising interval "
                    "or reducing scope.",
                    elapsed,
                    INTERVAL,
                )

        except AsanaSyncError as e:
            # Config error — not transient. Log loudly but keep retrying in case
            # the user fixes env vars without redeploying.
            consecutive_failures += 1
            log.error("Config error (cycle %d): %s", consecutive_failures, e)

        except Exception:
            consecutive_failures += 1
            log.exception("Sync cycle failed (cycle %d)", consecutive_failures)

        # ── Sleep until next cycle, with backoff on repeated failures ──
        if consecutive_failures > 0:
            # Exponential backoff: 15s, 30s, 60s, 120s, 300s (capped)
            wait = min(INTERVAL * (2 ** min(consecutive_failures - 1, 5)), MAX_BACKOFF)
            log.info("Backing off %ss after %d failures", wait, consecutive_failures)
        else:
            # Subtract time already spent so we hit roughly INTERVAL pacing.
            elapsed = time.monotonic() - cycle_start
            wait = max(0, INTERVAL - elapsed)

        # Sleep in short chunks so SIGTERM response stays snappy.
        slept = 0.0
        while slept < wait and not _shutdown_requested:
            chunk = min(1.0, wait - slept)
            time.sleep(chunk)
            slept += chunk

    log.info("Shutdown complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
