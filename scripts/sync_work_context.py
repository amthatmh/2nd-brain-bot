#!/usr/bin/env python3
"""Sync Notion Work Sync library to Markdown files.

Run manually:
    python scripts/sync_work_context.py
    python scripts/sync_work_context.py --out /tmp/work-sync --dry-run
    python scripts/sync_work_context.py --db <notion_db_id>

The bot scheduler imports run_sync from second_brain.work_sync.sync directly.
This script is the CLI entry point.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from second_brain.work_sync.sync import run_sync  # noqa: E402 — path setup above


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Notion Work Sync DB to Markdown files")
    parser.add_argument("--out", default=os.environ.get("WORK_SYNC_OUT", "./work-sync"), help="Output directory")
    parser.add_argument("--db", default=None, help="Override Notion Work Sync DB ID")
    parser.add_argument("--dry-run", action="store_true", help="Print output without writing files")
    args = parser.parse_args()

    summary = run_sync(out=args.out, db_id=args.db, dry_run=args.dry_run)
    print(f"written: {summary['written']}")
    if summary["skipped"]:
        print(f"skipped: {summary['skipped']}")
    if summary["errors"]:
        print(f"errors:  {summary['errors']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
