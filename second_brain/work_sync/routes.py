"""HTTP routes for serving work-sync Markdown files."""

from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web

from second_brain.http_utils import cors_headers

log = logging.getLogger(__name__)


def register_work_sync_routes(app: web.Application, out_dir: Path) -> None:
    """Serve files from out_dir at /work-sync/{path}."""

    async def _handler(request: web.Request) -> web.Response:
        rel = request.match_info.get("path", "")
        if not rel:
            rel = "README.md"

        # Reject path-traversal attempts
        try:
            target = (out_dir / rel).resolve()
            out_resolved = out_dir.resolve()
            target.relative_to(out_resolved)
        except (ValueError, Exception):
            return web.Response(status=400, text="Bad path", headers=cors_headers())

        if not target.exists():
            return web.Response(status=404, text="Not found", headers=cors_headers())

        content = target.read_text(encoding="utf-8")
        return web.Response(
            text=content,
            content_type="text/plain",
            headers={**cors_headers(), "Content-Type": "text/markdown; charset=utf-8"},
        )

    app.router.add_get("/work-sync", _handler)
    app.router.add_get("/work-sync/{path:.*}", _handler)
    log.info("work-sync routes registered: GET /work-sync/{path}")
