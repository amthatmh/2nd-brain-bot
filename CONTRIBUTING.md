# Contributing

## Adding a new Notion-backed feature

Use this protocol when introducing a new database integration:

1. Add the env var in `second_brain/config.py` (keep existing env names unchanged).
2. Add feature-specific Notion helpers in `second_brain/notion/<feature>.py`.
3. Add classifier support in `second_brain/ai/classify.py` when intent detection is required.
4. Add the database and user-flow wiring in `second_brain/main.py`.

Notes integration (`NOTION_NOTES_DB`) is the canonical reference flow.

## Adding a new health/webhook feature

Use this protocol when introducing or extending health/webhook flows:

1. Follow `second_brain/healthtrack/` as the canonical reference implementation for config, route parsing, and step-processing helpers.
2. Keep feature-specific helpers in dedicated module files under `second_brain/healthtrack/` (mirroring the Notion-backed feature structure).
3. Register new HTTP endpoints via `register_*_routes()` helper functions, then call those helpers from `start_http_server()` rather than adding inline route logic there.
4. Add/adjust focused tests in `tests/` for payload parsing and state transitions before wiring into bot startup scheduling.
