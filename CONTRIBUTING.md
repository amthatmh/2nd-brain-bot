# Contributing

## Adding a new Notion-backed feature

Use this protocol when introducing a new database integration:

1. Add the env var in `second_brain/config.py`.
2. Add Notion helpers in `second_brain/notion/<feature>.py` using dependency
   injection — pass `notion` and `notion_db_id` as parameters, never import
   module-level globals. See `second_brain/notion/tasks.py` as the canonical
   reference.
3. Add keyboard builders to `second_brain/keyboards.py` and formatters to
   `second_brain/formatters.py` if the feature needs UI components.
4. Add classifier support in `second_brain/ai/classify.py` if intent
   detection is required.
5. Wire the handler and scheduler jobs in `second_brain/main.py` — this file
   should contain only handler registration and startup logic, no business
   logic.

The canonical end-to-end reference flow is the Notes integration:
`second_brain/notion/notes.py` → `second_brain/notes/flow.py` →
`second_brain/ai/classify.py` → `second_brain/main.py`.

## Adding a new health/webhook feature

Use this protocol when introducing or extending health/webhook flows:

1. Follow `second_brain/healthtrack/` as the canonical reference implementation for config, route parsing, and step-processing helpers.
2. Keep feature-specific helpers in dedicated module files under `second_brain/healthtrack/` (mirroring the Notion-backed feature structure).
3. Register new HTTP endpoints via `register_*_routes()` helper functions, then call those helpers from `start_http_server()` rather than adding inline route logic there.
4. Add/adjust focused tests in `tests/` for payload parsing and state transitions before wiring into bot startup scheduling.
