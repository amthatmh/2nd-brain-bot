# Contributing

## Adding a new Notion-backed feature

Use this protocol when introducing a new database integration:

1. Add the env var in `second_brain/config.py` (keep existing env names unchanged).
2. Add feature-specific Notion helpers in `second_brain/notion/<feature>.py`.
3. Add classifier support in `second_brain/ai/classify.py` when intent detection is required.
4. Register message/callback routes in `second_brain/handlers/messages.py` and/or `second_brain/handlers/callbacks.py`.
5. Add the database to startup health checks in `second_brain/main.py`.

Notes integration (`NOTION_NOTES_DB`) is the canonical reference flow.
