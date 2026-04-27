# Contributing

## Adding a new Notion-backed feature

Use this protocol when introducing a new database integration:

1. Add the env var in `second_brain/config.py` (keep existing env names unchanged).
2. Add feature-specific Notion helpers in `second_brain/notion/<feature>.py`.
3. Add classifier support in `second_brain/ai/classify.py` when intent detection is required.
4. Add the database and user-flow wiring in `second_brain/main.py`.

Notes integration (`NOTION_NOTES_DB`) is the canonical reference flow.
