# Python 3.13 P0 execution checklist

This document records the P0 work for Python 3.13 migration. Runtime is pinned in both `.python-version` (`3.13.9`, for local/dev tooling) and `runtime.txt` (`python-3.13.9`, for deployment platforms like Heroku).


1. **Dependency baseline refresh**
   - Upgraded pinned runtime dependencies to current maintained releases with Python 3.13 support targets.
2. **Next validation steps (required in CI/deploy pipeline)**
   - Create a Python 3.13 job and run `pytest -q`.
   - Run a cold-start import smoke test: `python -c "import second_brain.main"`.
   - Run bot startup smoke test in staging.

## Updated dependency pins

- python-telegram-bot==21.11.1
- anthropic==0.54.0
- notion-client==2.3.0
- apscheduler==3.11.0
- python-dotenv==1.1.0
- pytz==2025.2
- aiohttp==3.11.18
- httpx==0.28.1
