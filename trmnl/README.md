# TRMNL private-plugin markup (GitHub sync)

Source of truth for the e-ink card markup. Edit the `.liquid` files here; on
merge to `main`, [`.github/workflows/trmnl-sync.yml`](../.github/workflows/trmnl-sync.yml)
pushes each plugin to TRMNL with `trmnlp push`. No more copy/paste.

```
trmnl/
  health/   → Health Dashboard plugin   (GET /trmnl/health)
  habits/   → Habits Kit plugin         (GET /trmnl/habits)
    .trmnlp.yml        local preview config (not uploaded)
    src/full.liquid    the card markup (edit this)
    src/settings.yml   plugin id + config (hydrated from the live plugin)
```

Only markup lives here — the JSON data still comes from the Railway endpoints.

## One-time setup

You do this once (needs your TRMNL account, so it can't be done in CI or by an
agent):

1. **Install the CLI** (Ruby gem):
   ```bash
   gem install trmnl_preview      # provides `trmnlp`
   trmnlp login                   # paste your TRMNL API key
   ```
2. **Hydrate each plugin's settings** so `push` keeps the polling config instead
   of clobbering it. Get each Plugin UUID from the TRMNL UI (right panel of the
   plugin), put it in `src/settings.yml` under `id:`, then pull:
   ```bash
   cd trmnl/health && trmnlp pull      # fills strategy / polling_url / etc.
   cd ../habits    && trmnlp pull
   ```
   (Or `trmnlp clone health <id>` into a scratch dir and copy its
   `settings.yml` over — either way `settings.yml` ends up with the live config.)
   ⚠️ **Keep the `full.liquid` in this repo** (our version). If `pull`/`clone`
   overwrites it with the old markup, restore it from git.
3. **Add the CI secret:** repo → Settings → Secrets → Actions →
   `TRMNL_API_KEY` = your TRMNL API key.

Until `id` is set in a plugin's `settings.yml`, the workflow **skips** that
plugin's push (so it can never create a duplicate plugin).

## Everyday use

- Edit `trmnl/<plugin>/src/full.liquid`, open a PR → CI lints it.
- Merge to `main` → CI runs `trmnlp push` → the live plugin updates.
- Preview locally: `cd trmnl/<plugin> && trmnlp serve`.

## Note

`second_brain/healthtrack/trmnl_card.liquid` and
`second_brain/habitkit/trmnl_card.liquid` are the previous hand-paste copies.
Once sync is verified, delete them so this stays the single source of truth.
