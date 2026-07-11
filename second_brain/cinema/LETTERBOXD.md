# Letterboxd ↔ Cinema Log Reconciliation

Status: **design approved 2026-07-04, not yet implemented.**

## Goal

Make Letterboxd the complete film diary (home + cinema) while keeping the Notion
**Cinema Log** (`NOTION_CINEMA_LOG_DB`) as the single source of truth. Today the
Cinema Log only holds in-person theatre viewings logged via Telegram.

## Model

One store: the Notion Cinema Log. Letterboxd and Telegram are **input feeds**, not
a second database to maintain.

- Letterboxd captures the *watch event* (what / when / rating).
- Location detail (venue, seat, auditorium) stays **private in Notion only** — never
  written to Letterboxd, because Letterboxd reviews and profiles are public.

## Schema changes

- `Source` select: add option `🎬 Letterboxd`.
- New `Cinema` **checkbox**: `true` = in-person theatre, `false` = home/streaming.
  `Venue` / `Seat` / `Auditorium` are populated only when `Cinema = true`.

## Steady state (ongoing sync)

1. Poll Letterboxd RSS `letterboxd.com/<user>/rss/` hourly. The feed carries
   `tmdb:movieId`, `letterboxd:watchedDate`, and `letterboxd:memberRating`.
2. New film → create Notion row (`Source = 🎬 Letterboxd`, `Cinema = false`).
3. Telegram inline keyboard prompt: **`🎬 Cinema`** / **`🏠 Home`**.
   - `🏠 Home` → `Cinema = false`, omit venue fields, done.
   - `🎬 Cinema` → `Cinema = true`, then the existing venue → seat → auditorium
     capture flow.
4. Silent when nothing new (no Telegram message). Default if untapped = Home.

Reuses `pending_entertainment_*` stash + a `rate:`-style callback
(`watchloc:<pid>:cinema|home`); seat/auditorium parsed via existing
`_extract_cinema_visit_details()`.

### Dedup / dup-loop guard

After the one-time push of Notion history up to Letterboxd, those entries appear in
the RSS feed. The poller must dedup on `(tmdbID, watchedDate)` against existing Notion
rows → **silent skip, no prompt**. Also seed a watermark at poller startup. Only
genuinely new films trigger a prompt.

## One-time backfill (run once, bidirectional)

- **Notion → Letterboxd:** generate a Letterboxd import CSV (`tmdbID`, `Title`,
  `Year`, `WatchedDate`; `Review` blank for privacy) and upload via Letterboxd
  Settings → Import & Export. The importer matches exactly on `tmdbID`, and
  re-importing the same film + `WatchedDate` **updates** the existing entry, so the
  import is idempotent.
- **Letterboxd → Notion:** export Letterboxd `diary.csv` and create any Notion rows
  missing (old home viewings from the dormant account).

## Rating conversion

Notion rating = integer **−3..+3** (7 points, `0` neutral, `−3` hated, `+3` loved).
Letterboxd = 0.5–5.0★ (10 points) plus an *unrated* state.

**Backfill (Notion → Letterboxd):**

| Notion | Letterboxd | `Rating10` |
|---|---|---|
| −3 | ★ 0.5 | 1 |
| −2 | ★ 1.0 | 2 |
| −1 | ★★ 2.0 | 4 |
| 0 | ★★½ 2.5 | 5 |
| +1 | ★★★½ 3.5 | 7 |
| +2 | ★★★★ 4.0 | 8 |
| +3 | ★★★★★ 5.0 | 10 |

**Ingest (Letterboxd → Notion, lossy bucket):**

| Letterboxd | Notion |
|---|---|
| ★ 0.5 | −3 |
| ★ 1.0 | −2 |
| ★½ 1.5 | −2 |
| ★★ 2.0 | −1 |
| ★★½ 2.5 | 0 |
| ★★★ 3.0 | 0 |
| ★★★½ 3.5 | +1 |
| ★★★★ 4.0 | +2 |
| ★★★★½ 4.5 | +2 |
| ★★★★★ 5.0 | +3 |
| unrated | *(leave empty, ≠ 0)* |

On ingest, always prompt to confirm the converted rating via the −3..+3 keyboard; the
default auto-fills and the user taps only to override.

## Relevant code

- `second_brain/entertainment/log.py`
- `second_brain/cinema/sync.py`
- `second_brain/routers.py`
