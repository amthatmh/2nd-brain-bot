# v9 Asana Sync Audit (main.py)

Date: 2026-04-22

## Scope
- Verified that `main.py` v9 keeps v8 behavior and only layers Asana sync orchestration.
- Verified function parity between v8 baseline commit `5f1955e` and current v9 commit.

## Findings

### 1) v8 behavior remains intact
- Existing v8 scheduler setup and startup flow are still present (`post_init`, `main`) with an additive Asana block.
- Habit/task logic and Telegram handlers were not removed in v9.

### 2) Asana sync is integrated additively
- New import from `asana_sync.py`: `reconcile`, `AsanaSyncError`.
- New Asana environment/config variables:
  - `ASANA_PAT`
  - `ASANA_PROJECT_GID`
  - `ASANA_SYNC_SOURCE`
  - `ASANA_SYNC_INTERVAL`
- New async function `run_asana_sync(bot)`:
  - returns immediately if `ASANA_PAT` is unset
  - runs `reconcile(...)` in a thread executor
  - logs only meaningful sync stats
  - handles `AsanaSyncError` and generic exceptions
- `post_init` now conditionally registers `run_asana_sync` on APScheduler interval with `max_instances=1`, `coalesce=True`, and immediate first run.
- Startup log now includes `asana_sync=ON/OFF` and interval.

### 3) Function parity vs v8
Command used:

```bash
python - <<'PY'
import re,subprocess
old=subprocess.check_output(['git','show','5f1955e:main.py'],text=True)
new=open('main.py').read()
pat=re.compile(r'^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(',re.M)
oldset=set(pat.findall(old)); newset=set(pat.findall(new))
print('missing sync defs:',sorted(oldset-newset))
apat=re.compile(r'^async def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(',re.M)
oldas=set(apat.findall(old)); newas=set(apat.findall(new))
print('missing async defs:',sorted(oldas-newas))
print('added async defs:',sorted(newas-oldas))
PY
```

Observed result:
- `missing sync defs: []`
- `missing async defs: []`
- `added async defs: ['run_asana_sync']`

## Conclusion
✅ All v8 functions in `main.py` are intact.

✅ v9 adds Asana sync in an additive way without removing v8 function definitions.

✅ The only new async function in `main.py` is `run_asana_sync`.
