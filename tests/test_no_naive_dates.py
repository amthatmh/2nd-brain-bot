"""Guard against timezone-naive 'today'/'now' in product code.

The app has a single source of truth for the current local date/time —
``second_brain.utils.local_today`` (and ``datetime.now(TZ)``) — because the
process runs in UTC on the host while the user lives in another timezone.
Using ``date.today()`` or a bare ``datetime.now()`` reintroduces the off-by-one
drift that mislabeled tasks (e.g. a "tomorrow" deadline showing as "today").

This test fails if any of those naive patterns reappear in second_brain/.
"""

import re
from pathlib import Path

SECOND_BRAIN = Path(__file__).resolve().parent.parent / "second_brain"

# date.today(...), datetime.utcnow(...), and datetime.now() with no tz argument.
FORBIDDEN = [
    (re.compile(r"\bdate\.today\s*\("), "date.today() — use second_brain.utils.local_today()"),
    (re.compile(r"\bdatetime\.utcnow\s*\("), "datetime.utcnow() — use datetime.now(timezone.utc) or local time"),
    (re.compile(r"\bdatetime\.now\s*\(\s*\)"), "datetime.now() with no tz — pass TZ, e.g. datetime.now(TZ)"),
]


def test_no_naive_today_or_now_in_product_code():
    violations: list[str] = []
    for path in SECOND_BRAIN.rglob("*.py"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if "# noqa: naive-date" in line:
                continue
            for pattern, message in FORBIDDEN:
                if pattern.search(line):
                    rel = path.relative_to(SECOND_BRAIN.parent)
                    violations.append(f"{rel}:{lineno}: {message}\n    {line.strip()}")

    assert not violations, (
        "Timezone-naive date/time usage found (use local_today() / datetime.now(TZ)):\n"
        + "\n".join(violations)
    )
