"""Pre-fetch the gcat parquet fixtures so the test run does no network I/O.

`tests/modeling/gcat/setup.sql` builds its tables from nine parquet files on
GCS. Left to the tests that is ~6.5MB pulled inside the pytest step, plus an
`INSTALL httpfs`, on every job of the matrix -- and it is latency, not
bandwidth, that hurts: a degraded runner once took gcat from 4.4s to 365s.
Fetched here it is a named step, cacheable, and the tests read local files.

Fetching is an optimization, never a gate. If a download fails the conftest
falls back to the original URLs, so a run without network access to GCS
behaves exactly as it did before.
"""

from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SETUP_SQL = ROOT / "tests" / "modeling" / "gcat" / "setup.sql"
DATA_DIR = ROOT / "tests" / "modeling" / "gcat" / "data"
URL_PATTERN = re.compile(r"https://storage\.googleapis\.com/[^']+\.parquet")


def urls_from(setup_sql: Path) -> list[str]:
    return sorted(set(URL_PATTERN.findall(setup_sql.read_text(encoding="utf-8"))))


def fetch(url: str, target: Path) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            payload = response.read()
    except Exception as e:
        print(f"WARNING: could not fetch {url}: {e}")
        return False
    target.write_bytes(payload)
    return True


def main() -> int:
    if not SETUP_SQL.exists():
        print(f"WARNING: {SETUP_SQL} missing; nothing to fetch")
        return 0
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for url in urls_from(SETUP_SQL):
        target = DATA_DIR / url.rsplit("/", 1)[-1]
        if target.exists() and target.stat().st_size:
            print(f"cached  {target.name}")
            continue
        print(f"{'fetched' if fetch(url, target) else 'FAILED '} {target.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
