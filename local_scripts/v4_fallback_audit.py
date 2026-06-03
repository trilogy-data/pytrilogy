"""Audit which v4 queries hit the v3 fallback, per derivation.

Monkeypatches dispatch._fallback_to_v3 to record `derivation` per query,
then plans every test_set.txt query through v4 (planning only, no DB)."""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from discovery_v4 import run_tpcds_query  # noqa: E402

import trilogy.core.processing.v4_node_generators.dispatch as dispatch  # noqa: E402

OUT_DIR = Path(__file__).parent / "v4_compare"
hits: Counter = Counter()
per_query: dict[str, Counter] = {}
_orig = dispatch._fallback_to_v3


def main() -> None:
    ids = []
    for line in (OUT_DIR / "test_set.txt").read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            ids.append(f"{int(line):02d}" if line.isdigit() else line)

    for qid in ids:
        local: Counter = Counter()

        def cb(*args, **kw):  # noqa: ANN001
            local[kw["derivation"]] += 1
            hits[kw["derivation"]] += 1
            return _orig(*args, **kw)

        dispatch._fallback_to_v3 = cb
        try:
            run_tpcds_query(qid)
        except Exception as exc:
            print(f"[{qid}] ERROR {type(exc).__name__}: {exc}")
        finally:
            dispatch._fallback_to_v3 = _orig
        if local:
            per_query[qid] = local
            print(f"[{qid}] {dict(local)}")

    print("\nTotals:", dict(hits))


if __name__ == "__main__":
    main()
