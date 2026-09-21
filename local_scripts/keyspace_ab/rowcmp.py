"""Diff two ks_rowdump files: the tests whose DISTINCT result sets differ, and
the rows each side has that the other lacks."""

import json
import sys
from collections import defaultdict


def load(path: str) -> dict[str, set[tuple[str, ...]]]:
    out: dict[str, set[tuple[str, ...]]] = defaultdict(set)
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            out[row["test"]].add(tuple(row["rows"]))
    return out


def main(before_path: str, after_path: str, limit: int = 6) -> None:
    before, after = load(before_path), load(after_path)
    tests = sorted(set(before) & set(after))
    differing = [t for t in tests if before[t] != after[t]]
    for test in differing:
        gone = {r for result in before[test] - after[test] for r in result}
        new = {r for result in after[test] - before[test] for r in result}
        print(test)
        for row in sorted(gone - new)[:limit]:
            print("   -", row)
        for row in sorted(new - gone)[:limit]:
            print("   +", row)
    print(f"{len(tests)} tests; {len(differing)} differ")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
