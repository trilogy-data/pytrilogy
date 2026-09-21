"""Diff two ks_sqldump files: the tests whose DISTINCT compiled SQL differs.
Distinct, because the benchmark tests compile a varying number of times."""

import json
import sys
from collections import defaultdict


def load(path: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            out[row["test"]].add(row["sql"])
    return out


def main(before_path: str, after_path: str) -> None:
    before, after = load(before_path), load(after_path)
    tests = sorted(set(before) | set(after))
    differing = [t for t in tests if before.get(t, set()) != after.get(t, set())]
    for test in differing:
        gone = before.get(test, set()) - after.get(test, set())
        new = after.get(test, set()) - before.get(test, set())
        print(
            f"{test}: {len(gone)} -> {len(new)} statements,"
            f" {sum(map(len, gone))} -> {sum(map(len, new))} chars"
        )
    print(f"{len(tests)} tests; {len(differing)} differ")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
