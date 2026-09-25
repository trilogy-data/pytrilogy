"""Print the unified diff of a test's distinct compiled SQL between two dumps."""

import difflib
import sys

from sqlcmp import load


def main(before_path: str, after_path: str, needle: str) -> None:
    before, after = load(before_path), load(after_path)
    for test in sorted(set(before) | set(after)):
        if needle not in test:
            continue
        gone = sorted(before.get(test, set()) - after.get(test, set()))
        new = sorted(after.get(test, set()) - before.get(test, set()))
        if not gone and not new:
            continue
        print(f"##### {test}")
        for a, b in zip(gone, new):
            for line in difflib.unified_diff(
                a.splitlines(), b.splitlines(), "before", "after", lineterm="", n=2
            ):
                print(line)
        for extra in gone[len(new) :]:
            print("--- only before:\n" + extra)
        for extra in new[len(gone) :]:
            print("+++ only after:\n" + extra)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
