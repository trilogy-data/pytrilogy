#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy"]
#
# # Resolved from this checkout, not PyPI: these references are ground truth
# # for the current code, not whatever wheel was last published.
# [tool.uv.sources]
# pytrilogy = { path = "../../../", editable = true }
# ///

from trilogy.io import run


def distance(left: str, right: str) -> int:
    prior = list(range(len(right) + 1))
    for i, a in enumerate(left, 1):
        current = [i]
        for j, b in enumerate(right, 1):
            current.append(min(current[-1] + 1, prior[j] + 1, prior[j - 1] + (a != b)))
        prior = current
    return prior[-1]


pairs = [("kitten", "sitting"), ("flaw", "lawn"), ("trilogy", "trilogy")]


def rows() -> list[dict]:
    return [
        {"left_text": left, "right_text": right, "distance": distance(left, right)}
        for left, right in sorted(pairs)
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
