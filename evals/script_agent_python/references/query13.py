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

import itertools

from trilogy.io import run

items = [("A", 6, 30), ("B", 3, 14), ("C", 4, 16), ("D", 2, 9)]
options = []
for size in range(len(items) + 1):
    for chosen in itertools.combinations(items, size):
        weight = sum(item[1] for item in chosen)
        if weight <= 10:
            options.append(
                (
                    sum(item[2] for item in chosen),
                    ",".join(item[0] for item in chosen),
                    weight,
                )
            )
value, names, weight = max(
    options, key=lambda item: (item[0], tuple(-ord(c) for c in item[1]))
)


def rows() -> list[dict]:
    return [{"items": names, "weight": weight, "value": value}]


if __name__ == "__main__":
    raise SystemExit(run(rows))
