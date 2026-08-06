#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy"]
#
# # Resolved from this checkout so the fixture exercises the current contract.
# [tool.uv.sources]
# pytrilogy = { path = "../../../", editable = true }
# ///
"""Fixture for filter-pushdown tests.

Claims no pushdown, so every predicate the planner sends is applied by the
wrapper's fallback -- which is the case that must stay correct whether or not
the planner pushes anything.
"""

from trilogy.io import run

STATES = ["CA", "NY", "TX", "WA"]
LABELS = ["north side", "south side"]


def rows() -> list[dict]:
    return [
        {
            "id": i,
            "state": STATES[i % 4],
            "label": LABELS[i % 2],
            "score": i * 1.5,
        }
        for i in range(40)
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
