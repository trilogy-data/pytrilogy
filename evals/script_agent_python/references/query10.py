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

left = [[1, 2, 3], [4, 5, 6]]
right = [[7, 8], [9, 10], [11, 12]]


def rows() -> list[dict]:
    return [
        {
            "row": i,
            "column": j,
            "value": sum(left[i][k] * right[k][j] for k in range(3)),
        }
        for i in range(2)
        for j in range(2)
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
