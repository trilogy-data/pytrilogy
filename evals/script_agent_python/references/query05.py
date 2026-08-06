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

values = [4, 8, 15, 16, 23, 42]


def rows() -> list[dict]:
    return [
        {
            "index": i,
            "value": value,
            "moving_average": sum(values[max(0, i - 2) : i + 1])
            / len(values[max(0, i - 2) : i + 1]),
        }
        for i, value in enumerate(values)
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
