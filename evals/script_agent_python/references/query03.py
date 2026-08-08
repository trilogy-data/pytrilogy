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


def collatz(value: int) -> list[int]:
    values = [value]
    while value != 1:
        value = value // 2 if value % 2 == 0 else 3 * value + 1
        values.append(value)
    return values


def rows() -> list[dict]:
    return [{"step": i, "value": value} for i, value in enumerate(collatz(27))]


if __name__ == "__main__":
    raise SystemExit(run(rows))
