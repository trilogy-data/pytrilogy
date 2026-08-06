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


def roman(value: int) -> str:
    pairs = [(10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")]
    output = ""
    for amount, symbol in pairs:
        while value >= amount:
            output += symbol
            value -= amount
    return output


def rows() -> list[dict]:
    return [{"number": value, "roman": roman(value)} for value in range(1, 26)]


if __name__ == "__main__":
    raise SystemExit(run(rows))
