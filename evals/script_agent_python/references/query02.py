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


def is_prime(value: int) -> bool:
    return value >= 2 and all(
        value % divisor for divisor in range(2, int(value**0.5) + 1)
    )


values = [value for value in range(2, 201) if is_prime(value)]


def rows() -> list[dict]:
    return [{"prime": value, "ordinal": i + 1} for i, value in enumerate(values)]


if __name__ == "__main__":
    raise SystemExit(run(rows))
