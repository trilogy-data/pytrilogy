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

xs = [1, 2, 3, 4, 5]
ys = [2, 3, 5, 4, 6]
x_mean = sum(xs) / len(xs)
y_mean = sum(ys) / len(ys)
slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / sum(
    (x - x_mean) ** 2 for x in xs
)


def rows() -> list[dict]:
    return [{"slope": slope, "intercept": y_mean - slope * x_mean}]


if __name__ == "__main__":
    raise SystemExit(run(rows))
