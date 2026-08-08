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

import random

from trilogy.io import run

samples = 100000
rng = random.Random(2026)
inside = sum(rng.random() ** 2 + rng.random() ** 2 <= 1 for _ in range(samples))


def rows() -> list[dict]:
    return [
        {
            "samples": samples,
            "inside_circle": inside,
            "pi_estimate": 4 * inside / samples,
        }
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
