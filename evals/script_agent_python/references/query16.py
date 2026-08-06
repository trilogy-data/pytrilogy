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

from datetime import date

from trilogy.io import run


def rows() -> list[dict]:
    return [
        {"date": date(year, month, 13).isoformat()}
        for year in range(2020, 2031)
        for month in range(1, 13)
        if date(year, month, 13).weekday() == 4
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
