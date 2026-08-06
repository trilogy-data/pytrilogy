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

import json
from urllib.request import urlopen

from trilogy.io import run

with urlopen(
    "https://date.nager.at/api/v3/PublicHolidays/2024/US", timeout=30
) as response:
    data = json.load(response)


def rows() -> list[dict]:
    return [
        {
            "date": item["date"],
            "name": item["name"],
            "global": item["global"],
        }
        for item in data
    ]
    rows.sort(key=lambda row: (row["date"], row["name"]))


if __name__ == "__main__":
    raise SystemExit(run(rows))
