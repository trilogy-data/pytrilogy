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

import csv
import io
from collections import defaultdict
from urllib.request import urlopen

from trilogy.io import run

with urlopen(
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv", timeout=30
) as response:
    text = response.read().decode()
groups: dict[str, list[float]] = defaultdict(list)
for item in csv.DictReader(io.StringIO(text)):
    groups[item["species"]].append(float(item["sepal_length"]))


def rows() -> list[dict]:
    return [
        {
            "species": species,
            "row_count": len(values),
            "average_sepal_length": sum(values) / len(values),
        }
        for species, values in sorted(groups.items())
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
