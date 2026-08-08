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

from trilogy.io import run

source = (
    "product,quantity,price\napple,3,1.20\nbanana,5,0.50\napple,2,1.20\npear,4,0.80\n"
)
totals: dict[str, list[float]] = defaultdict(lambda: [0, 0.0])
for item in csv.DictReader(io.StringIO(source)):
    totals[item["product"]][0] += int(item["quantity"])
    totals[item["product"]][1] += int(item["quantity"]) * float(item["price"])


def rows() -> list[dict]:
    return [
        {"product": product, "quantity": int(values[0]), "revenue": values[1]}
        for product, values in sorted(totals.items())
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
