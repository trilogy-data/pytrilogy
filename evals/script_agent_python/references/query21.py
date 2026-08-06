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
from collections import defaultdict
from urllib.request import urlopen

from trilogy.io import run

with urlopen("https://jsonplaceholder.typicode.com/todos", timeout=30) as response:
    data = json.load(response)
counts: dict[int, list[int]] = defaultdict(lambda: [0, 0])
for item in data:
    counts[item["userId"]][0 if item["completed"] else 1] += 1


def rows() -> list[dict]:
    return [
        {"user_id": user, "completed_count": values[0], "incomplete_count": values[1]}
        for user, values in sorted(counts.items())
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
