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
from collections import Counter
from urllib.request import urlopen

from trilogy.io import run

with urlopen("https://jsonplaceholder.typicode.com/albums", timeout=30) as response:
    data = json.load(response)
counts = Counter(item["userId"] for item in data)


def rows() -> list[dict]:
    return [
        {"user_id": user_id, "album_count": count}
        for user_id, count in sorted(counts.items())
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
