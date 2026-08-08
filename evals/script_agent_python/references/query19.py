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

with urlopen("https://jsonplaceholder.typicode.com/users", timeout=30) as response:
    data = json.load(response)


def rows() -> list[dict]:
    return [
        {"id": item["id"], "name": item["name"], "email": item["email"].lower()}
        for item in data
        if item["email"].lower().endswith(".biz")
    ]
    rows.sort(key=lambda row: row["id"])


if __name__ == "__main__":
    raise SystemExit(run(rows))
