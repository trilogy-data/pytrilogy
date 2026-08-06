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

from trilogy.io import run

data = json.loads(
    '{"departments":[{"name":"engineering","employees":[{"name":"Ada","level":3},{"name":"Linus","level":4}]},{"name":"sales","employees":[{"name":"Grace","level":2}]}]}'
)


def rows() -> list[dict]:
    return [
        {
            "department": department["name"],
            "employee": employee["name"],
            "level": employee["level"],
        }
        for department in data["departments"]
        for employee in department["employees"]
    ]
    rows.sort(key=lambda row: (row["department"], row["employee"]))


if __name__ == "__main__":
    raise SystemExit(run(rows))
