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
from urllib.parse import urlencode
from urllib.request import urlopen

from trilogy.io import run

params = {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "start_date": "2024-01-01",
    "end_date": "2024-01-07",
    "daily": "temperature_2m_max",
    "temperature_unit": "fahrenheit",
    "timezone": "UTC",
}
with urlopen(
    "https://archive-api.open-meteo.com/v1/archive?" + urlencode(params), timeout=30
) as response:
    daily = json.load(response)["daily"]


def rows() -> list[dict]:
    return [
        {"date": day, "max_temperature_f": temperature}
        for day, temperature in zip(daily["time"], daily["temperature_2m_max"])
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
