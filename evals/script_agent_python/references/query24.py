#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

import pyarrow as pa

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
rows = [
    {"date": day, "max_temperature_f": temperature}
    for day, temperature in zip(daily["time"], daily["temperature_2m_max"])
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
