"""Validate a Trilogy datasource that reads many HTTPS parquet files.

The TRILOGY block below is self-contained — copy it verbatim into a .preql
file and it will parse and execute. No Python logic is needed to build it.
"""

from __future__ import annotations

from trilogy import Dialects
from trilogy.core.models.environment import Environment

TRILOGY = """
const FLIGHT_URLS <- [
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_1987.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_1988.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_1989.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2000.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2001.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2002.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2003.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2004.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2005.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2006.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2007.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2008.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2009.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2010.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2011.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2012.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2013.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2014.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2015.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2016.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2017.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2018.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2019.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2020.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2021.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2022.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2023.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2024.parquet',
    'https://storage.googleapis.com/trilogy_public_models/duckdb/faa/flights/flights_2025.parquet',
];

key id int;
key carrier string;
key flight_date date;
property id.flight_num string;
property id.distance int;

datasource flight (
    id2: id,
    carrier: carrier,
    flight_date: flight_date,
    flight_num: flight_num,
    distance: distance,
)
grain (id)
file FLIGHT_URLS;
"""


def main() -> None:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(TRILOGY)

    ds = [d for d in executor.environment.datasources.values() if d.name == "flight"][0]
    assert ds.address.type.value == "parquet"
    assert len(ds.address.all_locations) == 29

    query = (
        "select carrier, count(id) as flight_count "
        "order by flight_count desc limit 5;"
    )
    print("-- Generated SQL (excerpt) --")
    print(executor.generate_sql(query)[0][:220], "...\n")

    rows = executor.execute_query(query).fetchall()
    print(
        f"Read {len(ds.address.all_locations)} files from GCS via HTTPS "
        "(via const FLIGHT_URLS)."
    )
    print("Top 5 carriers by flight count:")
    for r in rows:
        print(f"  {r.carrier}: {r.flight_count:,}")


if __name__ == "__main__":
    main()
