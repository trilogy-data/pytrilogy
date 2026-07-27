"""Asset keying rules: a stable, purely physical identity per address type.

Keys dispatch on ``AddressType`` and contain nothing logical — no script, no
datasource name — so two scripts writing the same file key to the same asset.
The absolute path of a file changes per checkout, and per run under an
orchestrator that materializes into a fresh scratch dir, so it can never be
the identity. Two address types are not plain data artifacts and carry a type
label: a Python datasource script (a procedure) and an inline query (no
artifact at all).
"""

import sys
from pathlib import PurePath

import pytest

from trilogy.core.enums import AddressType
from trilogy.execution.state.snapshot import (
    is_remote_address,
    project_relative_path,
    query_digest,
    stable_asset_key,
)

WIN = sys.platform == "win32"
ROOT = PurePath("C:/proj" if WIN else "/proj")
FILE = str(ROOT / "data" / "out.parquet")
PY_SOURCE = str(ROOT / "ingest" / "load.py")
SQL_FILE = str(ROOT / "ingest" / "load.sql")


def test_remote_addresses_are_never_relativized():
    assert is_remote_address("gs://bucket/obj.parquet") is True
    assert is_remote_address("s3://bucket/obj.parquet") is True
    assert is_remote_address("https://example.com/x.csv") is True
    assert is_remote_address(FILE) is False
    assert is_remote_address("schema.target_table") is False


@pytest.mark.parametrize(
    "address,address_type",
    [
        ("schema.target_table", AddressType.TABLE),
        ("bare_table", AddressType.TABLE),
        ("gs://bucket/obj.parquet", AddressType.PARQUET),
        ("s3://bucket/obj.csv", AddressType.CSV),
    ],
)
def test_tables_and_remote_objects_pass_through_verbatim(address, address_type):
    """Already stable, and shared by every model pointing at the same object."""
    assert stable_asset_key(address, address_type, ROOT) == address


def test_project_relative_path_inside_and_outside_root():
    assert project_relative_path(FILE, ROOT) == "data/out.parquet"
    outside = str(PurePath("C:/elsewhere/x.csv" if WIN else "/elsewhere/x.csv"))
    # Outside the root the absolute form is kept: stable on one machine, and
    # relativizing against nothing would manufacture collisions.
    assert project_relative_path(outside, ROOT) == outside
    assert project_relative_path(FILE, None) == FILE


def test_file_key_is_the_project_relative_path():
    assert stable_asset_key(FILE, AddressType.PARQUET, ROOT) == "data/out.parquet"


def test_key_carries_nothing_logical():
    """The identity is the physical pointer. Two scripts writing the same file
    must produce the same key — it is one asset, however many scripts see it."""
    assert stable_asset_key(FILE, AddressType.PARQUET, ROOT) == stable_asset_key(
        FILE, AddressType.PARQUET, ROOT
    )
    # The signature has no owner/script parameter at all to smuggle one in.
    with pytest.raises(TypeError):
        stable_asset_key(FILE, AddressType.PARQUET, ROOT, owner_script="a.preql")  # type: ignore[call-arg]


def test_already_relative_file_path_relativizes_to_itself():
    """A preql that wrote a relative file address is already stable; keying it
    must be idempotent rather than manufacture a second identity."""
    assert stable_asset_key("data/out.parquet", AddressType.PARQUET, ROOT) == (
        "data/out.parquet"
    )


def test_sql_file_is_a_file_not_an_inline_query():
    """``AddressType.SQL`` is a ``.sql`` FILE — only ``query '''...'''`` text is
    QUERY. Digesting a .sql file would hash its absolute path: unportable, and
    mislabeled as a query."""
    assert stable_asset_key(SQL_FILE, AddressType.SQL, ROOT) == "ingest/load.sql"


def test_python_script_source_is_labeled():
    """A datasource script is a procedure, not a data artifact — its path is
    the identity, labeled so a reader can tell the two apart."""
    assert (
        stable_asset_key(PY_SOURCE, AddressType.PYTHON_SCRIPT, ROOT)
        == "script::ingest/load.py"
    )


def test_inline_query_is_keyed_by_digest():
    sql = "\nSELECT 1 AS id, TIMESTAMP '2024-01-10' AS ts\n"
    key = stable_asset_key(sql, AddressType.QUERY, ROOT)
    assert key == f"query::{query_digest(sql)}"
    assert len(query_digest(sql)) == 16


def test_query_digest_survives_reformatting_but_tracks_content():
    """Reindenting a query must not churn its identity; changing it must."""
    a = "SELECT 1 AS id"
    b = "\n   SELECT   1   AS id\n"
    c = "SELECT 2 AS id"
    assert query_digest(a) == query_digest(b)
    assert query_digest(a) != query_digest(c)
