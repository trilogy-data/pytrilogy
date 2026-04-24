from pathlib import Path

import duckdb
from pytest import raises

from trilogy import Dialects
from trilogy.core.enums import AddressType, FunctionType, Purpose
from trilogy.core.models.author import Function
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Address
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.executor import Executor
from trilogy.parsing.v2.model import HydrationError
from trilogy.parsing.v2.rules.datasource_rules import (
    FilePathList,
    _build_file_from_paths,
    _resolve_const_paths,
    file_node,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def test_duckdb_load():
    env = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=env)

    results = exec.execute_query(r"""
        auto csv <- _env_working_path || '/test.csv';

        RAW_SQL('''
        CREATE TABLE ages AS FROM read_csv(:csv);
        '''
        );""")

    results = exec.execute_raw_sql("SELECT * FROM ages;").fetchall()

    assert results[0].age == 23


def test_parquet_format_access():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    parquet_path = Path(__file__).parent / "customer.parquet"
    nations_path = Path(__file__).parent / "nation.parquet"
    executor.parse_text(f"""

key id int;
property id.text_id string;
property id.last_name string;
property id.first_name string;
property id.salutation string;

property id.full_name <- concat(salutation, ' ', first_name, ' ', last_name);

key nation_id int;
property nation_id.nation_name string;

datasource customers (
    C_CUSTKEY: id,
    C_NAME: full_name,
    C_NATIONKEY: nation_id,
)
grain (id)
address `{parquet_path}`;

datasource nations (
    N_NATIONKEY: nation_id,
    N_NAME: nation_name,
)
grain(nation_id)
address `{nations_path}`;
""")
    _ = executor.execute_raw_sql(f'select * from "{parquet_path}" limit 1;')
    r = executor.execute_query("select count(id) as customer_count;")

    assert r.fetchall()[0].customer_count == 1500

    r = executor.execute_query(
        "select nation_name,  count(id) as customer_count order by customer_count desc;"
    )

    assert r.fetchall()[0].customer_count == 72


def test_hive_partitioned_parquet_glob(tmp_path):
    base = tmp_path / "sales"
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE sales AS
        SELECT *
        FROM (VALUES
            ('2024-01-01', 'US', 10),
            ('2024-01-02', 'US', 20),
            ('2024-01-03', 'CA', 5),
            ('2024-01-04', 'CA', 8),
            ('2024-01-05', 'MX', 3)
        ) t(sale_date, country, amount)
        """)
    con.execute(
        f"COPY sales TO '{base.as_posix()}' (FORMAT PARQUET, PARTITION_BY (country))"
    )
    con.close()

    glob_path = (base / "**" / "*.parquet").as_posix()

    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(f"""
key country string;
key sale_date string;
property <country, sale_date>.amount int;

datasource sales (
    sale_date: sale_date,
    country: country,
    amount: amount,
)
grain (country, sale_date)
file `{glob_path}`
partition by country;
""")

    ds = list(executor.environment.datasources.values())[0]
    assert ds.address.partition_columns == ["country"]
    assert ds.address.exists is True

    sql = executor.generate_sql(
        "select country, sum(amount) as total_amount order by country asc;"
    )
    assert any("hive_partitioning=true" in s for s in sql)

    rows = executor.execute_query(
        "select country, sum(amount) as total_amount order by country asc;"
    ).fetchall()
    assert [tuple(r) for r in rows] == [("CA", 13), ("MX", 3), ("US", 30)]

    rows = executor.execute_query(
        "where country = 'US' select sale_date, amount order by sale_date asc;"
    ).fetchall()
    assert [tuple(r) for r in rows] == [("2024-01-01", 10), ("2024-01-02", 20)]


def test_file_const_ref(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT 1 AS id, 'alpha' AS name) TO '{a.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT 2 AS id, 'beta' AS name) TO '{b.as_posix()}' (FORMAT PARQUET)"
    )
    con.close()

    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(f"""
const URLS <- ['{a.as_posix()}', '{b.as_posix()}'];

key id int;
property id.name string;

datasource things (
    id: id,
    name: name,
)
grain (id)
file URLS;
""")

    ds = [d for d in executor.environment.datasources.values() if d.name == "things"][0]
    assert len(ds.address.all_locations) == 2
    assert ds.address.additional_locations
    rows = executor.execute_query("select id, name order by id asc;").fetchall()
    assert [tuple(r) for r in rows] == [(1, "alpha"), (2, "beta")]

    # Scalar const: single path
    executor2: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor2.parse_text(f"""
const ONE_URL <- '{a.as_posix()}';

key id int;
property id.name string;

datasource things (
    id: id,
    name: name,
)
grain (id)
file ONE_URL;
""")
    ds2 = [d for d in executor2.environment.datasources.values() if d.name == "things"][
        0
    ]
    assert ds2.address.location.endswith("a.parquet")
    assert ds2.address.additional_locations == []


def test_file_path_array(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT 1 AS id, 'alpha' AS name) TO '{a.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT 2 AS id, 'beta' AS name) TO '{b.as_posix()}' (FORMAT PARQUET)"
    )
    con.close()

    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(f"""
key id int;
property id.name string;

datasource things (
    id: id,
    name: name,
)
grain (id)
file [`{a.as_posix()}`, `{b.as_posix()}`];
""")

    ds = list(executor.environment.datasources.values())[0]
    assert len(ds.address.all_locations) == 2
    assert ds.address.additional_locations  # array form active

    sql = executor.generate_sql("select id, name order by id asc;")
    assert "read_parquet([" in sql[0]

    rows = executor.execute_query("select id, name order by id asc;").fetchall()
    assert [tuple(r) for r in rows] == [(1, "alpha"), (2, "beta")]


def test_file_unhappy_paths():
    # file IDENTIFIER — identifier is not defined anywhere
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    with raises(HydrationError, match="Unknown reference 'MISSING'"):
        ex.parse_text("""
key id int;
property id.name string;
datasource things (id: id, name: name) grain (id) file MISSING;
""")

    # file IDENTIFIER — identifier resolves but is not a constant
    ex2 = Dialects.DUCK_DB.default_executor(environment=Environment())
    with raises(HydrationError, match="must reference a constant"):
        ex2.parse_text("""
key id int;
property id.name string;
auto derived <- 1 + 1;
datasource things (id: id, name: name) grain (id) file derived;
""")

    # file [a.csv, b.parquet] — mixed extensions
    ex3 = Dialects.DUCK_DB.default_executor(environment=Environment())
    with raises(HydrationError, match="must share the same extension"):
        ex3.parse_text("""
key id int;
property id.name string;
datasource things (id: id, name: name) grain (id)
file [`/tmp/a.csv`, `/tmp/b.parquet`];
""")


def test_address_helpers():
    # is_glob
    assert Address(location="data/*.parquet", type=AddressType.PARQUET).is_glob
    assert Address(location="data/?.parquet", type=AddressType.PARQUET).is_glob
    assert Address(location="data/[ab].parquet", type=AddressType.PARQUET).is_glob
    assert not Address(location="data/one.parquet", type=AddressType.PARQUET).is_glob

    # all_locations — array and single forms
    single = Address(location="a.parquet", type=AddressType.PARQUET)
    assert single.all_locations == ["a.parquet"]

    multi = Address(
        location="a.parquet",
        type=AddressType.PARQUET,
        additional_locations=["b.parquet", "c.parquet"],
    )
    assert multi.all_locations == ["a.parquet", "b.parquet", "c.parquet"]


def test_file_defensive_guards():
    """Defensive checks in _build_file_from_paths / _resolve_const_paths / file_node
    that the grammar already prevents; call the helpers directly so the error
    branches aren't dead code on the coverage report."""
    fake_node = SyntaxNode(name="file", children=[], kind=SyntaxNodeKind.FILE)

    class _Ctx:
        class environment:
            working_path = "."
            concepts: dict = {}

    # Empty path list → "must share the same extension" (no suffixes in set).
    with raises(HydrationError, match="must share the same extension"):
        _build_file_from_paths(fake_node, _Ctx(), [])

    # Unsupported extension → "Unsupported file type"
    with raises(HydrationError, match="Unsupported file type"):
        _build_file_from_paths(fake_node, _Ctx(), ["https://host/data.xyz"])

    # _resolve_const_paths with unknown name → "Unknown reference"
    class _Ctx2:
        class environment:
            working_path = "."

            class concepts:
                @staticmethod
                def get(_):
                    return None

    with raises(HydrationError, match="Unknown reference 'NOPE'"):
        _resolve_const_paths(fake_node, _Ctx2(), "NOPE")

    # _resolve_const_paths with empty list const → "is empty"
    empty_fn = Function(
        operator=FunctionType.CONSTANT,
        arguments=[()],  # empty tuple bypasses parse-time empty-list rejection
        output_datatype=DataType.STRING,
        output_purpose=Purpose.CONSTANT,
    )

    class _EmptyConstCtx:
        class environment:
            working_path = "."

            class concepts:
                @staticmethod
                def get(_):
                    class _Concept:
                        lineage = empty_fn
                        purpose = "CONSTANT"

                    return _Concept()

    with raises(HydrationError, match="is empty"):
        _resolve_const_paths(fake_node, _EmptyConstCtx(), "EMPTY")

    # file_node with a synthesized empty FilePathList → "requires at least one path"
    node_with_empty_list = SyntaxNode(
        name="file",
        children=[object()],  # placeholder; hydrate returns our FilePathList
        kind=SyntaxNodeKind.FILE,
    )
    with raises(HydrationError, match="requires at least one path"):
        file_node(node_with_empty_list, _Ctx(), lambda _child: FilePathList(paths=[]))


def test_file_relative_path(tmp_path, monkeypatch):
    """Relative paths in `file` should resolve against the environment's
    working_path. Covers the relative-path branch of _process_file_path."""
    (tmp_path / "data").mkdir()
    p = tmp_path / "data" / "rel.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT 1 AS id, 'x' AS name) TO '{p.as_posix()}' (FORMAT PARQUET)"
    )
    con.close()

    monkeypatch.chdir(tmp_path)
    env = Environment(working_path=str(tmp_path))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.parse_text("""
key id int;
property id.name string;
datasource t (id: id, name: name) grain (id)
file `./data/rel.parquet`;
""")
    rows = executor.execute_query("select id, name;").fetchall()
    assert [tuple(r) for r in rows] == [(1, "x")]


def test_gcs_cache_bust_render_source():
    config = DuckDBConfig(gcs_cache_bust=True)
    dialect = DuckDBDialect(config=config)
    assert dialect._gcs_cache_bust_token is not None

    gcs_address = Address(
        location="gcs://bucket/data.parquet", type=AddressType.PARQUET
    )
    gs_address = Address(location="gs://bucket/data.parquet", type=AddressType.PARQUET)
    gcs_https_address = Address(
        location="https://storage.googleapis.com/bucket/data.parquet",
        type=AddressType.PARQUET,
    )
    local_address = Address(location="./local/data.parquet", type=AddressType.PARQUET)
    other_https_address = Address(
        location="https://example.com/data.parquet", type=AddressType.PARQUET
    )

    token = dialect._gcs_cache_bust_token
    assert f"?cache_bust={token}" in dialect.render_source(gcs_address)
    assert f"?cache_bust={token}" in dialect.render_source(gs_address)
    assert f"?cache_bust={token}" in dialect.render_source(gcs_https_address)
    assert "cache_bust" not in dialect.render_source(local_address)
    assert "cache_bust" not in dialect.render_source(other_https_address)


def test_gcs_cache_bust_disabled_by_default():
    dialect = DuckDBDialect()
    assert dialect._gcs_cache_bust_token is None

    gcs_address = Address(
        location="gcs://bucket/data.parquet", type=AddressType.PARQUET
    )
    assert "cache_bust" not in dialect.render_source(gcs_address)


def test_gcs_cache_bust_token_unique_per_instance():
    config = DuckDBConfig(gcs_cache_bust=True)
    d1 = DuckDBDialect(config=config)
    d2 = DuckDBDialect(config=config)
    assert d1._gcs_cache_bust_token != d2._gcs_cache_bust_token
