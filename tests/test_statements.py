from pathlib import Path

from trilogy import Dialects
from trilogy.core.enums import FunctionType
from trilogy.core.statements.execute import ProcessedCopyStatement
from trilogy.parser import parse

# from trilogy.compiler import compile


def test_declarations():
    declarations = """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");
key post_id int;
    """
    parse(declarations)


def test_duplicate_declarations():
    declarations = """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");
key post_id int;
key post_id int;
    """
    try:
        parse(declarations)
    except Exception as e:
        assert "line 5" in str(e)


def test_datasource():
    text = """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");
key post_id int;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;
"""
    parse(text)


def test_io_statement():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    target = Path(__file__).parent / "test_io_statement.csv"
    if target.exists():
        target.unlink()
    text = f"""const array <- [1,2,3,4];

auto x <- unnest(array);

copy into csv '{target}' from select x -> test order by test asc;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    assert exec.environment.concepts["x"].lineage.operator == FunctionType.UNNEST
    assert isinstance(results[-1], ProcessedCopyStatement)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "csv file was not created"


def test_io_statement_json():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    target = Path(__file__).parent / "test_io_statement.json"
    if target.exists():
        target.unlink()
    text = f"""const array <- [1,2,3,4];

auto x <- unnest(array);

copy into json `{target}` from select x -> test order by test asc;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    assert exec.environment.concepts["x"].lineage.operator == FunctionType.UNNEST
    assert isinstance(results[-1], ProcessedCopyStatement)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "json file was not created"


def test_io_statement_parquet():
    import pyarrow.parquet as pq

    target = Path(__file__).parent / "test_io_statement.parquet"
    if target.exists():
        target.unlink()
    text = f"""const array <- [1,2,3,4];

auto x <- unnest(array);

copy into parquet '{target}' from select x -> test order by test asc;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    assert exec.environment.concepts["x"].lineage.operator == FunctionType.UNNEST
    assert isinstance(results[-1], ProcessedCopyStatement)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "parquet file was not created"
    # verify the parquet file contents
    table = pq.read_table(target)
    assert table.num_rows == 4
    assert table.column_names == ["test"]
    assert table.column("test").to_pylist() == [1, 2, 3, 4]


def test_persist_to_parquet_with_column_aliases():
    """Test that persisting to parquet uses datasource column names, not concept addresses."""
    import pyarrow.parquet as pq

    target = Path(__file__).parent / "test_persist_column_names.parquet"
    if target.exists():
        target.unlink()
    text = f"""
key user_id int;
property user_id.display_name string;

datasource users (
    user_id:user_id,
    display_name:display_name
)
grain (user_id)
query '''select 1 as user_id, 'Alice' as display_name''';

datasource user_export (
    id: user_id,
    name: display_name
)
grain (user_id)
file `{target}`;

persist into user_export from select user_id, display_name;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "parquet file was not created"
    table = pq.read_table(target)
    assert table.num_rows == 1
    # Verify the column names match the datasource aliases, not the concept addresses
    assert set(table.column_names) == {"id", "name"}, f"Got {table.column_names}"
    assert table.column("id").to_pylist() == [1]
    assert table.column("name").to_pylist() == ["Alice"]
    target.unlink()


def test_persist_to_csv_with_column_aliases():
    """Test that persisting to CSV uses datasource column names, not concept addresses."""
    import csv

    target = Path(__file__).parent / "test_persist_column_names.csv"
    if target.exists():
        target.unlink()
    text = f"""
key product_id int;
property product_id.product_name string;

datasource products (
    product_id:product_id,
    product_name:product_name
)
grain (product_id)
query '''select 42 as product_id, 'Widget' as product_name''';

datasource product_export (
    pid: product_id,
    pname: product_name
)
grain (product_id)
file `{target}`;

persist into product_export from select product_id, product_name;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "csv file was not created"
    with open(target, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 1
    # Verify the column names match the datasource aliases
    assert set(rows[0].keys()) == {"pid", "pname"}, f"Got {rows[0].keys()}"
    assert rows[0]["pid"] == "42"
    assert rows[0]["pname"] == "Widget"
    target.unlink()


def test_datasource_where():
    text = """key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");
key post_id int;


datasource x_posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
    where post_id = 2
;
"""
    parse(text)
