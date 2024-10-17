from trilogy.parser import parse
from trilogy import Dialects
from trilogy.core.models import ProcessedCopyStatement
from pathlib import Path

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
    address bigquery-public-data.stackoverflow.post_history
;
"""
    parse(text)


def test_io_statement():
    target = Path(__file__).parent / "test_io_statement.csv"
    if target.exists():
        target.unlink()
    text = f"""const array <- [1,2,3,4];

auto x <- unnest(array);

copy into csv '{target}' from select x -> test;
"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.parse_text(text)
    assert isinstance(results[-1], ProcessedCopyStatement)
    for z in results:
        exec.execute_query(z)
    assert target.exists(), "csv file was not created"


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
    address bigquery-public-data.stackoverflow.post_history
    where post_id = 2
;
"""
    parse(text)
