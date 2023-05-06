from preql.parser import parse

# from preql.compiler import compile


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
