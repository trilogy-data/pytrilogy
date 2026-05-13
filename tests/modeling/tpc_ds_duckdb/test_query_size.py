from tests.modeling.tpc_ds_duckdb.query_size import query_size, strip_line_comments


def test_preql_strips_hash_line_comment():
    text = "# header note\nselect x;\n"
    assert strip_line_comments(text, "preql") == "select x;"
    assert query_size(text, "preql") == len("select x;")


def test_sql_strips_dash_line_comment():
    text = "-- header note\nselect 1;\n"
    assert strip_line_comments(text, "sql") == "select 1;"
    assert query_size(text, "sql") == len("select 1;")


def test_trailing_comment_after_code():
    assert strip_line_comments("x = 1 # note", "preql") == "x = 1"
    assert strip_line_comments("select 1 -- note", "sql") == "select 1"


def test_marker_inside_single_quoted_string_is_preserved():
    sql = "select '--still text' as v;"
    assert strip_line_comments(sql, "sql") == sql
    preql = "select '# inside' as v;"
    assert strip_line_comments(preql, "preql") == preql


def test_marker_inside_double_quoted_string_is_preserved():
    preql = 'select "# inside" as v;'
    assert strip_line_comments(preql, "preql") == preql
    sql = 'select "-- inside" as v;'
    assert strip_line_comments(sql, "sql") == sql


def test_blank_and_whitespace_only_lines_drop_out():
    text = "\n   \nselect 1;\n   \n"
    assert strip_line_comments(text, "sql") == "select 1;"


def test_empty_input_is_zero():
    assert query_size("", "preql") == 0
    assert query_size("   \n   ", "sql") == 0


def test_single_dash_is_not_a_comment():
    assert strip_line_comments("select a-b", "sql") == "select a-b"


def test_multiline_mixed():
    text = (
        "-- top\n" "select 1, '--x' as a, -- trailing\n" "       2 as b\n" "-- bottom\n"
    )
    expected = "select 1, '--x' as a,\n       2 as b"
    assert strip_line_comments(text, "sql") == expected
