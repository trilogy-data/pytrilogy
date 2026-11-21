"""Tests for the pre-write .preql validation helpers."""

from trilogy.scripts.file_helpers import preql_validation as pv


def test_detect_html_escapes_finds_known_entities():
    assert set(pv.detect_html_escapes("a &lt;= b &amp; c")) == {"&lt;", "&amp;"}
    assert pv.detect_html_escapes("clean content") == []


def test_validate_preql_content_skips_non_preql():
    assert pv.validate_preql_content("notes.md", "anything <= here") is None


def test_validate_preql_content_rejects_html_escapes():
    msg = pv.validate_preql_content("q.preql", "where x &lt;= 5 select x;")
    assert msg is not None
    assert "HTML-escaped" in msg
    assert "&lt;" in msg


def test_validate_preql_content_rejects_invalid_syntax():
    msg = pv.validate_preql_content("q.preql", "this is not valid trilogy !!!")
    assert msg is not None
    assert "not syntactically valid Trilogy" in msg


def test_validate_preql_content_allows_valid_syntax():
    assert pv.validate_preql_content("q.preql", "select 1 -> answer;") is None


def test_detect_grouping_clause():
    assert pv.detect_grouping_clause(
        "select s.c, sum(s.a) as t having sum(s.a) > 5 by rollup (s.c);"
    )
    assert pv.detect_grouping_clause(
        "select x, sum(y) as t\nby cube (x)\nhaving sum(y) > 5\norder by x;"
    )
    assert pv.detect_grouping_clause("select x, sum(y) as t by grouping sets ((x));")
    assert not pv.detect_grouping_clause("select *rollup((select x, sum(y) as t), x);")
    # a raw SQL body and a comment are not the clause
    assert not pv.detect_grouping_clause(
        "datasource d (x: x) query '''select x from t group by rollup (x)''';"
    )
    assert not pv.detect_grouping_clause("# no by rollup (x) here\nselect x;")


MODEL = (
    "key sid int;\nproperty sid.c string;\nproperty sid.a float;\n"
    "datasource s (sid: sid, c: c, a: a) grain (sid)\n"
    "query '''select 1 as sid, 'x' as c, 1.0 as a''';\n"
)


def test_validate_preql_content_refuses_the_removed_grouping_clause():
    """The clause still parses where it used to be legal, so a syntax-only check
    has to refuse it itself."""
    content = MODEL + "select s.c, sum(s.a) as t by rollup (s.c) order by s.c;\n"
    msg = pv.validate_preql_content("q.preql", content)
    assert msg is not None
    assert "select *rollup((select" in msg
    migrated = (
        MODEL + "select *rollup((select s.c, sum(s.a) as t order by s.c), s.c);\n"
    )
    assert pv.validate_preql_content("q.preql", migrated) is None


def test_validate_preql_content_hints_grouping_after_having():
    """After HAVING the removed clause is a parse error with an opaque
    "expected order_by or limit". The refusal must add the call-form hint."""
    content = MODEL + (
        "select s.c, sum(s.a) as t\nhaving sum(s.a) > 5\nby rollup (s.c)\n"
        "order by s.c;\n"
    )
    msg = pv.validate_preql_content("q.preql", content)
    assert msg is not None
    assert "not syntactically valid Trilogy" in msg
    assert "select *rollup((select" in msg
