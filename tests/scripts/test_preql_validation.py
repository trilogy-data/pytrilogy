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


def test_detect_grouping_after_having():
    assert pv.detect_grouping_after_having(
        "select s.c, sum(s.a) as t having sum(s.a) > 5 by rollup (s.c);"
    )
    assert pv.detect_grouping_after_having(
        "select x, sum(y) as t\nhaving sum(y) > 5\nby cube (x)\norder by x;"
    )
    # Correct order (grouping clause before HAVING) must not match.
    assert not pv.detect_grouping_after_having(
        "select s.c, sum(s.a) as t by rollup (s.c) having sum(s.a) > 5;"
    )
    # A grouping clause in a *separate* statement after a having select must not match.
    assert not pv.detect_grouping_after_having(
        "select z having count(z) > 1; select x, sum(y) as t by rollup (x);"
    )


def test_validate_preql_content_hints_grouping_after_having():
    """The legacy order (HAVING before `by rollup`) is a parse error with an
    opaque "expected order_by or limit". The refusal must add the reorder hint."""
    content = (
        "key sid int;\nproperty sid.c string;\nproperty sid.a float;\n"
        "datasource s (sid: sid, c: c, a: a) grain (sid)\n"
        "query '''select 1 as sid, 'x' as c, 1.0 as a''';\n"
        "select s.c, sum(s.a) as t\nhaving sum(s.a) > 5\nby rollup (s.c)\n"
        "order by s.c;\n"
    )
    msg = pv.validate_preql_content("q.preql", content)
    assert msg is not None
    assert "not syntactically valid Trilogy" in msg
    assert "must come *before* HAVING" in msg


def test_validate_preql_syntax_lark_fallback_when_wheel_absent(monkeypatch):
    """If the rust parser raises ImportError, validation falls back to lark."""
    import trilogy.parsing.parse_engine_v2 as pe

    def boom(_content):
        raise ImportError("rust wheel missing")

    monkeypatch.setattr(pe, "parse_syntax", boom)
    # Valid syntax → lark agrees → None.
    assert pv.validate_preql_syntax("select 1 -> answer;") is None
    # Invalid syntax → lark raises InvalidSyntaxException → returned as a string.
    err = pv.validate_preql_syntax("definitely !!! not valid")
    assert err is not None
    assert isinstance(err, str)


def test_validate_preql_syntax_lark_safety_net(monkeypatch):
    """A non-syntax error from the lark fallback is caught, not raised."""
    import trilogy.parsing.parse_engine_v2 as pe
    import trilogy.parsing.v2.lark_backend as lark

    def boom(_content):
        raise ImportError("rust wheel missing")

    def explode(_content):
        raise ValueError("unexpected lark crash")

    monkeypatch.setattr(pe, "parse_syntax", boom)
    monkeypatch.setattr(lark, "parse_lark", explode)
    err = pv.validate_preql_syntax("select 1 -> answer;")
    assert err == "ValueError: unexpected lark crash"
