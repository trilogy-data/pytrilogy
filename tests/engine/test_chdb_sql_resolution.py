"""`_statement_to_sql` must mirror SQLAlchemy's compile-time unescaping of
`\\:` — the executor escapes colons inside string literals so text() doesn't
read them as bind params, and every path that skips real compilation has to
undo that itself. Regression for datetime literals reaching ClickHouse as
`'2024-03-15 10\\:20\\:30'`. Runs without chdb installed (pure helper)."""

from sqlalchemy import text

from trilogy.engine import escape_literal_colons, statement_to_sql


def test_no_param_branch_unescapes_colons():
    raw = "SELECT toDateTime64('2024-03-15 10:20:30', 3) AS x"
    clause = text(escape_literal_colons(raw))
    assert statement_to_sql(clause, None) == raw


def test_param_branch_unescapes_colons():
    raw = "SELECT ':not a param', :val AS v"
    clause = text(escape_literal_colons(raw))
    resolved = statement_to_sql(clause, {"val": 1})
    assert "':not a param'" in resolved
    assert "1" in resolved


def test_plain_string_passthrough():
    assert statement_to_sql("SELECT 1", None) == "SELECT 1"
