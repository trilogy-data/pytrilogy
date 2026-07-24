"""`select natural '...'` and the `validate ... matches ( select ... )` branch:
grammar (both backends), hydration, config coercion, inert execution, and
render round-trip."""

from __future__ import annotations

import pytest

from trilogy import Dialects
from trilogy.core.enums import QueryComparison
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    NaturalSelectStatement,
    ValidateNaturalStatement,
)
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_MODEL = """key order_id int;
property order_id.amount float;
property order_id.natural string;
datasource orders (order_id: order_id, amount: amount, natural: natural) grain (order_id) address orders_tbl;
"""

_STATEMENTS = [
    "select natural 'which order is largest?';",
    "validate biggest select natural 'largest order?' matches ( select max(amount) -> m );",
    "validate select natural 'largest order?' matches ( select max(amount) -> m );",
    "validate b select natural 'q' matches ( select amount ) with (repetitions = 3, target = 0.5, comparison = exact, tags = ['smoke'], timeout = 60);",
    "show select natural 'q?';",
    "show validate b select natural 'q?' matches ( select amount );",
    # `natural` stays usable as a concept name / identifier prefix
    "select natural, amount;",
    "select order_id where natural = 'x';",
    # existing validate family unaffected
    "validate all;",
    "validate concepts order_id;",
    "validate datasources orders;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("statement", _STATEMENTS)
def test_grammar_parses_both_backends(backend, statement):
    backend(_MODEL + statement)


def test_hydration_defaults():
    env = Environment()
    _, parsed = parse_text(
        _MODEL + "validate select natural 'q' matches ( select amount );", env
    )
    statement = parsed[-1]
    assert isinstance(statement, ValidateNaturalStatement)
    assert statement.name is None
    assert statement.query.question == "q"
    assert statement.repetitions == 1
    assert statement.target == 1.0
    assert statement.comparison is QueryComparison.TOLERANT
    assert statement.tags == []
    assert statement.timeout is None


def test_hydration_full_config():
    env = Environment()
    _, parsed = parse_text(
        _MODEL + "validate b select natural 'q' matches ( select amount ) "
        "with (repetitions = 3, target = 0.5, comparison = ordered, "
        "tags = ['smoke', 'core'], timeout = 60);",
        env,
    )
    statement = parsed[-1]
    assert isinstance(statement, ValidateNaturalStatement)
    assert statement.name == "b"
    assert statement.repetitions == 3
    assert statement.target == 0.5
    assert statement.comparison is QueryComparison.ORDERED
    assert statement.tags == ["smoke", "core"]
    assert statement.timeout == 60


def test_standalone_natural_select_hydrates():
    env = Environment()
    _, parsed = parse_text(_MODEL + "select natural 'how many?';", env)
    statement = parsed[-1]
    assert isinstance(statement, NaturalSelectStatement)
    assert statement.question == "how many?"


@pytest.mark.parametrize(
    "config, message",
    [
        ("passrate = 0.5", "Unknown validation option"),
        ("target = 1.5", "must be in"),
        ("target = 'high'", "must be a number"),
        ("repetitions = 0", "must be >= 1"),
        ("comparison = fuzzy", "Unknown comparison"),
        ("tags = 'smoke'", "must be a list of strings"),
        ("repetitions = 2, repetitions = 3", "Duplicate validation option"),
    ],
)
def test_config_errors(config, message):
    env = Environment()
    with pytest.raises(Exception, match=message):
        parse_text(
            _MODEL
            + f"validate b select natural 'q' matches ( select amount ) with ({config});",
            env,
        )


def test_broken_expected_select_fails_at_parse():
    env = Environment()
    with pytest.raises(Exception, match="nonexistent"):
        parse_text(
            _MODEL + "validate b select natural 'q' matches ( select nonexistent );",
            env,
        )


def test_inert_execution_returns_skipped_row():
    executor = Dialects.DUCK_DB.default_executor()
    statements = executor.parse_text(
        _MODEL
        + "validate b select natural 'largest?' matches ( select max(amount) -> m );"
    )
    result = executor.execute_statement(statements[-1])
    rows = result.fetchall()
    assert len(rows) == 1
    assert "skipped" in rows[0]["status"]


def test_show_paths_execute_without_llm():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_MODEL)
    for statement in (
        "show validate b select natural 'q?' matches ( select amount );",
        "show select natural 'q?';",
    ):
        results = executor.execute_text(statement)
        assert results, statement
        assert results[-1].fetchall(), statement


def test_render_round_trip():
    env = Environment()
    src = (
        _MODEL + "validate biggest select natural 'largest order?' matches (\n"
        "select max(amount) -> m\n"
        ") with (repetitions = 2, target = 0.5, tags = ['smoke']);\n"
        "select natural 'count?';\n"
    )
    _, parsed = parse_text(src, env)
    renderer = Renderer()
    rendered = "\n".join(renderer.to_string(s) for s in parsed)
    env2 = Environment()
    _, reparsed = parse_text(rendered, env2)
    validates = [s for s in reparsed if isinstance(s, ValidateNaturalStatement)]
    naturals = [s for s in reparsed if isinstance(s, NaturalSelectStatement)]
    assert len(validates) == 1
    assert len(naturals) == 1
    assert validates[0].name == "biggest"
    assert validates[0].repetitions == 2
    assert validates[0].target == 0.5
    assert validates[0].tags == ["smoke"]
    assert naturals[0].question == "count?"
