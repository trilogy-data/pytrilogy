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
    SelectStatement,
    ValidateNaturalStatement,
)
from trilogy.core.statements.execute import (
    ProcessedNaturalSelectStatement,
    ProcessedValidateNaturalStatement,
)
from trilogy.hooks.base_hook import BaseHook
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.model import HydrationError
from trilogy.parsing.v2.pest_backend import parse_pest
from trilogy.parsing.v2.rules.operational_rules import (
    natural_select_statement,
    validate_query_config,
    validate_query_option,
    validate_statement,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
)

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


def test_comparison_enum_accepts_mixed_case():
    assert QueryComparison("ORDERED") is QueryComparison.ORDERED
    with pytest.raises(ValueError):
        QueryComparison("fuzzy")


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
        ("comparison = 5", "must be one of"),
        ("tags = 'smoke'", "must be a list of strings"),
        ("tags = [1, 2]", "must be a list of strings"),
        ("repetitions = 2, repetitions = 3", "Duplicate validation option"),
        ("repetitions = 1.5", "must be an integer"),
        ("timeout = 0", "must be >= 1"),
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


def _node(kind, children=()):
    return SyntaxNode(name=kind.value, children=list(children), kind=kind)


def _token(kind, value):
    return SyntaxToken(name=kind.value, value=value, kind=kind)


def _natural_node():
    return _node(SyntaxNodeKind.NATURAL_SELECT_STATEMENT)


@pytest.mark.parametrize(
    "node, hydrate, rule, message",
    [
        (
            _node(SyntaxNodeKind.NATURAL_SELECT_STATEMENT),
            lambda n: n,
            natural_select_statement,
            "missing its question string",
        ),
        (
            _node(SyntaxNodeKind.VALIDATE_QUERY_OPTION),
            lambda n: n,
            validate_query_option,
            "missing name",
        ),
        (
            _node(
                SyntaxNodeKind.VALIDATE_QUERY_OPTION,
                [_token(SyntaxTokenKind.IDENTIFIER, "target")],
            ),
            lambda n: n,
            validate_query_option,
            "missing value",
        ),
        (
            _node(
                SyntaxNodeKind.VALIDATE_QUERY_CONFIG,
                [_node(SyntaxNodeKind.VALIDATE_QUERY_OPTION)],
            ),
            lambda n: "not-a-tuple",
            validate_query_config,
            "failed to hydrate",
        ),
        (
            _node(SyntaxNodeKind.VALIDATE_STATEMENT, [_natural_node()]),
            lambda n: "not-a-natural-select",
            validate_statement,
            "Natural select failed to hydrate",
        ),
        (
            _node(SyntaxNodeKind.VALIDATE_STATEMENT, [_natural_node()]),
            lambda n: NaturalSelectStatement(question="q"),
            validate_statement,
            "missing its expected select",
        ),
        (
            _node(
                SyntaxNodeKind.VALIDATE_STATEMENT,
                [_natural_node(), _node(SyntaxNodeKind.SELECT_STATEMENT)],
            ),
            lambda n: (
                NaturalSelectStatement(question="q")
                if n.kind is SyntaxNodeKind.NATURAL_SELECT_STATEMENT
                else "not-a-select"
            ),
            validate_statement,
            "Expected select failed to hydrate",
        ),
    ],
)
def test_malformed_syntax_tree_fails_loudly(node, hydrate, rule, message):
    """Hydration guards for trees the grammar cannot currently produce: they
    must raise a located error, never silently build a half-formed statement."""
    with pytest.raises(HydrationError, match=message):
        rule(node, None, hydrate)


def test_malformed_config_node_fails_loudly():
    node = _node(
        SyntaxNodeKind.VALIDATE_STATEMENT,
        [
            _natural_node(),
            _node(SyntaxNodeKind.SELECT_STATEMENT),
            _node(SyntaxNodeKind.VALIDATE_QUERY_CONFIG),
        ],
    )

    def hydrate(child):
        if child.kind is SyntaxNodeKind.NATURAL_SELECT_STATEMENT:
            return NaturalSelectStatement(question="q")
        if child.kind is SyntaxNodeKind.SELECT_STATEMENT:
            return SelectStatement(selection=[])
        return "not-a-dict"

    with pytest.raises(HydrationError, match="Validation config failed to hydrate"):
        validate_statement(node, None, hydrate)


def test_processed_statements_compile_to_inert_sql():
    """Neither statement has a static SQL form; both compile to a self-describing
    `select 1` so `trilogy render`-style surfaces stay runnable."""
    executor = Dialects.DUCK_DB.default_executor()
    parsed = executor.parse_text(
        _MODEL + "select natural 'how many orders?';\n"
        "validate b select natural 'q' matches ( select max(amount) -> m );"
    )
    compiled = {
        type(s).__name__: executor.generator.compile_statement(s)
        for s in parsed
        if isinstance(
            s, (ProcessedNaturalSelectStatement, ProcessedValidateNaturalStatement)
        )
    }
    assert (
        "natural selects are answered by an agent"
        in compiled["ProcessedNaturalSelectStatement"]
    )
    assert "--include-type agent" in compiled["ProcessedValidateNaturalStatement"]
    assert all("select 1" in sql for sql in compiled.values())


def test_expected_select_reaches_hooks():
    seen = []

    class _Hook(BaseHook):
        def process_select_info(self, select):
            seen.append(select)

    executor = Dialects.DUCK_DB.default_executor(hooks=[_Hook()])
    executor.parse_text(
        _MODEL + "validate b select natural 'q' matches ( select max(amount) -> m );"
    )
    assert seen


def test_author_statements_execute_via_dispatch(monkeypatch):
    """`execute_query` on the un-processed author statements routes through
    generation to the processed handlers."""
    asked = []
    monkeypatch.setattr(
        "trilogy.scripts.validate_agent.execute_natural_select",
        lambda exec_, question: asked.append(question),
    )
    env = Environment()
    _, parsed = parse_text(
        _MODEL + "validate b select natural 'q' matches ( select max(amount) -> m );\n"
        "select natural 'q2';",
        env,
    )
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    validate_natural = next(
        s for s in parsed if isinstance(s, ValidateNaturalStatement)
    )
    natural = next(s for s in parsed if isinstance(s, NaturalSelectStatement))
    assert "skipped" in executor.execute_query(validate_natural).fetchall()[0]["status"]
    executor.execute_query(natural)
    assert asked == ["q2"]


def test_processed_natural_select_calls_the_agent(monkeypatch):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_MODEL)
    asked = []

    def fake(exec_, question):
        asked.append(question)
        return exec_.execute_raw_sql("select 1 as answer")

    monkeypatch.setattr("trilogy.scripts.validate_agent.execute_natural_select", fake)
    result = executor.execute_query(
        ProcessedNaturalSelectStatement(question="how many orders?")
    )
    assert asked == ["how many orders?"]
    assert result.fetchall()


def test_render_round_trip():
    env = Environment()
    src = (
        _MODEL + "validate biggest select natural 'largest order?' matches (\n"
        "select max(amount) -> m\n"
        ") with (repetitions = 2, target = 0.5, comparison = ordered, "
        "tags = ['smoke'], timeout = 30);\n"
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
    assert validates[0].comparison is QueryComparison.ORDERED
    assert validates[0].tags == ["smoke"]
    assert validates[0].timeout == 30
    assert naturals[0].question == "count?"


def test_render_omits_default_options():
    env = Environment()
    _, parsed = parse_text(
        _MODEL + "validate select natural 'q' matches ( select amount );", env
    )
    rendered = Renderer().to_string(parsed[-1])
    assert " with (" not in rendered
    assert rendered.startswith("validate select natural 'q'")
