"""Filter pushdown into script datasources.

Pushdown is a hint: the rendered SQL keeps its own WHERE, so the contract these
tests enforce is that turning it on never changes an answer. Every case runs the
same query with pushdown on and off and requires identical rows -- shape asserts
alone would only prove the feature fired, not that it was right.
"""

import shlex
from datetime import date, datetime
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Ordering
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildOrderBy,
    BuildOrderItem,
    BuildParenthetical,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.source_pushdown import (
    _is_transport_safe,
    _literal,
    _operator_spelling,
    render_args,
    serialize_condition,
    serialize_order_by,
)
from trilogy.io.contract import Filter, Sort, SourceRequest

SCRIPT = Path(__file__).parent / "pushdown_source.py"

MODEL = """
key id int;
property id.state string;
property id.label string;
property id.score float;

datasource src (
    id: id,
    state: state,
    label: label,
    score: score
)
grain (id)
file `./pushdown_source.py`;
"""


# --- unit -------------------------------------------------------------------


def test_only_safe_operators_are_pushable():
    assert _operator_spelling(ComparisonOperator.EQ) == "="
    assert _operator_spelling(ComparisonOperator.GTE) == ">="
    for rejected in (
        ComparisonOperator.LIKE,
        ComparisonOperator.IN,
        ComparisonOperator.NOT_IN,
        ComparisonOperator.IS,
        ComparisonOperator.ILIKE,
        ComparisonOperator.CONTAINS,
    ):
        assert _operator_spelling(rejected) is None


def test_transport_safety_rejects_anything_needing_escaping():
    assert _is_transport_safe("CA")
    assert _is_transport_safe("2026-01-01")
    assert _is_transport_safe("-3.5")
    for unsafe in ("new york", "O'Brien", 'say "hi"', "a|b", "a;b", "$HOME", "a b", ""):
        assert not _is_transport_safe(unsafe), unsafe


def test_literal_rendering():
    assert _literal(True) == "true"
    assert _literal(3) == "3"
    assert _literal("CA") == "CA"
    assert _literal(None) is None
    assert _literal([1, 2]) is None


def test_dates_render_as_iso_8601():
    assert _literal(date(2026, 1, 31)) == "2026-01-31"
    assert _literal(datetime(2026, 1, 31, 9, 30)) == "2026-01-31T09:30:00"


def test_bool_is_not_rendered_as_an_int():
    """bool is a subclass of int; the bool branch has to come first."""
    assert _literal(False) == "false"


def test_render_args_produces_one_shlex_token_per_filter():
    args = render_args(
        SourceRequest(
            filters=(Filter("state", "=", "CA"), Filter("id", ">", 10)), limit=5
        )
    )
    assert args == "--filter 'state=CA' --filter 'id>10' --limit 5"
    assert shlex.split(args) == [
        "--filter",
        "state=CA",
        "--filter",
        "id>10",
        "--limit",
        "5",
    ]


def test_render_args_round_trips_through_the_script_parser():
    """What the planner emits must be what the script's parser accepts."""
    args = render_args(SourceRequest(filters=(Filter("state", "=", "CA"),)))
    assert Filter.parse(shlex.split(args)[1]) == Filter("state", "=", "CA")


def test_render_args_empty_for_no_request():
    assert render_args(None) == ""


def test_render_args_drops_a_filter_whose_value_cannot_be_spelled():
    request = SourceRequest(
        filters=(Filter("tags", "=", [1, 2]), Filter("state", "=", "CA"))
    )
    assert render_args(request) == "--filter 'state=CA'"


# --- serialization ----------------------------------------------------------
# `serialize_condition` and `serialize_order_by` decline far more often than
# they accept, and every decline is what keeps a limit from travelling with an
# incomplete predicate. These drive the refusals directly; the integration
# cases below prove the accepted ones stay correct end to end.

SERIALIZATION_MODEL = """
key id int;
property id.state string;
property id.absent string;

datasource narrow (
    id: id,
    state: state
)
grain (id)
address narrow_tbl;
"""


@pytest.fixture(scope="module")
def build():
    environment = Environment()
    environment.parse(SERIALIZATION_MODEL)
    return environment.materialize_for_select()


@pytest.fixture(scope="module")
def datasource(build):
    return build.datasources["narrow"]


@pytest.fixture(scope="module")
def state(build):
    return build.concepts["state"]


def equals(left, right):
    return BuildComparison(left=left, right=right, operator=ComparisonOperator.EQ)


def ordering(expr, order):
    return BuildOrderBy(items=[BuildOrderItem(expr=expr, order=order)])


def test_no_condition_serializes_completely(datasource):
    assert serialize_condition(None, datasource) == ((), True)


def test_a_parenthesized_condition_is_unwrapped(datasource, state):
    condition = BuildParenthetical(content=equals(state, "CA"))
    assert serialize_condition(condition, datasource) == (
        (Filter("state", "=", "CA"),),
        True,
    )


def test_an_or_is_not_a_conjunction(datasource, state):
    condition = BuildConditional(
        left=equals(state, "CA"),
        right=equals(state, "NY"),
        operator=BooleanOperator.OR,
    )
    assert serialize_condition(condition, datasource) == ((), False)


def test_an_or_nested_inside_an_and_disqualifies_the_whole_condition(datasource, state):
    """Serializing only the AND side would silently widen the predicate."""
    nested = BuildConditional(
        left=equals(state, "CA"),
        right=equals(state, "NY"),
        operator=BooleanOperator.OR,
    )
    condition = BuildConditional(
        left=equals(state, "CA"), right=nested, operator=BooleanOperator.AND
    )
    assert serialize_condition(condition, datasource) == ((), False)


def test_a_mirrored_comparison_is_declined(datasource, state):
    """`'CA' = state` would need the operator flipped to travel."""
    assert serialize_condition(equals("CA", state), datasource) == ((), False)


def test_a_comparison_between_two_columns_is_declined(datasource, state, build):
    condition = equals(state, build.concepts["id"])
    assert serialize_condition(condition, datasource) == ((), False)


def test_a_concept_the_datasource_does_not_carry_is_declined(datasource, build):
    condition = equals(build.concepts["absent"], "x")
    assert serialize_condition(condition, datasource) == ((), False)


def test_an_incomplete_conjunct_leaves_the_rest_pushable(datasource, state, build):
    condition = BuildConditional(
        left=equals(state, "CA"),
        right=equals(build.concepts["absent"], "x"),
        operator=BooleanOperator.AND,
    )
    filters, complete = serialize_condition(condition, datasource)
    assert filters == (Filter("state", "=", "CA"),)
    assert complete is False


def test_no_ordering_serializes_completely(datasource):
    assert serialize_order_by(None, datasource) == ((), True)


def test_plain_directions_travel(datasource, state):
    assert serialize_order_by(ordering(state, Ordering.ASCENDING), datasource) == (
        (Sort("state", False),),
        True,
    )
    assert serialize_order_by(ordering(state, Ordering.DESCENDING), datasource) == (
        (Sort("state", True),),
        True,
    )


@pytest.mark.parametrize(
    "order",
    [
        Ordering.ASC_NULLS_FIRST,
        Ordering.ASC_NULLS_LAST,
        Ordering.ASC_NULLS_AUTO,
        Ordering.DESC_NULLS_FIRST,
        Ordering.DESC_NULLS_LAST,
        Ordering.DESC_NULLS_AUTO,
    ],
    ids=lambda o: o.name,
)
def test_null_placement_variants_do_not_travel(datasource, state, order):
    """They change which rows a LIMIT keeps, and pyarrow places nulls its own way."""
    assert serialize_order_by(ordering(state, order), datasource) == ((), False)


def test_an_ordering_on_an_expression_does_not_travel(datasource):
    assert serialize_order_by(ordering(5, Ordering.ASCENDING), datasource) == (
        (),
        False,
    )


def test_an_ordering_on_an_absent_concept_does_not_travel(datasource, build):
    order_by = ordering(build.concepts["absent"], Ordering.ASCENDING)
    assert serialize_order_by(order_by, datasource) == ((), False)


def test_one_untravellable_key_grounds_the_whole_ordering(datasource, state, build):
    """A partial sort is a different ordering, not a weaker one."""
    order_by = BuildOrderBy(
        items=[
            BuildOrderItem(expr=state, order=Ordering.ASCENDING),
            BuildOrderItem(expr=build.concepts["absent"], order=Ordering.ASCENDING),
        ]
    )
    assert serialize_order_by(order_by, datasource) == ((), False)


def test_render_args_survives_a_posix_shell():
    """The shellfs pipe is a shell command line, so `<` and `>` are redirects.

    shlex alone does not model that layer: it was green while the DuckDB
    transport truncated `--filter id>30` to `id` and wrote a file named `30`.
    """
    import shutil
    import subprocess

    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("no POSIX shell")
    args = render_args(
        SourceRequest(filters=(Filter("id", ">", 30), Filter("state", "=", "CA")))
    )
    printed = subprocess.run(
        [sh, "-c", f"for a in {args}; do echo $a; done"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert printed.stdout.split() == ["--filter", "id>30", "--filter", "state=CA"]


# --- integration ------------------------------------------------------------


@pytest.fixture(scope="module")
def workspace() -> Path:
    """The script's own directory -- it is not copied to a tmp dir.

    ``pushdown_source.py`` resolves pytrilogy through a *relative*
    ``[tool.uv.sources]`` path, which only points at this checkout from where
    the file actually lives.
    """
    return SCRIPT.parent


@pytest.fixture(scope="module")
def executor(workspace: Path):
    """One executor for the module, with ``MODEL`` already applied.

    Building an executor installs the duckdb extensions and recreates the
    ``uv_run`` macro -- ~0.37s that used to be paid on every ``sql_for`` and on
    both sides of every pushdown comparison. Declaring the model once and
    reusing the connection halves this file's runtime and removes the same
    number of script spawns, with no loss of coverage: pushdown is chosen when
    the query is generated, not when the executor is built, so one connection
    serves both sides.
    """
    built = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workspace),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    built.execute_text(MODEL)
    return built


def sql_for(executor, query: str) -> str:
    return "\n".join(executor.generate_sql(query))


def rows_for(executor, query: str, pushdown: bool) -> list[tuple]:
    original = BaseDialect.source_pushdown
    if not pushdown:
        BaseDialect.source_pushdown = lambda self, cte, address: None
    try:
        result = executor.execute_text(query)
        cursor = result[-1] if isinstance(result, list) else result
        return [tuple(row) for row in cursor.fetchall()]
    finally:
        BaseDialect.source_pushdown = original


def pushed_args(sql: str) -> str:
    """The command line inside ``args := '...'``, with SQL quote-doubling and
    shell quoting undone, so expectations read as the script's own argv."""
    literal = sql.split("args := '")[1]
    decoded: list[str] = []
    i = 0
    while i < len(literal):
        if literal[i] == "'":
            if literal[i + 1 : i + 2] != "'":
                break
            i += 1
        decoded.append(literal[i])
        i += 1
    return " ".join(shlex.split("".join(decoded)))


CASES = [
    # A limit only travels with the ordering that decides which rows it keeps.
    ("select id order by id desc limit 5;", "--order-by id:desc --limit 5"),
    (
        "where state = 'CA' select id, state order by id asc limit 3;",
        "--filter state=CA --order-by id:asc --limit 3",
    ),
    # Grouping: N groups is not N input rows. Ordered by the group key rather
    # than the count, which ties across all four states and would make the
    # comparison non-deterministic on its own.
    ("select state, count(id) -> c order by state asc limit 2;", None),
    # Condition does not serialize completely, so the limit stays home.
    ("where label = 'north side' select id order by id desc limit 5;", None),
    ("where state = 'CA' select id, state order by id asc;", "--filter state=CA"),
    ("where id > 30 select id order by id asc;", "--filter id>30"),
    (
        "where state = 'CA' and id > 10 select id, state order by id asc;",
        "--filter state=CA --filter id>10",
    ),
    # Not pushable, but must still be correct.
    ("where state = 'CA' or id > 30 select id, state order by id asc;", None),
    ("where label = 'north side' select id, label order by id asc;", None),
    ("where state like 'C%' select id, state order by id asc;", None),
    ("select id, state order by id asc;", None),
    ("where score >= 30.0 select id, score order by id asc;", "--filter score>=30.0"),
]


@pytest.mark.parametrize("query,expected_arg", CASES, ids=lambda v: str(v)[:40])
def test_pushdown_never_changes_the_answer(executor, query, expected_arg):
    on = rows_for(executor, query, pushdown=True)
    off = rows_for(executor, query, pushdown=False)
    assert on == off
    assert on, "query returned no rows; it would not discriminate"


@pytest.mark.parametrize("query,expected_arg", CASES, ids=lambda v: str(v)[:40])
def test_pushdown_fires_exactly_where_expected(executor, query, expected_arg):
    sql = sql_for(executor, query)
    if expected_arg is None:
        assert "args :=" not in sql, sql
    else:
        assert expected_arg in pushed_args(sql), sql


def test_a_limit_never_travels_without_its_ordering(executor):
    """The bug this guards: pushing `--limit N` alone under an ORDER BY makes
    the source truncate an arbitrary N rows instead of the top N."""
    sql = sql_for(executor, "select id order by id desc limit 5;")
    args = pushed_args(sql)
    assert "--limit" in args
    assert args.index("--order-by") < args.index("--limit"), args


def test_render_args_pairs_limit_with_order_by():
    request = SourceRequest(order_by=(Sort("id", descending=True),), limit=5)
    assert render_args(request) == "--order-by id:desc --limit 5"


def test_ordered_limit_returns_the_top_rows_not_arbitrary_ones(executor):
    rows = rows_for(executor, "select id order by id desc limit 5;", pushdown=True)
    assert [row[0] for row in rows] == [39, 38, 37, 36, 35]


def test_a_value_needing_quoting_is_not_pushed_but_still_filters(executor):
    """'north side' has a space, so it cannot survive the shell transport."""
    query = "where label = 'north side' select id, label order by id asc;"
    assert "args :=" not in sql_for(executor, query)
    rows = rows_for(executor, query, pushdown=True)
    assert rows == rows_for(executor, query, pushdown=False)
    assert {row[1] for row in rows} == {"north side"}


def test_partial_pushdown_leaves_the_rest_to_sql(executor):
    """One conjunct is pushable and one is not; the answer is still exact."""
    query = (
        "where state = 'CA' and label = 'north side' "
        "select id, state, label order by id asc;"
    )
    sql = sql_for(executor, query)
    assert "--filter state=CA" in pushed_args(sql)
    assert "label" not in pushed_args(sql)
    rows = rows_for(executor, query, pushdown=True)
    assert rows == rows_for(executor, query, pushdown=False)
    assert all(row[1] == "CA" and row[2] == "north side" for row in rows)
