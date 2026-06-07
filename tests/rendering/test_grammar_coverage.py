"""Targeted renderer coverage tests.

Each test maps to a specific render branch — Trilogy grammar feature that
the parser accepts but the existing tests don't yet exercise on the render
side. Use parse → render → reparse round-trips where possible; fall back
to direct ``Renderer().to_string(...)`` only when the AST node can't be
produced from a tiny source snippet.
"""

from datetime import date
from pathlib import Path

import pytest

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    DatasourceState,
    DatePart,
    FunctionType,
    Modifier,
    Purpose,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    Concept,
    ConceptRef,
    Function,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ImportStatement,
    MergeStatementV2,
    SelectItem,
)
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.render import Renderer

# ---------------------------------------------------------------------------
# Concept declaration variants
# ---------------------------------------------------------------------------


def test_render_unique_property():
    """``unique property`` must keep the space — ``unique_property`` is not a
    grammar token."""
    env = Environment()
    env.parse("key id int;\nunique property id.text_id string;")
    rendered = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["text_id"])
    )
    assert rendered == "unique property id.text_id string;", rendered

    env2 = Environment()
    env2.parse(f"key id int;\n{rendered}")
    assert env2.concepts["text_id"].purpose == Purpose.UNIQUE_PROPERTY


def test_render_concept_with_description():
    """A trailing ``#description`` rides through render."""
    env = Environment()
    env.parse("key id int; #the row id\n")
    rendered = Renderer().to_string(
        ConceptDeclarationStatement(concept=env.concepts["id"])
    )
    assert rendered == "key id int; #the row id", rendered


def test_render_concept_derivation_statement():
    """``ConceptDerivationStatement`` re-routes through the declaration render."""
    env = Environment()
    _, parsed = env.parse("key x int;\nauto y <- x + 1;")
    decl = parsed[-1]
    assert isinstance(decl, (ConceptDeclarationStatement, ConceptDerivationStatement))
    rendered = Renderer().to_string(decl)
    assert "auto y <-" in rendered


# ---------------------------------------------------------------------------
# Aggregate grouping variants
# ---------------------------------------------------------------------------


def test_render_aggregate_grouping_sets():
    """``sum(x) by grouping sets (a, b), (a), ()`` — exercises grouping_sets render."""
    env = Environment()
    _, parsed = env.parse("""
key a int;
key b int;
key x int;
select sum(x) by grouping sets (a, b), (a), () as gx;
""")
    rendered = Renderer().to_string(parsed[-1])
    assert "by grouping sets" in rendered, rendered
    assert "(a, b)" in rendered, rendered
    assert "(a)" in rendered, rendered


def test_render_aggregate_by_cube():
    """``sum(x) by cube a, b`` — exercises the cube branch."""
    env = Environment()
    _, parsed = env.parse("""
key a int;
key b int;
key x int;
select sum(x) by cube a, b as gx;
""")
    rendered = Renderer().to_string(parsed[-1])
    assert "by cube" in rendered, rendered


def test_render_aggregate_wrapper_grouping_sets_direct():
    """Direct AggregateWrapper render for grouping_sets without parse."""
    from trilogy.core.enums import AggregateGroupingMode

    a = Concept(name="a", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    b = Concept(name="b", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    x = Concept(name="x", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    agg = AggregateWrapper(
        function=Function(
            arguments=[x],
            operator=FunctionType.SUM,
            output_purpose=Purpose.METRIC,
            output_datatype=DataType.INTEGER,
            arg_count=1,
        ),
        by=[a, b],
        grouping=AggregateGroupingMode.GROUPING_SETS,
        grouping_sets=[[a], [b]],
    )
    rendered = Renderer().to_string(agg)
    assert "by grouping sets" in rendered, rendered
    assert "(a)" in rendered, rendered
    assert "(b)" in rendered, rendered


# ---------------------------------------------------------------------------
# Datasource modifiers
# ---------------------------------------------------------------------------


def test_render_root_datasource():
    """``root datasource`` keyword survives render."""
    env = Environment()
    env.parse("""
key id int;
root datasource users (
    id,
)
grain (id)
address users_table;
""")
    rendered = Renderer().to_string(env.datasources["users"])
    assert rendered.startswith("root datasource"), rendered


def test_render_partial_datasource():
    """``partial datasource`` keyword survives render; column-level ``~`` only
    re-emits for columns the source explicitly marked."""
    env = Environment()
    env.parse("""
key id int;
property id.x int;
partial datasource users (
    id,
    x,
)
grain (id)
address users_table;
""")
    rendered = Renderer().to_string(env.datasources["users"])
    assert "partial datasource" in rendered, rendered
    # No spurious ~ on the columns — the implicit PARTIAL came from the keyword.
    assert "~id" not in rendered
    assert "~x" not in rendered


def test_render_freshness_refresh():
    """``freshness by `path``` and ``refresh `path``` round-trip.

    Paths are OS-normalized, so we check for the keyword + filename tail.
    """
    src = """key id int;

root datasource events (
    id,
)
grain (id)
address events_tbl
freshness by `/fake/probe.py`
refresh `/fake/refresh.py`;
"""
    env = Environment()
    env.parse(src)
    rendered = Renderer().to_string(env.datasources["events"])
    assert "freshness by `" in rendered, rendered
    assert "probe.py" in rendered, rendered
    assert "refresh `" in rendered, rendered
    assert "refresh.py" in rendered, rendered


def test_render_freshness_by_concept():
    """``freshness by <concept>`` uses the concept-list form."""
    from trilogy.core.models.author import Grain

    user_id = Concept(name="user_id", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    updated_at = Concept(
        name="updated_at",
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        namespace="local",
    )
    ds = Datasource(
        name="users",
        columns=[
            ColumnAssignment(alias="user_id", concept=user_id),
            ColumnAssignment(alias="updated_at", concept=updated_at),
        ],
        address="users_tbl",
        grain=Grain(components=[user_id]),
        freshness_by=[ConceptRef(address="local.updated_at")],
    )
    rendered = Renderer().to_string(ds)
    assert "freshness by updated_at" in rendered, rendered


# ---------------------------------------------------------------------------
# Rowsets
# ---------------------------------------------------------------------------


def test_render_rowset_unmangles_inner_name():
    """Rowset-scoped concepts carry a ``_{rowset}_`` prefix; render must strip
    it so reparse doesn't double-prefix."""
    env = Environment()
    env.parse("""key x int;
rowset r <- select x+1 -> y;
""")
    # The locally materialized rowset concept is `local._r_y`.
    inside = env.concepts["local._r_y"]
    r = Renderer()
    with r._rowset_scope("r"):
        unmangled = r.to_string(inside)
    assert unmangled.endswith("y") and "_r_" not in unmangled, unmangled


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------


def test_render_self_import():
    """``self import as X`` round-trips."""
    out = Renderer().to_string(
        ImportStatement(
            alias="me",
            path=str(Path("self")),
            input_path="self",
            is_self=True,
        )
    )
    assert out == "self import as me;", out


# ---------------------------------------------------------------------------
# SelectItem modifiers
# ---------------------------------------------------------------------------


def test_render_partial_select_item():
    user_id = Concept(name="user_id", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    rendered = Renderer().to_string(
        SelectItem(content=user_id, modifiers=[Modifier.PARTIAL])
    )
    assert rendered == "~user_id", rendered


def test_render_hidden_and_partial_select_item():
    user_id = Concept(name="user_id", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    rendered = Renderer().to_string(
        SelectItem(content=user_id, modifiers=[Modifier.HIDDEN, Modifier.PARTIAL])
    )
    assert rendered == "--~user_id", rendered


# ---------------------------------------------------------------------------
# Atomic / leaf renderers
# ---------------------------------------------------------------------------


def test_render_magic_constant_null():
    assert Renderer().to_string(MagicConstants.NULL) == "null"


def test_render_date_part_enum():
    """DatePart enums render as their lowercase string value."""
    assert Renderer().to_string(DatePart.YEAR) == "year"
    assert Renderer().to_string(DatePart.DAY_OF_WEEK) == "day_of_week"


def test_render_modifier_nullable():
    assert Renderer().to_string(Modifier.NULLABLE) == "?"


def test_render_empty_list():
    assert Renderer().to_string([]) == "[]"


def test_render_bool():
    assert Renderer().to_string(True) == "True"
    assert Renderer().to_string(False) == "False"


def test_render_date_literal():
    out = Renderer().to_string(date(2024, 1, 2))
    assert out == "'2024-01-02'::date"


# ---------------------------------------------------------------------------
# Address (query + parens)
# ---------------------------------------------------------------------------


def test_render_query_address_strips_outer_parens():
    """``query '''(...)'''`` collapses to ``query '''...'''`` on render."""
    from trilogy.core.enums import AddressType

    addr = Address(location="(SELECT 1)", type=AddressType.QUERY)
    assert Renderer().to_string(addr) == "query '''SELECT 1'''"


# ---------------------------------------------------------------------------
# Merge wildcard form
# ---------------------------------------------------------------------------


def test_render_merge_wildcard():
    """``merge a.* into b.*`` exercises the wildcard branch."""
    out = Renderer().to_string(
        MergeStatementV2(
            sources=[
                Concept(name="x", purpose=Purpose.KEY, datatype=DataType.INTEGER),
                Concept(name="y", purpose=Purpose.KEY, datatype=DataType.INTEGER),
            ],
            targets={},
            source_wildcard="ns_a",
            target_wildcard="ns_b",
            modifiers=[Modifier.PARTIAL],
        )
    )
    assert out == "merge ns_a.* into ~ns_b.*;", out


# ---------------------------------------------------------------------------
# Multi-select with derive clause
# ---------------------------------------------------------------------------


def test_render_multi_select_with_derive_round_trip():
    """``align ... derive ...`` exercises DeriveClause + DeriveItem render."""
    env = Environment()
    src = """key a int;
key b int;
select a
merge
select b
align c: a, b
derive c + 1 -> d
;"""
    _, queries = parse_text(src, env)
    rendered = Renderer().to_string(queries[-1])
    assert "derive" in rendered, rendered
    # `->` and `as` are interchangeable on input; the renderer canonicalizes to `as`.
    assert "as d" in rendered, rendered

    env2 = Environment()
    parse_text("key a int;\nkey b int;\n" + rendered, env2)


# ---------------------------------------------------------------------------
# Standalone comment + statement-separator rules
# ---------------------------------------------------------------------------


def test_render_statement_separator_blank_between_blocks():
    """A blank line sits between a concept declaration and a datasource."""
    src = """key id int;

datasource users (
    id,
)
grain (id)
address users_tbl;
"""
    env = Environment()
    _, queries = env.parse(src)
    rendered = Renderer().render_statement_string(queries)
    assert "\n\n" in rendered


# ---------------------------------------------------------------------------
# FilterItem with arbitrary content
# ---------------------------------------------------------------------------


def test_render_filter_item_non_bare_content():
    """A FilterItem whose content is not a plain identifier gets wrapped in
    parens on render so ``_filter_alt`` re-binds it correctly."""
    env = Environment()
    _, commands = env.parse("""
key x int;
key y int;
auto z <- (x + y) ? x > 0;
""")
    rendered = Renderer(environment=env).to_string(commands[-1])
    # The parenthesized form survives.
    assert "?" in rendered
    assert "(" in rendered


# ---------------------------------------------------------------------------
# Selective import (``from X import Y``)
# ---------------------------------------------------------------------------


def test_selective_import_parse_round_trip(tmp_path):
    """Selective-import statements parse; render emits the standard import
    form because the renderer doesn't yet have a dedicated selective branch."""
    (tmp_path / "lib.preql").write_text("key id int;\nkey name string;")
    env = Environment(working_path=tmp_path)
    env.parse("from lib import id, name;")
    # We only assert the parse succeeded; selective import has no dedicated
    # render path today, so re-emission goes through standard import render.
    assert "id" in env.concepts
    assert "name" in env.concepts


# ---------------------------------------------------------------------------
# Datasource state UNPUBLISHED
# ---------------------------------------------------------------------------


def test_render_datasource_unpublished():
    from trilogy.core.models.author import Grain

    user_id = Concept(name="user_id", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    ds = Datasource(
        name="users",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address="users_tbl",
        grain=Grain(components=[user_id]),
        status=DatasourceState.UNPUBLISHED,
    )
    rendered = Renderer().to_string(ds)
    assert "state unpublished" in rendered, rendered


# ---------------------------------------------------------------------------
# Re-parse for stateful guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fragment,needle",
    [
        ("unique property id.t string;", "unique property id.t string;"),
        ("rowset r <- select id;", "rowset r <-"),
    ],
)
def test_grammar_features_render_inverse(fragment, needle):
    """Pin: known grammar features all roundtrip through the renderer."""
    env = Environment()
    src = f"key id int;\n{fragment}"
    _, queries = env.parse(src)
    rendered = Renderer().to_string(queries[-1])
    assert needle in rendered, rendered

    env2 = Environment()
    env2.parse(f"key id int;\n{rendered}")


# ---------------------------------------------------------------------------
# ConceptRef render — virtual concept passthrough
# ---------------------------------------------------------------------------


def test_render_concept_ref_all_rows_sentinel():
    """``__preql_internal.all_rows`` renders as ``*``."""
    out = Renderer().to_string(ConceptRef(address="__preql_internal.all_rows"))
    assert out == "*"


# ---------------------------------------------------------------------------
# Function GROUP single-arg form
# ---------------------------------------------------------------------------


def test_render_function_group_single_arg():
    """``group(1)`` with no ``by`` clause renders without a trailing ``by``."""
    rendered = Renderer().to_string(
        Function(
            arguments=[1],
            operator=FunctionType.GROUP,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=1,
        )
    )
    assert rendered == "group(1)", rendered


def test_render_function_mod():
    """``a % b`` renders the MOD operator."""
    rendered = Renderer().to_string(
        Function(
            arguments=[10, 3],
            operator=FunctionType.MOD,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.INTEGER,
            arg_count=2,
        )
    )
    assert rendered == "10 % 3", rendered


def test_render_modifier_hidden():
    assert Renderer().to_string(Modifier.HIDDEN) == "--"


def test_render_modifier_partial():
    assert Renderer().to_string(Modifier.PARTIAL) == "~"


# ---------------------------------------------------------------------------
# Datasource with multi-line `where` clause
# ---------------------------------------------------------------------------


def test_render_datasource_multi_line_where():
    """A long datasource WHERE clause that wraps onto multiple lines stays
    properly indented under the ``where`` keyword."""
    from trilogy.core.enums import BooleanOperator, ComparisonOperator
    from trilogy.core.models.author import (
        Comparison,
        Conditional,
        Grain,
        WhereClause,
    )

    user_id = Concept(name="user_id", purpose=Purpose.KEY, datatype=DataType.INTEGER)
    very_long_cond = Conditional(
        left=Conditional(
            left=Comparison(left=user_id, right=123, operator=ComparisonOperator.EQ),
            right=Comparison(left=user_id, right=456, operator=ComparisonOperator.EQ),
            operator=BooleanOperator.AND,
        ),
        right=Conditional(
            left=Comparison(left=user_id, right=789, operator=ComparisonOperator.EQ),
            right=Comparison(left=user_id, right=999, operator=ComparisonOperator.EQ),
            operator=BooleanOperator.AND,
        ),
        operator=BooleanOperator.OR,
    )
    ds = Datasource(
        name="users",
        columns=[ColumnAssignment(alias="user_id", concept=user_id)],
        address="users_tbl",
        grain=Grain(components=[user_id]),
        where=WhereClause(conditional=very_long_cond),
    )
    # Force wrap by tightening the line budget.
    rendered = Renderer(max_line_length=30).to_string(ds)
    assert "where" in rendered
    # Multi-line where must be indented under the keyword, not flush left.
    lines = rendered.split("\n")
    where_idx = next(i for i, line in enumerate(lines) if line.startswith("where "))
    # Continuation lines after the first carry leading whitespace.
    continuation = [
        line
        for line in lines[where_idx + 1 :]
        if line.strip() and not line.endswith(";")
    ]
    if continuation:
        assert continuation[0].startswith("    "), rendered


# ---------------------------------------------------------------------------
# Function declaration with default-valued binding
# ---------------------------------------------------------------------------


def test_render_function_declaration_with_default():
    """An ArgBinding carrying a default value renders as ``name=default``."""
    env = Environment()
    _, commands = env.parse("""
def add_with_default(x, y=2) -> x + y;
""")
    rendered = Renderer().to_string(commands[0])
    assert "y=2" in rendered, rendered
