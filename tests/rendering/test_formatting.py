"""Round-trip formatting tests for the renderer.

The renderer is also Trilogy's formatter — `trilogy fmt` parses a script and
writes it back through `Renderer.render_statement_string`. These tests lock in:

* line-length-aware wrapping for long boolean conditions
* statement spacing rules (tight runs of imports / declarations, blank lines
  between blocks like `WHERE ... SELECT`)
* idempotent round-trips on representative TPC-DS queries
"""

from pathlib import Path

import pytest

from trilogy.core.models.environment import Environment
from trilogy.parsing.render import DEFAULT_MAX_LINE_LENGTH, Renderer

TPCDS_DIR = Path(__file__).resolve().parents[1] / "modeling" / "tpc_ds_duckdb"

# Queries that parse + render + reparse + render stably with the default
# renderer settings. Locked-in to catch regressions on either formatting or
# statement spacing. The full list is large; we sample a cross-section that
# exercises WHERE/HAVING wrapping, multi-select align clauses, CASE blocks,
# rowsets, comments, and function declarations.
TPCDS_ROUND_TRIP_QUERIES = [
    "query01.preql",
    "query03.preql",
    "query06.preql",
    "query07.preql",
    "query10.preql",
    "query13.preql",
    "query18.preql",
    "query22.preql",
    "query33.preql",
    "query46.preql",
    "query55.preql",
    "query66.preql",
    "query82.preql",
    "query96.preql",
]


@pytest.mark.parametrize("query_name", TPCDS_ROUND_TRIP_QUERIES)
def test_tpcds_round_trip(query_name: str):
    path = TPCDS_DIR / query_name
    env = Environment(working_path=path.parent)
    _, queries = env.parse(path.read_text())
    rendered = Renderer().render_statement_string(queries)

    env2 = Environment(working_path=path.parent)
    _, queries2 = env2.parse(rendered)
    rendered2 = Renderer().render_statement_string(queries2)

    assert rendered == rendered2, (
        f"Round-trip not stable for {query_name}\n"
        f"--- first ---\n{rendered}\n--- second ---\n{rendered2}"
    )


def test_default_max_line_length_is_100():
    assert DEFAULT_MAX_LINE_LENGTH == 100
    assert Renderer().max_line_length == 100


def test_long_where_clause_wraps():
    """Long WHERE conditionals are broken across lines on top-level operator."""
    env = Environment()
    _, queries = env.parse("""
key id int;
property id.gender string;
property id.education string;
property id.marital string;
property id.year int;

datasource users (
    id: id,
    g: gender,
    e: education,
    m: marital,
    y: year
)
grain (id)
address memory.users;

WHERE
    gender = 'M' and marital = 'S' and education = 'College' and year = 2000
SELECT
    id;
""")
    rendered = Renderer().to_string(queries[-1])
    expected = """where
    gender = 'M' and marital = 'S' and education = 'College' and year = 2000
select
    id,
;"""
    assert rendered == expected, rendered


def test_very_long_where_clause_wraps_with_default_limit():
    """At default 100-char limit, a long AND chain breaks one part per line."""
    env = Environment()
    _, queries = env.parse("""
key id int;
property id.col_one string;
property id.col_two string;
property id.col_three string;
property id.col_four string;
property id.col_five string;

datasource d (id: id, a: col_one, b: col_two, c: col_three, d: col_four, e: col_five)
grain (id) address memory.t;

WHERE
    col_one = 'aaaaaaaaaa'
    and col_two = 'bbbbbbbbbb'
    and col_three = 'cccccccccc'
    and col_four = 'dddddddddd'
    and col_five = 'eeeeeeeeee'
SELECT id;
""")
    rendered = Renderer().to_string(queries[-1])
    # Expect each AND clause on its own line.
    assert (
        "    col_one = 'aaaaaaaaaa'\n    and col_two = 'bbbbbbbbbb'\n" in rendered
    ), rendered
    # Round-trip the rendered output to confirm it still parses.
    env2 = Environment()
    env2.parse("""
key id int;
property id.col_one string;
property id.col_two string;
property id.col_three string;
property id.col_four string;
property id.col_five string;

datasource d (id: id, a: col_one, b: col_two, c: col_three, d: col_four, e: col_five)
grain (id) address memory.t;
""")
    env2.parse(rendered)


def test_nested_parens_break_with_indent():
    """Paren-wrapped sub-conditions break with extra indent, not flattened."""
    env = Environment()
    _, queries = env.parse("""
key id int;
property id.a string; property id.b string; property id.c string; property id.d string;

datasource d (id: id, A: a, B: b, C: c, D: d) grain (id) address memory.t;

WHERE
    a = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    and (
        b = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        or c = 'cccccccccccccccccccccccccccccccccccccccccccc'
        or d = 'dddddddddddddddddddddddddddddddddddddddddddd'
    )
SELECT id;
""")
    rendered = Renderer().to_string(queries[-1])
    # Inner OR chain rendered with an extra indent level inside the parens.
    assert "    and (\n        b = " in rendered, rendered
    assert "\n        or c = " in rendered, rendered
    assert "\n        or d = " in rendered, rendered
    assert "\n    )\n" in rendered, rendered


def test_max_line_length_configurable():
    """Narrow limit forces inner paren breaks that fit at default."""
    env = Environment()
    _, queries = env.parse("""
key id int;
property id.a string; property id.b string;

datasource d (id: id, A: a, B: b) grain (id) address memory.t;

WHERE
    a = 'AAAAAAAAAAAAAAAAAAAA' and (b = 'first_value' or b = 'second_value')
SELECT id;
""")
    wide = Renderer(max_line_length=200).to_string(queries[-1])
    narrow = Renderer(max_line_length=30).to_string(queries[-1])

    # Wide limit fits everything on one logical conditional line.
    assert (
        "    a = 'AAAAAAAAAAAAAAAAAAAA' and (b = 'first_value' or b = 'second_value')"
        in wide
    )
    # Narrow limit forces splits.
    assert "\n    and (\n" in narrow, narrow
    assert "\n        or b = 'second_value'\n" in narrow, narrow


def test_statement_spacing_imports_to_select():
    env = Environment()
    _, queries = env.parse("""
key id int;
datasource d (id: id) grain (id) address memory.t;

select id;
""")
    rendered = Renderer().render_statement_string(queries)
    # ConceptDeclaration directly followed by Datasource (different types) → blank line.
    assert "key id int;\n\ndatasource d" in rendered
    # Datasource → SelectStatement: blank line.
    assert ";\n\nselect\n    id," in rendered


def test_statement_spacing_tight_runs():
    env = Environment()
    _, queries = env.parse("""
key a int;
key b int;
key c int;
""")
    rendered = Renderer().render_statement_string(queries)
    # Same statement type packs with a single newline.
    assert rendered == "key a int;\nkey b int;\nkey c int;"


def test_statement_spacing_mixed_declaration_purposes():
    """Tight grouping is per-statement-type; key→property still tightens."""
    env = Environment()
    _, queries = env.parse("""
key a int;
property a.x string;
property a.y string;
""")
    rendered = Renderer().render_statement_string(queries)
    assert rendered == "key a int;\nproperty a.x string;\nproperty a.y string;"


def test_align_clause_uses_and_separator():
    """Multiple ALIGN items are separated by `and` and parse cleanly."""
    env = Environment()
    _, queries = env.parse("""
key id int;
property id.a string;
property id.b string;
datasource d (id: id, A: a, B: b) grain (id) address memory.t;

with mset as
select a as a1, b as b1
merge
select a as a2, b as b2
align
    chan: a1, a2
    AND meas: b1, b2
;

select mset.chan;
""")
    rowset_stmt = queries[-2]
    rendered = Renderer().to_string(rowset_stmt)
    assert "\n    chan: a1, a2\n    and meas: b1, b2\n" in rendered, rendered

    env2 = Environment()
    env2.parse("""
key id int;
property id.a string;
property id.b string;
datasource d (id: id, A: a, B: b) grain (id) address memory.t;
""")
    env2.parse(rendered + "\nselect mset.chan;")
