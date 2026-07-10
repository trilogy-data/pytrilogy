from trilogy import Dialects, parse
from trilogy.parsing.render import render_query

_FIXTURE = """
key id int;
property id.nm string;

datasource t (
    id: id,
    nm: nm,
)
grain (id)
query '''
select 1 as id, 'a' as nm union all
select 2 as id, cast(null as varchar) as nm union all
select 3 as id, '' as nm
''';
"""


def _run(query: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    return [tuple(r) for r in executor.execute_text(query)[0].fetchall()]


def test_concat_function_skips_nulls():
    results = _run("""
select id, concat(nm, '_suffix') as c
order by id asc;
""")
    assert results == [(1, "a_suffix"), (2, "_suffix"), (3, "_suffix")]


def test_concat_operator_propagates_nulls():
    results = _run("""
select id, nm || '_suffix' as c
order by id asc;
""")
    assert results == [(1, "a_suffix"), (2, None), (3, "_suffix")]


def test_concat_ws_skips_nulls_keeps_empty():
    # a NULL argument drops its separator too; an empty string keeps it
    results = _run("""
select id, concat_ws('-', nm, 'x', 'y') as c
order by id asc;
""")
    assert results == [(1, "a-x-y"), (2, "x-y"), (3, "-x-y")]


def test_concat_ws_all_null():
    results = _run("""
select id, concat_ws('-', nm, nm) as c
order by id asc;
""")
    assert results == [(1, "a-a"), (2, ""), (3, "-")]


def test_concat_forms_round_trip_through_render():
    text = _FIXTURE + """
select
    id,
    nm || '_s' as strict_c,
    concat(nm, '_s') as fn_c,
    concat_ws('-', nm, '_s') as ws_c
order by id asc;
"""
    env, parsed = parse(text)
    rendered = render_query(parsed[-1])
    assert "|| '_s'" in rendered
    assert "concat(" in rendered
    assert "concat_ws('-'" in rendered
    reparse_env, _ = parse(_FIXTURE + rendered)
    assert reparse_env.concepts["strict_c"].lineage is not None
