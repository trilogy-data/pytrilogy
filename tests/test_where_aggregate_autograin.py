from trilogy import Dialects, Environment

# A bare aggregate (no explicit `by`) in a top-level WHERE auto-grains to the
# SELECT grain (like HAVING) instead of collapsing to a global scalar no-op.
# `by *` remains the escape hatch for a genuine global comparison.
# Rows (rid, item, ch): item 100 has a STORE row, item 200 does not, item 300 does.
MODEL = """
key rid int;
property rid.item int;
property rid.ch string;
datasource rows (rid: rid, item: item, ch: ch)
grain (rid)
query '''
select 1 rid, 100 item, 'S' ch
union all select 2, 100, 'W'
union all select 3, 200, 'W'
union all select 4, 300, 'S'
''';
auto has_store_bare <- count(rid ? ch = 'S') > 0;
auto has_store_global <- count(rid ? ch = 'S') by * > 0;
"""

# items with a STORE row; the global no-op bug returned all three.
STORE_ITEMS = [(100,), (300,)]
ALL_ITEMS = [(100,), (200,), (300,)]


def _run(query: str):
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    return sorted(tuple(r) for r in exec.execute_query(MODEL + query).fetchall())


def test_bare_inline_aggregate_in_where_cograins_to_select():
    assert (
        _run("where count(rid ? ch='S') > 0 select item order by item;") == STORE_ITEMS
    )


def test_bare_named_aggregate_concept_in_where_cograins_to_select():
    assert _run("where has_store_bare select item order by item;") == STORE_ITEMS


def test_explicit_by_select_grain_in_where_unchanged():
    assert (
        _run("where count(rid ? ch='S') by item > 0 select item order by item;")
        == STORE_ITEMS
    )


def test_by_star_inline_preserves_global_semantics():
    assert (
        _run("where count(rid ? ch='S') by * > 0 select item order by item;")
        == ALL_ITEMS
    )


def test_by_star_named_preserves_global_semantics():
    assert _run("where has_store_global select item order by item;") == ALL_ITEMS


def test_plain_row_predicate_unchanged():
    assert _run("where ch = 'S' select item order by item;") == STORE_ITEMS


def test_composite_select_grain_cograins_each_key():
    # Select grain (item, ch): the bare aggregate co-grains to the full key.
    # Every (item, ch) group has >=1 row, so all survive — the point is it
    # builds and groups at the composite grain rather than erroring or globaling.
    rows = _run("where count(rid) > 0 select item, ch order by item, ch;")
    assert rows == [(100, "S"), (100, "W"), (200, "W"), (300, "S")]


def test_abstract_select_grain_stays_global():
    # No grain keys in the projection: a bare aggregate condition has nothing to
    # co-grain to and must remain global (and not crash).
    assert _run("where count(rid ? ch='S') > 0 select count(rid) as c;") == [(4,)]
