"""End-to-end suite for the disconnected-components error.

When a query's required concepts (outputs + filter args) cannot be welded into
one connected query, discovery raises `DisconnectedConceptsException` naming the
disconnected subgraphs and asking whether a join/merge is missing.

This file exercises that behaviour through `generate_sql` on small, fully
inline models — both the cases that SHOULD raise (with their exact subgraph
partition) and control cases that must RESOLVE (guarding against false
positives, e.g. FK-joined or merge-bridged models). Pure-helper tests for
`identify_disconnected_subgraphs` live in `test_disconnected_subgraphs.py`.

Every assertion here was verified against the engine, not assumed.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException

# --- reusable inline model fragments -------------------------------------------------

# model A: key a_id with property av
_A = """
key a_id int;
property a_id.av int;
datasource a (id: a_id, v: av) grain (a_id)
query '''select 1 id, 10 v''';
"""
# model B: unrelated key b_id with property bv
_B = """
key b_id int;
property b_id.bv int;
datasource b (id: b_id, v: bv) grain (b_id)
query '''select 1 id, 100 v''';
"""
# model C: unrelated key c_id with property cv
_C = """
key c_id int;
property c_id.cv int;
datasource c (id: c_id, v: cv) grain (c_id)
query '''select 1 id, 5 v''';
"""
# a second datasource on model A's key (so av + a2 stay connected)
_A2 = """
property a_id.a2 int;
datasource a_extra (id: a_id, x: a2) grain (a_id)
query '''select 1 id, 7 x''';
"""
# fact + dimension related by a foreign key column on the fact
_FACT_DIM = """
key cust_id int;
property cust_id.cname string;
datasource customers (id: cust_id, name: cname) grain (cust_id)
query '''select 1 id, 'x' name''';

key order_id int;
property order_id.amt float;
property order_id.buyer int;
datasource orders (id: order_id, amt: amt, buyer: cust_id) grain (order_id)
query '''select 1 id, 9.0 amt, 1 buyer''';
"""


def _groups(sql: str) -> list[list[str]]:
    """Run the query, assert it raises DisconnectedConceptsException, return its
    subgraph partition as sorted address groups."""
    eng = Dialects.DUCK_DB.default_executor()
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(sql)
    return sorted(sorted(g) for g in exc.value.subgraphs)


def _resolves(sql: str) -> None:
    """Assert the query plans without a disconnected-components error."""
    eng = Dialects.DUCK_DB.default_executor()
    eng.generate_sql(sql)


# --- cases that SHOULD raise (with exact partition) ----------------------------------


def test_two_unrelated_models_raise():
    assert _groups(_A + _B + "select av, bv;") == [["local.av"], ["local.bv"]]


def test_three_unrelated_models_raise():
    assert _groups(_A + _B + _C + "select av, bv, cv;") == [
        ["local.av"],
        ["local.bv"],
        ["local.cv"],
    ]


def test_unrelated_keys_only_raise():
    assert _groups(_A + _B + "select a_id, b_id;") == [["local.a_id"], ["local.b_id"]]


def test_connected_concepts_stay_grouped_orphan_split():
    # a_id and its property av share a model; bv is unrelated -> 2 groups, with
    # the connected pair kept together.
    assert _groups(_A + _B + "select a_id, av, bv;") == [
        ["local.a_id", "local.av"],
        ["local.bv"],
    ]


def test_merge_collapses_two_of_three_models():
    # merging a_id<->b_id welds A and B into one subgraph; C stays separate.
    sql = _A + _B + _C + "merge a_id into ~b_id;\nselect av, bv, cv;"
    assert _groups(sql) == [["local.av", "local.bv"], ["local.cv"]]


def test_where_filter_pulls_in_disconnected_model():
    # the filter `bv > 0` adds model B to the required set even though only `av`
    # is projected -> the split is still detected.
    assert _groups(_A + _B + "select av where bv > 0;") == [
        ["local.av"],
        ["local.bv"],
    ]


def test_fk_joined_concepts_stay_grouped_against_orphan():
    # The graph-reachability win: amt and cname are FK-joined (would resolve
    # together), cv is a true orphan. The split must keep the FK pair as ONE
    # group and isolate only cv -> [{amt, cname}, {cv}]. A lineage-anchor
    # partition would wrongly report THREE groups, falsely splitting amt/cname.
    assert _groups(_FACT_DIM + _C + "select amt, cname, cv;") == [
        ["local.amt", "local.cname"],
        ["local.cv"],
    ]


# --- control cases that must RESOLVE (no false positives) -----------------------------


def test_single_model_resolves():
    _resolves(_A + "select a_id, av;")


def test_one_of_two_models_resolves():
    # only model A is projected; B being present in the env must not trip it.
    _resolves(_A + _B + "select av;")


def test_two_properties_same_key_resolve():
    _resolves(_A + _A2 + "select av, a2;")


def test_merge_bridged_models_resolve():
    _resolves(_A + _B + "merge a_id into ~b_id;\nselect av, bv;")


def test_scoped_join_bridged_models_resolve():
    _resolves(_A + _B + "select av, bv left join a_id = b_id;")


def test_fk_joined_fact_dimension_resolves():
    # amt (order grain) and cname (customer grain) have no shared lineage anchor,
    # but the FK join path resolves them -> must NOT raise.
    _resolves(_FACT_DIM + "select amt, cname;")


def test_abstract_aggregates_cross_join_resolve():
    # two ungrouped aggregates are single-row and cross-join freely; not a split.
    _resolves(_A + _B + "select sum(av) as sa, sum(bv) as sb;")


# --- message + structure -------------------------------------------------------------


def test_message_names_subgraphs_and_suggests_fix():
    eng = Dialects.DUCK_DB.default_executor()
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_A + _B + "select av, bv;")
    message = str(exc.value)
    assert "disconnected subgraphs" in message
    # default namespace is stripped from the rendered message
    assert "{av}" in message
    assert "{bv}" in message
    assert "join or merge" in message


def test_message_includes_failing_statement_line():
    # the failing SELECT is the last line; the message must name it so multi-
    # statement files (rowsets + a final select) point at the right statement.
    sql = _A + _B + "\nselect av, bv;"
    line = sql.count("\n", 0, sql.index("select av, bv;")) + 1
    eng = Dialects.DUCK_DB.default_executor()
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(sql)
    assert f"statement at line {line}" in str(exc.value)


def test_message_suggests_connected_nested_equivalent(tmp_path):
    """The separate-import mistake (the dominant q75 thrash): a model already
    reachable by chaining through one import (`all_sales`) is imported a SECOND
    time as a disconnected copy, so `date.year` / `item.category` split off from
    the measure. The message must point at the connected `all_sales.*` path rather
    than (wrongly) suggesting a join/merge."""
    (tmp_path / "dates.preql").write_text(
        "key date_id int;\n"
        "property date_id.year int;\n"
        "datasource dates (id: date_id, yr: year) grain (date_id)\n"
        "query '''select 1 id, 2001 yr''';\n"
    )
    (tmp_path / "items.preql").write_text(
        "key item_id int;\n"
        "property item_id.category string;\n"
        "datasource items (id: item_id, cat: category) grain (item_id)\n"
        "query '''select 1 id, 'A' cat''';\n"
    )
    (tmp_path / "sales.preql").write_text(
        "import dates as date;\n"
        "import items as item;\n"
        "key sale_id int;\n"
        "property sale_id.amt float;\n"
        "datasource sales (id: sale_id, amt: amt, d: date.date_id, i: item.item_id)\n"
        "grain (sale_id)\n"
        "query '''select 1 id, 9.0 amt, 1 d, 1 i''';\n"
    )
    eng = Dialects.DUCK_DB.default_executor(working_path=tmp_path)
    sql = (
        "import sales as all_sales;\n"
        "import dates as date;\n"
        "import items as item;\n"
        "select date.year, item.category, all_sales.amt;\n"
    )
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(sql)
    message = str(exc.value)
    assert "did you mean `all_sales.date.year`" in message
    assert "did you mean `all_sales.item.category`" in message
    # the misleading join/merge hint is replaced by the chain-the-import guidance
    assert "join or merge" not in message
    assert "separately-imported copies" in message


def test_subgraphs_attribute_is_address_partition():
    eng = Dialects.DUCK_DB.default_executor()
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_A + _B + _C + "select av, bv, cv;")
    flattened = {addr for group in exc.value.subgraphs for addr in group}
    assert flattened == {"local.av", "local.bv", "local.cv"}
    assert all(isinstance(group, list) for group in exc.value.subgraphs)
