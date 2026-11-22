from pathlib import Path

import pytest

import tests.join_matrix.test_null_group_matrix as null_groups
import tests.join_matrix.test_two_source_matrix as two_source
from tests.engine.test_padded_null_pairing import TWO_REGIONS
from tests.join_matrix.harness import make_engine, run_cell, write_models
from trilogy import Dialects, Environment
from trilogy.core.optimizations.value_set_join_upgrade import (
    UpgradeOuterFromKeySetEquivalence,
)

ROWSETS = """
key cust_id int;
key order_id int;
property order_id.yr int;
datasource orders (order_id, cust_id, yr) grain (order_id)
query '''select 10 order_id, 1 cust_id, 2001 yr union all select 11, 2, 2002
union all select 12, 1, 2002''';
rowset early <- where yr = 2001 select cust_id as k;
rowset late <- where yr = 2002 select cust_id as k;
"""


@pytest.fixture(autouse=True)
def planner_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        UpgradeOuterFromKeySetEquivalence,
        "optimize",
        lambda self, cte, inverse_map: (False, None),
    )


def _joins_and_rows(model: str, query: str) -> tuple[list[str], list[tuple]]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(query)[-1]
    joins = [line.strip() for line in sql.splitlines() if " JOIN " in line]
    rows = executor.execute_text(query)[-1].fetchall()
    return joins, sorted(rows, key=str)


def test_subset_join_into_a_rowset_boundary_keeps_only_the_anchor():
    joins, rows = _joins_and_rows(
        ROWSETS, "select late.k, early.k subset join early.k = late.k;"
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["LEFT OUTER"], joins
    assert rows == [(1, 1), (2, 2)]


def test_partial_fact_beside_its_complete_dimension_is_typed_inner():
    joins, rows = _joins_and_rows(
        TWO_REGIONS, "select bucket, sum(amount) as s where name is null;"
    )
    customer_join = next(j for j in joins if "customer_id" in j)
    assert customer_join.startswith("INNER JOIN"), joins
    assert rows == [("z", None)]


UNFILTERED = """
key id int;
key bill_id int;
key ship_id int;
datasource orders (id: id, bill: bill_id, ship: ship_id) grain (id)
query '''select 1 id, 100 bill, 200 ship union all select 2, 101, 201''';
with buyers_a as select id, bill_id as cust_id;
with buyers_b as select id, ship_id as cust_id;
"""


def test_unfiltered_rowsets_beside_their_anchor_are_typed_inner():
    joins, rows = _joins_and_rows(
        UNFILTERED,
        "select id, buyers_a.cust_id as a, buyers_b.cust_id as b "
        "subset join buyers_a.id = id and buyers_b.id = id;",
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["INNER", "INNER"], joins
    assert rows == [(1, 100, 200), (2, 101, 201)]


TWINS = """
key cust_id int;
key order_id int;
property order_id.amount int;
key visit_id int;
property visit_id.pages int;
datasource orders (order_id, cust_id, amount) grain (order_id)
query '''select 1 order_id, 1 cust_id, 10 amount union all select 2, 2, 20''';
datasource visits (visit_id, cust_id, pages) grain (visit_id)
query '''select 1 visit_id, 1 cust_id, 5 pages union all select 2, 2, 7''';
"""

TWIN_QUERY = "select cust_id, sum(amount) as spend, sum(pages) as seen;"

ONE_SIDED_NULL = """
key cust_id int;
key order_id int;
property order_id.amount int;
key visit_id int;
property visit_id.pages int;
datasource orders (order_id, {order_binding}, amount) grain (order_id)
query '''select 1 order_id, 1 cust_id, 10 amount union all select 2, null, 20''';
datasource visits (visit_id, ?cust_id, pages) grain (visit_id)
query '''select 1 visit_id, 1 cust_id, 5 pages union all select 2, 2, 7''';
"""


def test_twin_reads_of_one_complete_key_are_typed_inner():
    joins, rows = _joins_and_rows(TWINS, TWIN_QUERY)
    assert [j.split(" JOIN ")[0] for j in joins] == ["INNER"], joins
    assert rows == [(1, 10, 5), (2, 20, 7)]


@pytest.mark.parametrize("order_binding", ["?cust_id", "cust_id: ~?cust_id"])
def test_a_null_group_on_one_side_is_no_value_of_the_domain(order_binding: str):
    joins, rows = _joins_and_rows(
        ONE_SIDED_NULL.format(order_binding=order_binding), TWIN_QUERY
    )
    assert joins[-1].startswith("FULL JOIN"), joins
    assert (None, 20, None) in rows


@pytest.mark.usefixtures("planner_only")
@pytest.mark.parametrize("order_binding", ["?cust_id", "cust_id: ~?cust_id"])
def test_the_optimizer_keeps_a_one_sided_null_group(
    order_binding: str, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.undo()
    _, rows = _joins_and_rows(
        ONE_SIDED_NULL.format(order_binding=order_binding), TWIN_QUERY
    )
    assert (None, 20, None) in rows


SHARED_DATES = """
key date_id int;
property date_id.year int;
key ticket int;
key order_number int;
datasource dates (date_id, y: year) grain (date_id)
query '''select 1 date_id, 2020 y union all select 2, 2021''';
datasource store_sales (ticket, date_id: ~?date_id) grain (ticket)
query '''select 1 ticket, 1 date_id union all select 2, null''';
datasource web_sales (order_number, date_id: ~?date_id) grain (order_number)
query '''select 1 order_number, 2 date_id''';
"""


def test_undated_sales_of_one_fact_keep_their_null_year(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.undo()
    _, rows = _joins_and_rows(
        SHARED_DATES,
        "select year + 1 as y, count(ticket) as s, count(order_number) as w;",
    )
    assert rows == [(2021, 1, 0), (2022, 0, 1), (None, 1, 0)]


def test_twin_aggregates_of_one_stream_are_typed_inner():
    env = Environment()
    env.parse(
        null_groups._model(
            null_groups._rows("complete", True, 1),
            null_groups._rows("complete", False, 100),
            "complete",
            "complete",
            False,
        )
    )
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = "select k + 1 as key, sum(l_val) as l, sum(r_val) as r;"
    joins = [
        line.strip()
        for line in executor.generate_sql(query)[-1].splitlines()
        if " JOIN " in line
    ]
    assert joins[-1].startswith("INNER JOIN"), joins
    rows = sorted(executor.execute_text(query)[-1].fetchall(), key=str)
    assert rows == [(2, 3, 300), (3, 4, 400), (4, 8, 800), (None, 16, None)]


@pytest.mark.parametrize("null_sides", ["left", "right", "both"])
def test_equal_keys_both_sides_carry_whole_are_typed_inner(
    tmp_path: Path, null_sides: str
):
    query = two_source._query("root", "equal", "merge")
    workdir = write_models(tmp_path, True, null_sides)
    sql = make_engine(workdir).generate_sql(query)[-1]
    assert "INNER JOIN" in sql and " OUTER " not in sql and "FULL" not in sql, sql
    assert run_cell(workdir, query) == two_source._oracle(
        "root", "equal", True, null_sides
    )


@pytest.mark.parametrize("form", ["join", "merge", "into"])
def test_rowsets_over_complete_bodies_carry_an_equal_key(tmp_path: Path, form: str):
    query = two_source._query("rowset", "equal", form)
    workdir = write_models(tmp_path)
    sql = make_engine(workdir).generate_sql(query)[-1]
    assert "INNER JOIN" in sql and "FULL" not in sql, sql
    assert run_cell(workdir, query) == two_source._oracle("rowset", "equal")


MERGED_YEARS = """
key s_date_id int;
property s_date_id.s_year int;
datasource s_dates (d: s_date_id, y: s_year) grain (s_date_id)
query '''select 1 d, 2020 y union all select 2, 2021''';
key w_date_id int;
property w_date_id.w_year int;
datasource w_dates (d: w_date_id, y: w_year) grain (w_date_id)
query '''select 1 d, 2020 y union all select 2, 2021''';
key ticket int;
datasource store_sales (t: ticket, d: ~?s_date_id) grain (ticket)
query '''select 1 t, 1 d union all select 2, null''';
key order_number int;
datasource web_sales (o: order_number, d: ~?w_date_id) grain (order_number)
query '''select 1 o, 2 d''';
merge w_year into s_year;
"""


def test_a_merged_attribute_keeps_one_facts_undated_rows(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.undo()
    _, rows = _joins_and_rows(
        MERGED_YEARS, "select s_year, count(ticket) as s, count(order_number) as w;"
    )
    assert rows == [(2020, 1, 0), (2021, 0, 1), (None, 1, 0)]


UNNESTED = """
key ecoregion_id int;
property ecoregion_id.biome string;
datasource eco (ecoregion_id, biome) grain (ecoregion_id)
query '''select 1 ecoregion_id, 'forest' biome union all select 2, 'desert'
union all select 3, 'tundra' ''';
key species string;
property species.native_ecoregions list<int>;
datasource enrich (species, ?native_ecoregions) grain (species)
query '''select 'oak' species, [1, 2] native_ecoregions
union all select 'cactus', [2] union all select 'moss', []''';
auto native_ecoregion <- unnest(native_ecoregions);
merge native_ecoregion into ecoregion_id;
"""


def test_elements_of_complete_lists_carry_an_equal_key():
    joins, rows = _joins_and_rows(
        UNNESTED, "select species, array_sort(array_agg(biome)) as b;"
    )
    assert joins and all(j.startswith("INNER JOIN") for j in joins), joins
    assert rows == [("cactus", ["desert"]), ("oak", ["desert", "forest"])]


def test_an_owner_with_an_empty_list_keeps_its_row():
    joins, rows = _joins_and_rows(
        UNNESTED,
        "select species, native_ecoregions, array_sort(array_agg(biome)) as b;",
    )
    assert rows == [
        ("cactus", [2], ["desert"]),
        ("moss", [], None),
        ("oak", [1, 2], ["desert", "forest"]),
    ], joins


PROCESSES = """
key pid int;
property pid.parent int?;
property pid.name string;
property pid.mem int;
datasource procs (pid, parent, name, mem) grain (pid)
query '''select 1 as pid, null as parent, 'code' as name, 10 as mem
union all select 2, 1, 'node', 5 union all select 3, 2, 'py', 1
union all select 4, null, 'fx', 7''';
key app_id int;
property app_id.app_name string;
datasource apps (pid: app_id, name: app_name) grain (app_id)
query '''select 1 as pid, 'code' as name union all select 2, 'node'
union all select 3, 'py' union all select 4, 'fx' ''';
auto root <- recurse_edge(pid, parent);
merge root into app_id;
"""


def test_a_walk_carries_its_seeds_and_names_no_null_root():
    joins, rows = _joins_and_rows(
        PROCESSES, "select app_id, app_name, sum(mem) as total;"
    )
    assert not any(j.startswith(("FULL", "RIGHT")) for j in joins), joins
    assert not any("is not distinct from" in j for j in joins[1:]), joins
    assert rows == [(1, "code", 16), (4, "fx", 7)]


LAUNCHES = """
key vehicle string;
key variant string;
datasource vehicles (vehicle, variant) grain (vehicle, variant)
query '''select 'atlas' vehicle, '-' variant union all select 'atlas', 'v'
union all select 'soyuz', '-' ''';
key id string;
datasource launches (id, vehicle: ~vehicle, variant: ~variant) grain (id)
query '''select 'l1' id, 'atlas' vehicle, '-' variant
union all select 'l2', 'atlas', 'v' ''';
key sat string;
key sat_launch string;
datasource satellites (sat, sat_launch: ~sat_launch) grain (sat)
query '''select 's1' sat, 'l1' sat_launch union all select 's2', 'l1'
union all select 's3', 'l2' ''';
merge sat_launch into ~id;
"""


def test_per_key_subsets_keep_every_launch_beside_its_vehicles():
    joins, rows = _joins_and_rows(
        LAUNCHES, "select vehicle, variant, count(sat) as sats;"
    )
    assert not any(j.startswith("FULL") for j in joins), joins
    assert rows == [("atlas", "-", 2), ("atlas", "v", 1), ("soyuz", "-", 0)]


STORE_YEARS = """
key store_id int;
property store_id.store_name string;
datasource stores (i: store_id, n: store_name) grain (store_id)
query '''select 10 i, 'a' n union all select 20, 'b' union all select 30, 'c' ''';
key sid int;
property sid.yr int;
property sid.st int;
property sid.amt int;
datasource sales (i: sid, y: yr, s: st, a: amt) grain (sid)
query '''select 1 i, 2001 y, 10 s, 5 a union all select 2, 2002, 20, 7
union all select 3, 2002, 30, 1''';
merge st into store_id;
with early as where yr = 2001 select st as store, store_name, sum(amt) as amt;
with late as where yr = 2002 select st as store, sum(amt) as amt;
"""


def test_an_equal_key_within_a_complete_scan_is_covered():
    joins, rows = _joins_and_rows(
        STORE_YEARS,
        "select late.store, late.amt, early.amt subset join early.store = late.store;",
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["INNER", "LEFT OUTER"], joins
    assert rows == [(20, 7, None), (30, 1, None)]


def test_a_union_join_narrows_where_the_graph_proves_a_subset():
    joins, rows = _joins_and_rows(
        STORE_YEARS + "with every as select st as store, sum(amt) as amt;",
        "select every.store, every.amt, late.amt union join every.store = late.store;",
    )
    assert not any(j.startswith("FULL") for j in joins), joins
    assert rows == [(10, 5, None), (20, 7, 7), (30, 1, 1)]


EQUAL_CHAIN = """
key a_id int;
property a_id.ak int;
property a_id.av int;
datasource a (i: a_id, k: ak, v: av) grain (a_id)
query '''select 1 i, 1 k, 10 v union all select 2, 2, 20''';
key b_id int;
property b_id.bk int;
property b_id.bv int;
datasource b (i: b_id, k: bk, v: bv) grain (b_id)
query '''select 1 i, 1 k, 100 v union all select 2, 2, 200''';
key c_id int;
property c_id.ck int;
property c_id.cv int;
datasource c (i: c_id, k: ck, v: cv) grain (c_id)
query '''select 1 i, 1 k, 7 v union all select 2, 2, 77''';
auto ka <- ak + 1;
auto kb <- bk + 1;
auto kc <- ck + 1;
merge ka into kb;
merge kb into kc;
"""


def test_an_equal_chain_both_sides_carry_whole_joins_inner():
    joins, rows = _joins_and_rows(
        EQUAL_CHAIN, "select ka, sum(av) as a, sum(bv) as b, sum(cv) as c;"
    )
    assert all(j.startswith("INNER") for j in joins), joins
    assert rows == [(2, 10, 100, 7), (3, 20, 200, 77)]


def test_a_union_join_member_read_off_its_lookup_is_covered():
    """q64's `union join agg_99.item_sk_99 = agg_00.item_sk_00 = ss.item.sk`:
    the graph puts the member within the lookup's key, and the veto still
    yields to the proof."""
    path = Path(__file__).parent / "modeling" / "tpc_ds_duckdb" / "query64.preql"
    env, statements = Environment(working_path=path.parent).parse(path.read_text())
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
        statements[-1]
    )[-1]
    item = next(
        line
        for line in sql.splitlines()
        if '"memory"."item"' in line and "item_sk_99" in line
    )
    assert item.strip().startswith("INNER JOIN"), item
