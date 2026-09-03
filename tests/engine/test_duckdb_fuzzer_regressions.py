"""Committed repros for the 2026-08-11 fuzzer sweep regressions.

The full differential-fuzzer corpus (local_scripts/fuzzer) had not run since
before the V4-default flip; when it did, 20 old-family cases were red plus two
partial-binder wrong-results bugs. Each test here pins one root cause with the
fuzzer's own data and oracle-derived expectations, so the families cannot
silently regress again without a full fuzzer sweep.

Root causes covered:
1. Rowset-boundary readback: a scoped join declared INSIDE a `with ... as
   select` rowset body has no edge in the outer reference graph, so the
   pre-discovery connectivity gate split the boundary's own handles
   (DisconnectedConceptsException).
2. Grouping placement: a ROLLUP/CUBE/GROUPING SETS contributor suppresses the
   FINAL dedup, so a row-grain leaf-dim contributor must dedup itself; and a
   grouping() identity flag must inherit its pass siblings' axis widening so
   it co-buckets with the pass (else a leaf column leaks bare into the
   grouped CTE -> BinderException, or the literal-0 flag pairing drops
   subtotal rows).
3. Union-arm partition: two pure renames of datasource-sibling roots split
   into separate scan buckets and cross-joined ON 1=1 with the arm's WHERE on
   one leg only.
4. Coalescing presence over a composite union join: an aggregate reading one
   leg of the composite axis deduped its input to that leg alone, collapsing
   distinct pairs.
5. Partial-binder filtered counts: a derived concept over a KEY inherited the
   key's fk-derived keys (last-declared fact wins), letting an undemanded
   partial (`~`) binder elect into the plan; and a named derived row
   expression's filter narrowed to the content's key grain instead of the
   condition's row population.
"""

import pytest

from trilogy import Dialects

GROUPS_EVENTS = """
key group_id int;
property group_id.group_name string;
property group_id.active bool;
datasource groups (
    gid: group_id,
    name: group_name
)
grain (group_id)
query '''select 1 as gid, 'alpha' as name
union all
select 2 as gid, 'beta' as name
union all
select 3 as gid, 'gamma' as name
union all
select 4 as gid, 'delta' as name''';

key event_id int;
property event_id.event_amount int;
property event_id.nullable_amount int?;
property event_id.event_active bool;
datasource events (
    eid: event_id,
    gid: group_id,
    amount: event_amount,
    nullable_amount: nullable_amount,
    active: event_active
)
grain (event_id)
query '''select 1 as eid, 1 as gid, 0 as amount, 0 as nullable_amount, false as active
union all
select 2 as eid, 1 as gid, -2 as amount, null as nullable_amount, true as active
union all
select 3 as eid, 2 as gid, 6 as amount, 3 as nullable_amount, true as active
union all
select 4 as eid, 2 as gid, 6 as amount, 3 as nullable_amount, true as active
union all
select 5 as eid, 3 as gid, 9 as amount, null as nullable_amount, false as active
union all
select 6 as eid, 3 as gid, 1 as amount, 1 as nullable_amount, true as active
union all
select 7 as eid, 4 as gid, 12 as amount, null as nullable_amount, false as active''';
"""

FACTS = """
key left_id int;
property left_id.left_key int?;
property left_id.left_value int;
datasource left_facts (id: left_id, k: left_key, value: left_value)
grain (left_id)
query '''select 1 as id, 1 as k, 1 as value
union all
select 2 as id, 1 as k, 2 as value
union all
select 3 as id, 2 as k, 4 as value
union all
select 4 as id, 3 as k, 8 as value
union all
select 5 as id, null as k, 16 as value''';

key subset_id int;
property subset_id.subset_key int?;
property subset_id.subset_value int;
datasource subset_facts (
    id: subset_id,
    k: subset_key,
    value: subset_value
)
grain (subset_id)
query '''select 1 as id, 1 as k, 100 as value
union all
select 2 as id, 2 as k, 200 as value
union all
select 3 as id, 2 as k, 400 as value
union all
select 4 as id, null as k, 800 as value''';
"""

VISITS = """
key visit_id int;
property visit_id.visit_amount int;
datasource visits (id: visit_id, gid: ?group_id, amount: visit_amount)
grain (visit_id)
query '''select 1 as id, null as gid, 0 as amount
union all
select 2 as id, 1 as gid, 4 as amount''';
"""

SALES_RETURNS = """
key sale_id int;
property sale_id.sale_amount int;
datasource sales (id: sale_id, gid: ~group_id, amount: sale_amount)
grain (sale_id)
query '''select 1 as id, 1 as gid, 10 as amount
union all
select 2 as id, 1 as gid, 20 as amount
union all
select 3 as id, 2 as gid, 30 as amount
union all
select 4 as id, 3 as gid, 40 as amount
union all
select 5 as id, 3 as gid, 5 as amount''';

key return_id int;
property return_id.return_amount int;
datasource returns (id: return_id, gid: ~group_id, amount: return_amount)
grain (return_id)
query '''select 1 as id, 1 as gid, 1 as amount
union all
select 2 as id, 1 as gid, 2 as amount
union all
select 3 as id, 1 as gid, 4 as amount
union all
select 4 as id, 2 as gid, 8 as amount
union all
select 5 as id, 4 as gid, 16 as amount''';
"""


def run(model: str, query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    return executor.execute_text(model + query)[-1].fetchall()


def test_rowset_boundary_subset_subordinate_readback():
    # fuzzer {edge,dense}__rowset_boundary__{subset,union}_subordinate_readback:
    # the subset join lives INSIDE the boundary body, and reading the
    # subordinate key back under its authored address raised
    # DisconnectedConceptsException at the pre-discovery connectivity gate.
    rows = run(
        FACTS,
        """
rowset anchor <- select left_key as k, sum(left_value) as anchor_total;
rowset subordinate <- select
    subset_key as k,
    sum(subset_value) as subordinate_total;

with boundary as
select
    subordinate.k,
    anchor.anchor_total,
    subordinate.subordinate_total
subset join subordinate.k = anchor.k;

select
    boundary.subordinate.k,
    boundary.anchor_total,
    boundary.subordinate_total
order by boundary.subordinate.k asc nulls last;
""",
    )
    assert rows == [(1, 3, 100), (2, 4, 600), (3, 8, None), (None, 16, 800)]


def test_rowset_boundary_union_subordinate_readback():
    rows = run(
        FACTS,
        """
rowset anchor <- select left_key as k, sum(left_value) as anchor_total;
rowset subordinate <- select
    subset_key as k,
    sum(subset_value) as subordinate_total;

with boundary as
select
    subordinate.k,
    anchor.anchor_total,
    subordinate.subordinate_total
union join subordinate.k = anchor.k;

select
    boundary.subordinate.k,
    boundary.anchor_total,
    boundary.subordinate_total
order by boundary.subordinate.k asc nulls last;
""",
    )
    assert rows == [(1, 3, 100), (2, 4, 600), (3, 8, None), (None, 16, 800)]


def test_rollup_extra_leaf_dim_dedups_pairs():
    # fuzzer dense__grouping_placement__{rollup,cube,grouping_sets}_extra_leaf_dim:
    # gid=2 has two rows with the same (gid, active) pair; the leaf join-back
    # must be per distinct pair, not per source row. The subtotal row keeps a
    # NULL leaf dim.
    rows = run(
        GROUPS_EVENTS,
        """
select group_id, event_active, sum(event_amount) as total
by rollup (group_id)
order by group_id asc nulls last, event_active asc nulls last;
""",
    )
    assert rows == [
        (1, False, -2),
        (1, True, -2),
        (2, True, 12),
        (3, False, 10),
        (3, True, 10),
        (4, False, 12),
        (None, None, 32),
    ]


def test_rollup_extra_leaf_dim_with_grouping_sets():
    rows = run(
        GROUPS_EVENTS,
        """
select group_id, event_active, sum(event_amount) as total
by grouping sets ((group_id), ())
order by group_id asc nulls last, event_active asc nulls last;
""",
    )
    assert rows == [
        (1, False, -2),
        (1, True, -2),
        (2, True, 12),
        (3, False, 10),
        (3, True, 10),
        (4, False, 12),
        (None, None, 32),
    ]


def test_rollup_label_over_union_joined_rowsets():
    # fuzzer *__grouping_placement__rollup_label_over_union_joined_rowsets:
    # ROLLUP over a derived label while the backing key stays a leaf dim. The
    # grouping() identity flag must ride the aggregates' own rollup pass —
    # separately bucketed it either leaked the leaf key bare into a grouped
    # CTE (BinderException) or paired via a literal-0 stamp and dropped the
    # grand-total row.
    rows = run(
        GROUPS_EVENTS + SALES_RETURNS,
        """
rowset sale_side <-
select group_id as g, sum(sale_amount) as sales;
rowset return_side <-
select group_id as g, sum(return_amount) as returns;

select
    'g' || coalesce(sale_side.g, return_side.g)::string as label,
    coalesce(sale_side.g, return_side.g) as g,
    sum(coalesce(sale_side.sales, 0)) as sales,
    sum(coalesce(return_side.returns, 0)) as returns
union join sale_side.g = return_side.g
by rollup (label)
order by label asc nulls last, g asc nulls last;
""",
    )
    assert rows == [
        ("g1", 1, 30, 7),
        ("g2", 2, 30, 8),
        ("g3", 3, 45, 0),
        ("g4", 4, 0, 16),
        (None, None, 105, 31),
    ]


def test_union_arm_where_partition():
    # fuzzer *__union__nullable_partition: each arm renames a fact FK column
    # and a fact property; the two roots must co-source one scan so the arm's
    # WHERE covers both columns (split, they cross-joined ON 1=1 with the
    # filter on one leg).
    rows = run(
        GROUPS_EVENTS,
        """
with combined as union(
    (where nullable_amount is null select group_id as gid, event_amount as value),
    (where nullable_amount is not null select group_id as gid, nullable_amount as value)
) -> (gid, value);

select combined.gid, sum(combined.value) as total
having sum(combined.value) > 2
order by combined.gid asc nulls last;
""",
    )
    assert rows == [(2, 3), (3, 10), (4, 12)]


def test_union_arm_partition_with_optional_fk_sibling():
    # An optional (`?`) binder for the arm's key buckets the arm's two outputs
    # into different domains, so each consumer regroups the shared scan to its
    # own single column. Those two groups have no key to pair on: the passthrough
    # over the scan must be elided as the scan is built, before the consumers
    # take their copies of it.
    rows = run(
        GROUPS_EVENTS + VISITS,
        """
with combined as union(
    (where event_active select group_id as gid, event_amount as value),
    (where not event_active select group_id as gid, event_amount as value)
) -> (gid, value);

select combined.gid, sum(combined.value) as total
having sum(combined.value) > 2
order by combined.gid asc nulls last;
""",
    )
    assert rows == [(2, 6), (3, 10), (4, 12)]


def test_renamed_key_and_property_share_scan():
    # The reduced shape behind the union-arm defect: both outputs are pure
    # renames, so no shared consumer relates the two roots — only the events
    # table does.
    rows = run(
        GROUPS_EVENTS,
        """
where nullable_amount is not null
select group_id as gid, nullable_amount as value
order by gid asc, value asc;
""",
    )
    assert rows == [(1, 0), (2, 3), (3, 1)]


def test_coalescing_presence_composite_keeps_pairs():
    # fuzzer dense__coalescing_presence__union_plain_composite: the FULL join
    # pairs on BOTH legs, so presence sums must range over distinct
    # (customer, item) pairs — deduping to the customer leg alone collapsed
    # catalog pairs (4,1)/(4,0) into one row.
    rows = run(
        """
key left_id int;
property left_id.left_key int?;
property left_id.left_value int;
datasource left_facts (id: left_id, k: left_key, value: left_value)
grain (left_id)
query '''select 1 as id, 1 as k, 3 as value
union all
select 2 as id, 2 as k, 5 as value
union all
select 3 as id, 2 as k, 7 as value
union all
select 4 as id, 3 as k, 11 as value
union all
select 5 as id, null as k, 13 as value''';

key union_id int;
property union_id.union_key int?;
property union_id.union_value int;
datasource union_facts (id: union_id, k: union_key, value: union_value)
grain (union_id)
query '''select 1 as id, 2 as k, 31 as value
union all
select 2 as id, 3 as k, 37 as value
union all
select 3 as id, 4 as k, 41 as value
union all
select 4 as id, 4 as k, 43 as value
union all
select 5 as id, null as k, 47 as value''';
""",
        """
rowset store_set <- where left_key is not null and left_value <= 8
select
    left_key as customer_id,
    left_id % 2 as item_id;
rowset catalog_set <- where union_key is not null and union_value <= 80
select
    union_key as customer_id,
    union_id % 2 as item_id;

select
    sum(case
        when store_set.customer_id is not null
            and catalog_set.customer_id is null
        then 1 else 0
    end) as store_only,
    sum(case
        when store_set.customer_id is null
            and catalog_set.customer_id is not null
        then 1 else 0
    end) as catalog_only,
    sum(case
        when store_set.customer_id is not null
            and catalog_set.customer_id is not null
        then 1 else 0
    end) as both
union join store_set.customer_id = catalog_set.customer_id and store_set.item_id = catalog_set.item_id;
""",
    )
    assert rows == [(2, 3, 1)]


@pytest.fixture
def partial_binder_executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(GROUPS_EVENTS + SALES_RETURNS)
    return executor


def test_partial_binder_not_elected_for_grain_count(partial_binder_executor):
    # fuzzer *__distinct_count__filtered_grain_row_population: `sales` and
    # `returns` bind group_id partially (~) and are never referenced; the
    # filtered grain() count must plan a single events scan. Fk-derived keys
    # (last declared fact wins) previously re-keyed the hash onto returns and
    # its rows multiplied the count.
    sql = partial_binder_executor.generate_sql(
        "select count(grain(group_id, event_active) ? event_amount > 4) as x;"
    )[-1]
    assert "returns" not in sql
    assert "sales" not in sql
    rows = partial_binder_executor.execute_text(
        "select count(grain(group_id, event_active) ? event_amount > 4) as x;"
    )[-1].fetchall()
    assert rows == [(4,)]


def test_named_derived_filtered_count_matches_inline(partial_binder_executor):
    # fuzzer edge__distinct_count__filtered_named_derived_key_count: a named
    # derived row expression counts the condition's row population, exactly
    # like its anonymous inline spelling (previously it deduped to the key
    # grain and self-joined, fanning within groups).
    partial_binder_executor.execute_text("auto gid_label <- group_id + 0;")
    named = partial_binder_executor.execute_text(
        "select count(gid_label ? event_amount > 4) as x;"
    )[-1].fetchall()
    inline = partial_binder_executor.execute_text(
        "select count(group_id + 0 ? event_amount > 4) as x;"
    )[-1].fetchall()
    assert named == inline == [(4,)]


def test_bare_key_filtered_count_keeps_domain_semantics(partial_binder_executor):
    # The settled standard the two fixes above must not disturb: a filtered
    # BARE key counts its own domain (distinct keys with a matching row).
    rows = partial_binder_executor.execute_text(
        "select count(group_id ? event_amount > 4) as x;"
    )[-1].fetchall()
    assert rows == [(3,)]


def test_nested_global_aggregate_over_rowset():
    # Guard for the near-miss found while fixing the partial-binder bugs: a
    # global sum over a rowset's aggregate output must stay one row (an
    # over-eager key-identity change collapsed the sum via the grain-match
    # formula and returned per-group rows).
    rows = run(
        GROUPS_EVENTS,
        """
rowset grouped <- select group_id as gid, sum(event_amount) as total;
rowset grand <- select sum(grouped.total) as grand_total;
select grand.grand_total;
""",
    )
    assert rows == [(32,)]
