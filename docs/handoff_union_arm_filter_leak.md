# Handoff: a concept-level union is planned as a join of its arms, not a stacked source

**Status**: FIXED on `generation_refactoring` (2026-09-05). Pre-existing bug on
`main` (verified at `0455d97b3`, clean tree). Found 2026-09-05 while auditing why
`gen_union` sources its own parents instead of consuming the ones the group graph
hands it.

## Resolution

The structural direction below is what landed. A concept-level union is now
planned as a stacked source:

- `v4_helper/union_arms.py` gives every union argument an arm identity (the
  key it reads, filled in from the family for keyless arguments such as a
  constant). Sibling unions sharing arms form one family.
- `concept_graph._add_concept` walks each arm's lineage under its own scope
  label (`arm:<identity>`), the way rowset internals get a label, so an arm's
  roots never bucket with another arm's. A union itself is labelled like a
  ROOT (scope only): a WHERE over a stacked column reads the same union node.
  A WHERE argument sourced only inside one arm walks under that arm's scope.
- `group_rules.partition_unions` buckets one family into one UNION group, and
  `partition_roots` treats a union consumer as the arm's projection (roots
  feeding it co-source, like output roots at FINAL).
- `strategy_builder` merges a UNION group's parents only within an arm and
  hands `gen_union` one parent per arm; the `satisfiable_outputs` carve-out is
  gone. A union group can host an atom over its own stacked columns
  (`condition_placement._reachable_input`), and the demand pass asks the arms
  for the inputs of a stacked column the WHERE alone references.
- `gen_union` consumes `parents`, projects each arm's stacked columns off its
  own contributing arguments, and applies a hosted condition in every arm.
  `dispatch` forwards `preexisting_conditions`.

`select all_k, all_amt where amt > 0.15` now filters arm one only
(`[(2, 0.20), (3, 0.00), (4, 0.00)]`): the atom hosts on the arm's own scan,
which is where the column exists.

The rowset site (last section) is fixed separately:
`group_graph._attach_condition_roots_to_rowset_consumers` feeds a WHERE-only
root scan whose members hang off a rowset boundary's base key into the grouping
consumers of that boundary through a CONSTRAINT edge; the consumer's merge
preserves the boundary's base key as the join axis and is forced INNER, since a
filter scan may only remove rows.

Every `WHERE` on a `union(...)` output fails. Filtering the stacked output raises
`UnresolvableQueryException`; filtering an arm's private column silently returns
every row with no `WHERE` in the emitted SQL. Both are the same root cause.

Regression tests pinning it:

- `tests/core/processing/test_v4_union_arms.py` (arm identity and alignment, one
  parent per arm, which group hosts each atom)
- `tests/engine/test_duckdb_union_arm_filter_leak.py` (stacked-output and
  arm-private filters, the constant-arm cross product)
- `tests/engine/test_duckdb_rowset_aggregate_filter_leak.py` (the independent
  second site, see the last section)

## The contract

A `BuildUnionDatasource` is injected by `network_build._union_candidates` to read a
partition family as one source. Its arms carry the same column addresses, the
predicate is pushed into each arm, and WHERE handling is exactly what it would be
for a single datasource. That works today. Using the `tests/engine/test_duckdb_partition_cover.py`
four-cell grid:

| query | rows | `WHERE` in SQL |
| --- | --- | --- |
| `select id` | 8, correct | 0 |
| `select id where x > 1.5` | 4, correct | 4 (one per arm) |
| `select id, x where source = 'OSM'` | 4, correct | 0 (arm selection) |
| `select city, sum(x) -> t where x > 1.5` | correct | 4 |

A concept-level `union(a, b)` is the same idea one layer up: the UnionNode is one
source whose columns are the stacked outputs. So a predicate on a stacked output
(`all_amt`, `all_k`) should push into each arm rewritten to that arm's contributing
column, and a predicate on an arm's private column (`amt`, which is not a column of
the stacked source at all) is not a filter on that source.

That is not what the planner does.

## Repro

Both use the shared union fixture `tests/engine/test_duckdb_union_arm_cast.MODEL`:
arm 1 is `sales` with `k1 in (1, 2)` and `amt in (0.10, 0.20)`, arm 2 is `returns`
with `k2 in (3, 4)` and `pad = 0`; `all_k <- union(k1, k2)`,
`all_amt <- union(amt, pad)`.

```python
from tests.engine.test_duckdb_union_arm_cast import MODEL
from trilogy import Dialects

ex = Dialects.DUCK_DB.default_executor()
for q in (
    "select all_k, all_amt where all_amt > 0.15 order by all_k asc;",   # crashes
    "select all_k, all_amt where amt > 0.15 order by all_k asc;",       # leaks
):
    print(ex.generate_sql(MODEL + "\n" + q)[-1])
```

**Filtering the stacked output crashes.** All four spellings raise
`UnresolvableQueryException: Planner emitted a keyless join between row-bearing
sources ...: returns_at_local_k2_join_sales_at_local_k1_at_local_k1_local_k2`:

- `where all_amt > 0.15`
- `where all_k > 2`
- `where all_k > 2` with the value column dropped from the SELECT
- `select sum(all_amt) -> t where all_k > 2`

`HAVING all_amt > 0.15` works and returns `[(2, 0.20)]`, which is the committed
green control: the machinery above the union node is fine, only the pre-union path
is broken.

**Filtering an arm's private column leaks.** `where amt > 0.15` returns
`[(1, 0.10), (2, 0.20), (3, 0.00), (4, 0.00)]` and the generated SQL contains **no
`WHERE` clause anywhere**. Same for `where pad > 0.15`, and with the value column
dropped. A control without the union (`select k1 where amt > 1.0`) emits the WHERE
and returns one row.

## Root cause: the arms are joined, not stacked

Instrumenting `strategy_builder.build_node` for the UNION group.

No predicate, correct answer:

```
UNION group outputs= ['local.all_amt', 'local.all_k']
  parents= [('MergeNode', ['local.amt', 'local.k1', 'local.k2', 'local.pad'])]
  COND= None | PRE= None
```

That MergeNode is

```
MergeNode ['local.amt', 'local.k1', 'local.k2', 'local.pad']
  SelectNode ['local.amt', 'local.k1']
  SelectNode ['local.k2', 'local.pad']
```

a join of the two arms on nothing, since they share no key. `gen_union` ignores
`parents` (its only use of the name is the signature at `union.py:54`), re-plans
each arm through `search_parent`, and the join is quietly dropped. The answer comes
out right and the wasted plan is invisible: `build_node` runs four times for a query
that needs three nodes (one discarded `root` over all four arm concepts, the
`union`, then two re-planned `root`s).

Give a predicate somewhere to live and the same join is forced into the plan:

```
select all_k, all_amt where all_k > 2

UNION group outputs= ['local.all_k', 'local.k1', 'local.k2']
  parents= [('MergeNode', ['local.k1', 'local.k2'])]            <- keyless join
UNION group outputs= ['local.all_amt', 'local.all_k', 'local.k1', 'local.k2']
  parents= [('MergeNode', [... , 'local.all_k'])]
  COND= local.all_k > 2
UNION group outputs= ['local.all_k', 'local.k2']
UNION group outputs= ['local.all_k', 'local.k1', 'local.k2']
-> UnresolvableQueryException (keyless join guard)
```

The union group's outputs get widened with the arm keys `k1`/`k2` beside `all_k`,
four separate UNION groups get built, and the `['local.k1', 'local.k2']` merge is
the keyless join the guard rejects. The guard is doing its job; the join should
never have been proposed.

The arm-private-column leak is the same defect with the predicate landing one group
lower. UNION is in `ROW_SHAPE_BARRIER_DERIVATIONS`, so an atom on `amt` cannot be
pushed below the union and is decided at the union's ancestor group. At
`strategy_builder.py:4250` it is collected by `_accumulated_atoms_above` and passed
to `build_node` as `preexisting_conditions`:

```
build: ('union', ['local.all_amt', 'local.all_k'], PRE='local.amt > 1.0', COND='None')
```

`dispatch.py:98-110` then routes `ROWSET`/`UNION`/`SUBSELECT` through a call that
omits `preexisting_conditions`, so `gen_union` never sees it, and the ancestor node
that does carry the WHERE is discarded along with `parents`. The atom ends up in no
node in the plan.

`dispatch.py:66-76` already documents this inversion for ROOT: `preexisting_conditions`
normally means "an ancestor applied this, do not re-emit it", but for a generator
that **re-sources** instead of consuming `parents` it means the opposite and the
generator must apply it. `gen_root` gets it and honors it. `gen_union` re-sources
identically and is not given it. `staged_conditions` and `complete_partials` are
dropped on the same branch.

## Direction: reuse the datasource-union CONTRACT, not its container

The obvious move, wrapping the arms in a `BuildUnionDatasource` so the whole thing
becomes an ordinary source candidate, does not work. `BuildUnionDatasource.children`
is `list[BuildDatasource]`, and a union arm is not required to be a datasource
column. All of these plan and answer correctly today:

| arm shape | query | result |
| --- | --- | --- |
| BASIC | `union(amt * 2, pad)` | `[(1,0.20),(2,0.40),(3,0.30),(4,0.40)]`, correct |
| FILTER | `union(filter amt where cat = 'a', pad)` | `[(1,0.10),(3,0.30),(4,0.40)]`, correct |
| AGGREGATE | `union(sum(amt) by k1, pad)` | correct |
| CONSTANT | `union(amt, 0.0)` | 8 rows, a cross product (see below) |

A FILTER or AGGREGATE arm has to be planned, so `gen_union`'s per-arm re-entry
through `search_parent` is necessary and correct. That is not the defect. UNION
stays a generator.

What transfers is the **contract**
`create_union_datasource_candidate` (`datasource_nodes.py:402`, live v4, imported at
`v4_helper/source_planning.py:49`) implements and `gen_union` does not:

1. the query condition pushed into **each arm** as `injected_conditions`,
2. arms the predicate excludes dropped outright (`filter_union_children`),
3. partiality healing recomputed over the surviving arms,
4. each arm sourced through its own columns.

Only (4) exists in `gen_union` today. (1) is the absence that produced this whole
handoff, and it is the piece to port: `union.py:74` should hand each arm's
`search_parent` the predicate rewritten to that arm's contributing concept, exactly
as `create_union_datasource_candidate` hands each child its `injected_conditions`.

**The rewrite is the new machinery, and it already exists on the render side.** The
partition case needs no rewrite: arms share column addresses, so the same predicate
goes into every arm unchanged. A concept union does need one, because the predicate
is on `all_amt` and each arm contributes a different column. That substitution rule
is already implemented at `dialect/base.py:1445-1468`, where the renderer resolves
a `FunctionType.UNION` concept by finding whichever arm is local to the CTE,
rendering that arm's column, and casting to the union's derived type via
`union_arm_cast_target`. The planner needs the same mapping applied to a predicate
rather than to a projection.

So the work is three pieces, in order:

1. **Stop the group graph proposing a join.** The merged parent it hands the UNION
   group is what the keyless guard rejects once a predicate needs a host. This
   alone fixes the stacked-output crash.
2. **Push the predicate per arm.** Forward `conditions` and `preexisting_conditions`
   into `gen_union` (`dispatch.py:98`), rewrite each atom on a stacked output to
   the arm's own concept, and pass it to that arm's `search_parent`.
3. **Reject an arm-private predicate.** `amt` is not a column of the stacked source,
   so it has no rewrite in the other arm. That is a disconnect, and the existing
   machinery already raises cleanly for the analogous rowset case
   (`select rs.oid where amt > 1.0` raises `DisconnectedConceptsException`).

Forwarding `preexisting_conditions` alone (step 2 without step 1) closes the
arm-column leak and does nothing for the stacked-output crash, which is the
spelling users will actually write.

**What must be carried over.** `union(...)` is a keyspace union: a key present in
both arms counts once (`test_union_overlapping_keys_set_semantics`). `gen_union`
gets that today by deliberately carrying each arm's own row grain so the FINAL
dedup GROUP BY fires (`union.py:84-88`). Partition arms are disjoint by
construction, so the datasource path has never had to dedup, and nothing in it can
be assumed to preserve this.

**A constant arm cross-joins today.** `auto zero <- 0.0; auto all_v <- union(amt, zero)`
beside `all_k <- union(k1, k2)` returns 8 rows, every key paired with every value,
instead of 4. A constant arm has no key, so the arm join the group graph proposes
degenerates into a visible cross product rather than being quietly discarded. Same
root cause as the rest of this document, surfacing in the output rather than in the
plan, and the clearest single demonstration that the arms are being joined.

Two symptoms to keep as regression signal along the way:

- The carve-out at `strategy_builder.py:4344-4350` exempting UNION from
  `satisfiable_outputs` exists because the merged parent cannot satisfy the union's
  outputs. It should stop being necessary.
- The arm-key widening (`k1`/`k2` appearing in the union group's outputs beside
  `all_k`) is what produces the keyless merge; `_union_key_siblings` in
  `v4_helper/concept_graph.py` is the nearest suspect and was not traced.

`gen_rowset` has the identical "ignores the group graph's pre-built parents" shape
and should be audited in the same pass.

## Why the suite is green

Zero corpus footprint. Instrumenting `build_node` across `tests/modeling` +
`tests/core` (1090 tests) plus the six union/rowset engine suites:

| derivation | groups built | with non-None `preexisting_conditions` |
| --- | --- | --- |
| rowset | 168 | 0 |
| union | 2 | 0 |
| subselect | 3 | 0 |

Nothing in the corpus filters a union at all, so no existing test can catch any of
this. Do not treat a green run as evidence for a fix here; use the committed tests
plus a fresh count of non-zero `preexisting_conditions` firings.

## Second, independent site: aggregate over a rowset handle

`tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`. Not the dispatch drop
(the rowset group is built with both `preexisting_conditions` and `conditions`
None), so it needs its own diagnosis.

```
with rs as select oid, amt;
select sum(rs.amt) -> t where cat = 'a';
```

Returns `6.00`, should be `2.00`. `WHERE filters data BEFORE it reaches aggregates`
(`trilogy/ai/constants.py`), so a selected aggregate's inputs are filtered. Both
flanking controls in that file pass: the same query without the rowset returns
`2.00`, and filtering on a rowset handle (`where rs.cat = 'a'` with `cat` carried
into the rowset) returns `2.00`. Only the mixed base-concept filter over a
rowset-handle aggregate leaks.

The emitted SQL is the familiar shape: the filter lands in a sibling CTE that is
`INNER JOIN ... on 1=1` to the aggregate CTE, so it gates nothing.

```sql
cheerful as (SELECT cat FROM quizzical WHERE cat = 'a' GROUP BY 1),
highfalutin as (SELECT sum(quizzical.amt) as t FROM quizzical)   -- unfiltered
SELECT highfalutin.t FROM cheerful INNER JOIN highfalutin on 1=1
```

Same signature as the already-fixed `docs/handoff_v4_where_agg_dropped_atoms.md`,
different trigger. Worth checking whether that fix's guard can be extended to cover
a rowset-handle host rather than writing a new one.
