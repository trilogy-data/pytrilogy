# v4 binder/reference crashes: q2.1 + test_filter_constant

Two remaining v4-only crashes (both reference-binding failures — invalid SQL reaches
the renderer/DB). Both are **deterministic** (each reproduces 6/6 in isolation; there is
no hash-seed nondeterminism here — see the determinism note at the bottom). They are the
only genuine correctness blockers left after the `seen`/`output_addresses` fix in
`discovery_utility.py` restored the v4 path.

Confirm current state with the classifier (worst-of-N over the skip list):

```bash
.venv/Scripts/python.exe local_scripts/v4_classify.py   # exits 1 while these escalate
```

---

## 1. q2.1 `test_two_one` — `BinderException` on a union CTE (NEW; A-fix uncovered it)

`tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_one`. Labeled `_TPCDS_SIZE` but
actually crashes — the classifier escalates it. This is **not** the old existence
`RecursionError` (that was fixed, see `v4_existence_recursion_handoff.md` item A); fixing
the recursion uncovered this union-rendering bug underneath.

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_one" --runxfail -q
```

### Symptom (at SQL execution, not planning)

```
_duckdb.BinderException: Binder Error: Values list "yummy" does not have a column
named "web_sales_date_id"
LINE 63:     "yummy"."web_sales_date_id" as "date_id",
```

The offending CTE is rendered malformed — note there is no `SELECT`, every column is
self-qualified with the CTE's own name, and it ends `"yummy")`:

```sql
yummy as (
    "yummy"."catalog_sales_item_id" as "catalog_sales_item_id",
    "yummy"."catalog_sales_order_number" as "catalog_sales_order_number",
    "yummy"."web_sales_date_id" as "date_id",            -- references a col yummy lacks
    "yummy"."web_sales_item_id" as "web_sales_item_id",
    "yummy"."web_sales_order_number" as "web_sales_order_number",
    CASE WHEN "yummy"."date_year" in (2001,2002)
         THEN "yummy"."date_week_seq" ELSE NULL END as "relevent_week_seq"
    "yummy"),
```

q2.1 is the union query `catalog_sales UNION ALL web_sales` with a virtual filter
(`week_seq ? year in (2001,2002)` → `relevent_week_seq`). The consumer references the
**arm-specific** column `web_sales_date_id`, but the unified CTE exposes that field as
`date_id` — the union output column names and the consumer's references are inconsistent.
The self-referential, `SELECT`-less CTE body strongly suggests an **inline/elide
optimizer pass mangled the union member** (it folded the member scan into the union CTE
but left the projection referencing pre-fold arm column names).

### Where to look

- The union member render + the inline/elide optimizer on union arms. This is the same
  family as `v4_union_elide_regression.md` (a union arm collapsed into its datasource
  scan, dropping member cols) and the q02 `union_dim_pushdown` work
  (`v4_q02_invalid_alias_handoff.md`). Check whether `_elide_single_parent_passthrough`
  / `inline_datasource` / `predicate_pushdown` is firing on this union member and
  rewriting column references to the wrong (arm-prefixed) names.
- The virtual-filter (`relevent_week_seq`) projection sitting on the union CTE — q02 had
  the same `relevent_week_seq` filter cause trouble when placed on the union scan.
- First isolate the layer: dump the **pre-optimizer** generated SQL (disable the inline
  optimizers) — if the union CTE is well-formed there, the bug is purely in the
  optimizer rewrite, not the planner.

### Acceptance

- q2.1 builds valid SQL with rows matching the reference; no `BinderException`. Then it
  reverts to `_TPCDS_SIZE` if still over its length ceiling.
- Full v4 sweep stays at 0 failed.

---

## 2. `test_filter_constant` — `ValueError: Invalid reference string` (known item B)

`tests/modeling/usa_names/test_names.py::test_filter_constant`. Correctly labeled as a
crash; this is **item B** from `v4_existence_recursion_handoff.md`, with a refined
diagnosis already captured in memory (`project_v4_disconnected_aggregate_gate.md`).

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/modeling/usa_names/test_names.py::test_filter_constant" --runxfail -q
```

### Symptom

```
ValueError: Invalid reference string found in query: SELECT ...
trilogy/dialect/base.py:2371: ValueError
```

A WHERE over per-name aggregates is **disconnected** from the constant output. The
sibling `test_filter_constant_with_constant` raises `DisconnectedConceptsException` for
exactly this shape and was fixed by `_is_global_aggregate_gate` in
`raise_if_disconnected_for` (drops a non-output all-`AGGREGATE` subgraph). For
`test_filter_constant` the gate columns are **not sourced into the constant SELECT**, so
an unbound reference renders and `base.py:2371` rejects it. Same *class* as the old q02
invalid-reference bug, different trigger (a `filter` over a constant), and it does not go
through `union_dim_pushdown`. Trace the gate concept's source map through
`_assemble_final_node` / `resolve_concept_map`; the gate's grain must be sourced and
either projected or hidden, not left dangling.

### Acceptance

- `test_filter_constant` renders valid SQL with correct rows; no `ValueError`.
- Full v4 sweep stays at 0 failed.

---

## Determinism note (do not re-introduce the "run 5x" myth)

Both tests were checked in isolation, serially: q2.1 fails 6/6, q47 (a size sibling)
passes 6/6 — i.e. **deterministic**. Apparent run-to-run variance only shows up under
the *parallel* classifier and is a harness artifact (cross-run contention), not query
nondeterminism. There is no evidence of hash-seed nondeterminism in union/rowset query
results. When a result looks flaky, re-run the single test in isolation — it will be
stable.
