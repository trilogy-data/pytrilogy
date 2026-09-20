# Handoff: a derived concept lives on its key domain (extension-row semantics)

Session of 2026-09-20, branch `extension-row-null-semantics` (off `main` at #700). The rule is decided, the oracle exists, one implementation route was tried and backed out. Start at "Pick up here".

Earlier context (responsive aggregates, `~` family pairing, key hosting) landed as #699 and #700; `docs/handoff_aggregate_grain_fd_canonicalization.md` covers it.

## The rule (decided by the owner)

A derived concept is a **function of its keys** and exists only on that key domain. On a row where a key's entity is absent it is NULL.

Customers, orders, not every customer has an order (`orders.customer_id: ~customer_id`), `status <- case when delivery_date is not null then 'delivered' else 'in-transit' end`. `select customer_id, status, count(order_id)` gives the orderless customer `(3, NULL, 0)`, not `(3, 'in-transit', 0)`: `status` is a property of `order_id` (already auto-derived: `Purpose.PROPERTY`, keys `{order_id}`) and there is no order.

- **It is a key-domain rule, not a CASE rule.** `is null`, `coalesce`, `concat` invent values the same way. Conversely `case when count(order_id) by customer_id > 0 then 'active' else 'dormant' end` is keyed on `customer_id`, which IS present, so its ELSE legitimately fires.
- **Multi-key is strict.** `concat(name, '-', status)` is NULL for the orderless customer, not `'cat-'`.
- **Defined on the semantic model.** A `~` binding makes the domain partial; joins are generated to conform to the logical model, never the reverse. Do not define correctness by join type.
- **Not BASIC-only** (owner's challenge, confirmed). Any derivation keyed on K reads a row stream restricted to K's domain:
  - row-local (BASIC, FILTER): the padded row only gets a wrong value for itself;
  - row-set (AGGREGATE, WINDOW): the padded row is an INPUT. `count(coalesce(amount, 0))` counts it (1 for a customer with 0 orders); `row_number order_id over customer_id order by amount` numbers it 1; across a span it can shift real rows' ranks depending on NULL sort order.
- **`?` is orthogonal to `~`.** A NULL foreign key is declared with `?`; that NULL is a value on a real row and `coalesce(customer.name, 'unknown')` must still evaluate there.

### The oracle: materialization invariance

Storing a derivation as a column at its grain must never change a query's rows. `tests/engine/test_derived_key_domain.py` runs every query against an `auto` model and a twin where the derivations are datasource columns, and compares rows.

- `HOLDS`: pass today. `OWED`: strict xfail, 17 queries plus `test_orderless_customer_has_no_status`. A fix shows up as XPASS; move the query to `HOLDS`.
- `test_inline_spelling_matches_named`: an inline aggregate argument and the same expression as a named concept must agree. Passes today because both are wrong together; it caught the backed-out guard making them disagree.
- Two traps pinned as must-keep-passing: `test_nullable_key_is_a_value_not_absence` and `test_rollup_subtotal_row_keeps_its_value`.
- In `test_duckdb_partial_key_assembly.py`: strict xfail `test_status_on_extension_rows_is_null_without_an_aggregate` is a target, and `test_composite_grain_families_with_by_span_aggregate` still pins `'LATER'` on its two extension rows, which must flip to `None` with the fix.

What main returns today for the orderless customer, so the scale is clear: `status = 'in-transit'`, `count(status) = 1`, `sum(case when undelivered then 1 else 0 end) = 1`, while `where status = 'in-transit'` EXCLUDES them. Even the aggregate spelling is wrong when the derived concept is a grouping key; the previous handoff's "the aggregate spelling is right" held only for one plan shape.

## What was tried and backed out: a domain guard in the lineage

Build-time rewrite `case when <key> is not null then <expr> end`, the same guard on inline aggregate arguments, and windows partitioned by `key is null` then nulled. It passed the whole oracle with no planner change. It is unsound and was removed (`bdbed7102`; the WIP commits before it hold the code if you want to read it).

`key is not null` cannot witness `~` padding:

- a `?`-bound key is NULL on a real row;
- ROLLUP/CUBE pads KEYS: tpc_ds q05 (`by rollup`) lost its grand-total values;
- a property is never a witness: tpc_ds q70 ranks `store.state` value GROUPS, and pulling `store_sk` in as the "key" produced a fan-out that hung DuckDB for 40 minutes;
- a model-wide gate (any `~` in the environment) failed 36 modeling tests, mostly `generated SQL grew` budgets. A statement-aware gate cannot live in the build: the baseline `BuildEnvironment` is "a pure function of the author environment" and shared across statements, and named concepts take their lineage from it (a flag on the statement's own `Factory` alone left 12 of 20 oracle queries wrong). `build_cache` itself is safe, its key embeds the lineage-hashed canonical name.

Padding is a fact about a JOIN (a source row's presence), not about a column's value.

## Pick up here: evaluate a derivation on its key's own rows

The planner already does this for one shape, which is why it is correct today: `rank order_id by amount desc` computes the window on the orders scan and LEFT joins it back on `order_id`, so the orderless customer gets NULL. The materialized twin has the same shape for everything (`status` read inside the orders scan CTE). Make that the invariant: a derivation keyed on K is computed before its row stream merges with anything K's closure does not cover.

Smallest failing shape, `select customer_id, status` (dump with a `build_group_graph` patch; `PYTHONIOENCODING=utf-8`, ids contain `∅`):

```
grp:root:root:∅                   primary = (customer_id, delivery_date)   -> customers LEFT JOIN orders
grp:basic:d*:local.order_id:sig:… primary = (status,)                      -> CASE over the padded stream
```

One ROOT bucket sources the complete `customer_id` and the BASIC's leaf input together, and the BASIC (grain `order_id`, though `order_id` is never sourced) runs on top. The target is the dim-peel shape: a root cluster keyed by the BASIC's entity carrying its inputs and the `~` join key, the BASIC over that, merged at FINAL with the `customer_id` cluster, which owns the extent.

Where the difficulty is (`v4_helper/group_graph.py`):

1. `_split_root_dimension_clusters` is the only machinery that does this and it is aggregate-only: it returns when `_grouping_keys` is empty and every gate assumes a downstream GROUP BY supplies the FINAL join column.
2. Roots are co-sourced on purpose so the source search finds the connector instead of `ON 1=1` (comment at the top of `build_group_graph`). Every eager root split so far broke something: see memories `project_dim_peel_foreign_key_axis_fix`, `project_filter_only_bridge_root_split`, `project_v4_merged_unnest_bridge_connector`.
3. A new group exposing the span changes `elect_extent_owners`; a predicted owner that diverges from the actual one leaves a contributor dangling at render (`extent_ownership.py` docstring, which also already states the contract: "everything outside the key's closure NULL").
4. A WHERE on a split-off column must still narrow the right rows; the aggregate peel carries four gates for that.
5. Gate it by the MODEL: only a statement that demands a licensed extension span (`spans_demanded_by`) whose key does not determine the derivation. Everything else must plan byte-identically; TPC-DS models use `~` and its SQL budgets are tight.

Aggregates need the argument evaluated on the key's rows (not the rows dropped: `count(order_id) by customer_id` must keep the orderless customer at 0). Inline aggregate arguments never pass `_build_concept`, so they are not concepts the group graph sees; that is why inline and named must be checked together.

Unverified, worth a look: `select customer_id, undelivered_customer` with `undelivered_customer <- filter name where undelivered` returned only customer 1, dropping customers 2 and 3 entirely. Not checked whether that is intended filter-as-row-restriction or a bug.

## Also on this branch

`f8779876f` + `10d90f610`: a constant CASE folding to a non-bool scalar (`auto x <- case when 1 = 1 then 'x' else 'y' end`) left a bare `str` lineage and broke graph generation for every later query. It now folds to `TYPED_CONSTANT` (CONSTANT derivation, rendered inline: a `CONSTANT` operator would bind a `:param` the author-side CASE cannot hydrate). Test: `test_constant_case_folds_to_inline_scalar`.

## Process notes

- `tests/modeling` (`-m "not adventureworks_execution"`) takes about 3.5 minutes. If it runs longer with no new `zquery*.log` written for 5 minutes, it is a runaway SQL execution, not a slow suite: reproduce the test alone under `faulthandler.dump_traceback_later(45, exit=True)`.
- Run pytest `-v` to a log file, never through `| tail`, so a stall is visible.
- Never run two pytest processes at once here. `tests/engine/test_clickhouse_server.py` errors locally without a server.
- Restore files with `git show <rev>:path > path`; never `checkout --` / `reset` / `stash` in this shared tree. `git revert --quit` clears a stuck sequencer without touching the tree.
- A `python - <<'EOF'` heredoc containing triple-quoted strings can break Git Bash quoting; write the script to a file and run it.
