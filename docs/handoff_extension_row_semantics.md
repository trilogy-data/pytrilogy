# Handoff: a derived concept lives on its key domain (extension-row semantics)

Session of 2026-09-20, branch `extension-row-null-semantics` (off `main` at #700). The rule is decided and the oracle exists. Two implementation routes were tried and backed out: a guard in the lineage, and a planner-side prototype that worked for one shape behind gates. The way forward is `docs/keyspace_phase_plan.md`; this file is the record of what the two attempts found.

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

- `HOLDS`: pass today. `OWED`: strict xfail. A fix shows up as XPASS; move the query to `HOLDS`.
- `test_inline_spelling_matches_named`: an inline aggregate argument and the same expression as a named concept must agree. It caught the backed-out guard making them disagree.
- Three traps pinned as must-keep-passing: `test_nullable_key_is_a_value_not_absence`, `test_rollup_subtotal_row_keeps_its_value` and `test_present_entity_with_an_unbound_property_still_evaluates`.
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

## What was tried and backed out, second: a span domain bucket in the group graph

Commits `e0f6424b7`..`d1c482137` hold the code; the commit after them removes it. It passed 19 of the oracle's 20 queries with every suite green, and was backed out anyway: it only fires when exactly one span is demanded AND the statement projects the span key AND every WHERE input is deliverable, so `select name, customer_id, status` returned NULL for the orderless customer while `select name, status` returned `'in-transit'`. Dropping a key column changed another column's value. That boundary is harder to explain than main's consistent wrong answer, and each gate existed because the domain was bolted on after grouping. What it found is the requirements list for the keyspace phase.

How it worked. Padding happens because one ROOT bucket sources the complete span key beside a derivation's inputs, and the extent owner's ancestors may all pad. The prototype duplicated the span's own members into a second ROOT bucket, the **span domain** (`group_graph._add_span_domain_buckets`, id `grp:root:root:∅:extent:<span>`, `GroupAttrs.extent_span`), modelled on the `root_d1` duplication:

```
grp:root:root:∅                          (customer_id, delivery_date)  extent-free: orders rows only
grp:basic:d*:…                           (status)                      over the solid stream
grp:root:root:∅:extent:local.customer_id (customer_id[, name])         the owner: customers
FINAL                                    customers LEFT JOIN status-stream ON customer_id
```

- **Election** (`extent_ownership.elect_extent_owners`): a domain always owns its span. `permitted` is the domain plus its descendants, minus `_solid_groups` (every row-stream derivation the span does not determine, and the ROOT / row-stream groups feeding it). `ExtentOwnership.carried` makes FINAL read every domain member (`name`) from the domain, not only the key.
- **FINAL**: the domain preserves only its span as a join key (`_refresh_final_contract`), otherwise it is re-sourced through the fact at the merge grain. It is never folded into a sibling that can render its columns (`_fold_passthrough_parents(keep=)`, `_drop_ancestor_parents(keep=)`): its contribution is ROWS.
- **Aggregates stay extent-free**, they do not read the domain. They inline their BASIC arguments from the root, so padding inside one re-evaluates `coalesce(amount, 0)` on the padded row. Extent-free plus the FINAL join is equivalent (NULL for sum, the existing count zero-fill for count) and is what makes INLINE aggregate arguments agree with named ones. A scalar over an aggregate by the span (`activity`) is the exception: it is keyed on the span, so it gets the domain as a parent (`_feed_span_domains_to_on_span_scalars`) and its ELSE fires for the orderless customer.
- **WHERE**, three deliveries, anything else gets no domain (`_filters_span_domain`; an undelivered atom is a silently lost filter, tpc_ds q95's `where eligible_order` found that):
  - a column the domain carries (`name = 'cat'`) filters the domain itself;
  - a null-rejecting atom over an off-span derivation pin-healed the `~` (`partial_bridging._proven_bound`: `status = 'delivered'` proves an order exists through the derivation's keys). Only for derivations that never cross an aggregate (`projection.reads_rows_only`): `count(...) > 0` is false on an extension row, not NULL;
  - a null-accepting atom over a projected off-span value (`status is null`) is hosted at FINAL ONLY (`PlacementReason.FINAL_SPAN_DOMAIN`), over the extended rows.
- **Gates** (everything else planned byte-identically): exactly one demanded span, projected itself; a row-stream reader (or inline aggregate argument) whose key entity is ABSENT on the span's extension rows (`extent_ownership.absent_on_extension`) and that is not already NULL on padding (`_null_on_padding`: plain arithmetic, a `?` filter over an off-span content; CASE / COALESCE / IS NULL / CONCAT / a window are the null-opaque ones). The pass also runs in WHERE-phase sub-plans, which is where tpc_h q20 and tpc_ds q73/q79/q97 tripped their SQL budgets before the NULL-propagation gate.

"The span does not determine it" is the WRONG test for absence, and the first cut used it. tpc_ds q94's shape (`tests/optimization/test_join_upgrade.py`, now also a trap in the oracle): `returns` binds `~order_id, ~item_id` as its own grain and `is_returned <- _ret_order is not null` is keyed on `(order_id, item_id)`. A line with no return still HAS that entity, from `lines`; `_ret_order` is a NULL value on a real row and `is_returned = false` is right. An extension row comes from a source binding the span completely and carries whatever that source binds beside it, so a key is absent only when no such source binds it (`customers` has no `order_id`).

## Pick up here

`docs/keyspace_phase_plan.md`. Its phase 0 is reading the parts of discovery neither attempt read.

Still true, whichever route is taken:

- KEPT from the prototype session: `predicate_pushdown._predicate_safe_past_null_extension` hand-rolled its outer-join check and missed a RIGHT/FULL join whose left is implicit (`left_cte` unset), so a null-accepting predicate could be copied below the null-supplying side. It now asks `null_padded_nodes` like its neighbours. No committed SQL log moves. (The prototype itself narrowed one tpc_ds q05 `date_dim` lookup from FULL to LEFT with matching rows; that went away with it, so it came from the planner change, most likely the pin-heal extension, not from this fix.)
- VERIFIED, pre-existing, not touched: `undelivered_customer <- filter name where undelivered`, `select customer_id, undelivered_customer` returns only `(1, 'ann')` on the materialized twin too. A FILTER in the SELECT restricts the row stream.
- Pre-existing crash: `select customer_id, status where activity = 'dormant'` fails to render (`Missing source map entry for local.order_id`).
- `select customer_id, status where customer_id in (2, 3)` returns only `(2, 'delivered')` on BOTH oracle models: the key filter lands on the fact side. Looks wrong under the rule, not investigated.

## Also on this branch

`f8779876f` + `10d90f610`: a constant CASE folding to a non-bool scalar (`auto x <- case when 1 = 1 then 'x' else 'y' end`) left a bare `str` lineage and broke graph generation for every later query. It now folds to `TYPED_CONSTANT` (CONSTANT derivation, rendered inline: a `CONSTANT` operator would bind a `:param` the author-side CASE cannot hydrate). Test: `test_constant_case_folds_to_inline_scalar`.

## Process notes

- `tests/modeling` (`-m "not adventureworks_execution"`) takes about 3.5 minutes. If it runs longer with no new `zquery*.log` written for 5 minutes, it is a runaway SQL execution, not a slow suite: reproduce the test alone under `faulthandler.dump_traceback_later(45, exit=True)`.
- Run pytest `-v` to a log file, never through `| tail`, so a stall is visible.
- Never run two pytest processes at once here. `tests/engine/test_clickhouse_server.py` errors locally without a server.
- Restore files with `git show <rev>:path > path`; never `checkout --` / `reset` / `stash` in this shared tree. `git revert --quit` clears a stuck sequencer without touching the tree.
- A `python - <<'EOF'` heredoc containing triple-quoted strings can break Git Bash quoting; write the script to a file and run it.
