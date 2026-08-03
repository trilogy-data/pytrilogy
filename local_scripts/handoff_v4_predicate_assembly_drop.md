# RESOLVED 2026-07-30 (s52) — v4 dropped a WHERE atom during plan assembly

Status: **fixed and gated.** Registry back to 0 entries. Kept as the record of
the mechanism, because the fix generalizes to any atom placed above a ROOT
group whose condition names a derived concept.

## The defect

```
import all_sales as sales;
auto total <- sum(sales.ext_list_price) by sales.billing_customer.sk;
where sales.billing_customer.sk is not null and total > 0
select sales.billing_customer.id order by sales.billing_customer.id asc nulls first;
```

v3 returned 98,992 rows, v4 **98,993** — the extra one is the NULL-key group,
surfacing as an all-NULL output row.

## Root cause — it is not assembly, it is a re-plan

The previous handoff's decisive fact was right (the conditioned nodes are built
and then discarded) but the inference from it was wrong: nothing *substitutes*
the node during assembly. The conditioned node is never consulted at all.

`gen_root` sources from datasources, **not from the `parents` the group graph
hands it** — that is by design. So when a ROOT group's condition names a
*derived* row arg (`total > 0`), `plan_source` cannot bind it, and `gen_root`
falls into its fallback path: source the plain outputs, then hand the condition
to `_resolve_root_condition_sources`, which re-plans the derived arg through a
fresh `search_concepts`.

That sub-search was called with `conditions=[]`. The atoms the ancestor groups
applied are, by construction, absent from the rows it plans — and passing no
conditions meant nothing re-applied them. The aggregate came back unfiltered,
the customer dimension was reached through a LEFT OUTER JOIN that cannot
enforce the predicate either, and the NULL group survived.

The walker already computes exactly the right value and threads it as
`preexisting_conditions` (`_accumulated_atoms_above`), but `dispatch.build_node`
did not forward it to ROOT — only non-ROOT generators received it, where it
means "already applied upstream, don't re-emit". For ROOT, which re-sources, it
means the opposite: **not** applied, so apply it.

## The fix

- `v4_node_generators/dispatch.py` — ROOT gets its own dispatch branch that
  forwards `preexisting_conditions`.
- `v4_node_generators/root.py` — `gen_root` accepts it and passes it to
  `_resolve_root_condition_sources`, which feeds the **inheritable subset** to
  both condition-source sub-searches (the correlated one and the retry).

### Not every ancestor atom may be inherited — this is the subtle half

Forwarding the whole clause is WRONG and breaks
`tests/test_where_select_dual_scope.py::
test_population_only_refs_share_sensitivity_cache`. Given

```
auto sx <- sum(z) by x;
where f = 1 and sx > 5 select x;
```

`sx` is population-only, so it must be computed over the UNFILTERED rows.
Inheriting `f = 1` narrows its input and x=1 drops (sum 12 → 2). That is exactly
the scope narrowing the population/select dual-scope split exists to prevent.

The distinction is semantic, not incidental:

- An atom over **the request's own concepts** — the derived args plus their
  grain keys — selects which GROUPS exist and cannot change any group's value.
  Safe, and required. (`billing_customer.sk is not null` on
  `sum(...) by billing_customer.sk`.)
- An atom over **any other row column** changes each group's value. Must not be
  inherited. (`f = 1` on `sum(z) by x`; q11's `channel` / `sale_date.year`.)

`_inheritable_atoms` decomposes the clause and keeps only atoms whose row args
are all in the sub-search's own mandatory list (`row_search + correlation`,
pseudonyms included). Existence-bearing atoms are excluded — their feeders are
wired separately.

## Blast radius — one query

`v4_sql_snapshot.py check`: 108 identical, **1 changed — q11**, the corpus
instance of this very bug.

- `sales.billing_customer.sk is not null` now renders in all three union arms.
- The customer join upgrades LEFT OUTER → INNER, matching v3.
- q11's `channel in (...)` / `year in (...)` are deliberately NOT inherited (see
  above). An earlier over-broad cut of this fix did apply them, which pruned the
  dead `catalog_sales` arm and pushed date_dim into the arms — tempting, but it
  is the same rule that breaks dual-scope, so it is gone. Those two atoms were
  already triaged as semantically redundant; not applying them costs nothing but
  a missed pruning opportunity.

Goldens refreshed; `check` is back to 109/109.

q11 verified structurally, not just by luck of the data. Re-running the
handoff's doctored-data experiment (a NULL-billing-customer row large enough to
clear both `> 0` gates *and* the ratio filter — note `DECIMAL(7,2)` caps at
99999.99, so use 100.00/90000.00 across the two years):

| | before | after |
|---|---|---|
| v3 | 90 rows, 0 all-NULL | 90 rows, 0 all-NULL |
| v4 | **91 rows, 1 all-NULL** | 90 rows, 0 all-NULL |

## Gates — all green

1. `v4_sql_snapshot.py check` — 109/109 after the classified q11 refresh.
2. `pytest tests/modeling/tpc_ds_duckdb/test_queries.py` — 107 passed.
3. `pytest tests/join_matrix tests/engine tests/core/processing
   tests/modeling/join_resolution tests/test_scoped_join.py` — 1470 passed /
   92 skipped / 5 xfailed / 11 xpassed. The xfail/xpass counts match the
   baseline exactly. Passed went 1454 → 1466 before my tests: that is the 12
   test *methods* added by commit `f4bda74c3`, not a behavior change (the
   handoff's 1454 predates them).
4. `pytest tests/modeling/tpc_h` — 29 passed.
5. `pytest tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py` — 37
   passed (whole file, not a `-k` selection, per the order-dependence trap).
6. `mypy trilogy` clean, `ruff --select E,F,I` clean, `black` clean.
7. **Full suite** (`-m "not adventureworks_execution"`, split into thirds to
   stay under the 10-minute background cap). Worth doing for anything touching
   ROOT condition sourcing — gate 3 alone would have shipped the over-broad cut.

## Regression guards

- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::
  test_not_null_on_aggregate_grain_key_is_enforced` — the row-level guard;
  un-xfailed (confirmed XPASS before the registry entry was deleted).
- `tests/core/processing/test_v4_root_condition_source_inherits.py` — new, ~3s,
  no database. Pins the atom rendering, the INNER upgrade, the sub-search
  actually receiving the inherited clause, the `gen_root` signature, and — read
  off q11 — the *upper* bound: date_dim must stay a single join above the union,
  which is what fails if the inheritance rule goes over-broad again. Every
  behavioral assert was confirmed to flip red by disabling the relevant half of
  the fix at runtime; none are vacuous.
- `tests/test_where_select_dual_scope.py::
  test_population_only_refs_share_sensitivity_cache` is the guard that caught
  the over-broad cut. It is NOT in the handoff's listed gate set — run it (or
  the whole non-modeling suite) for any change in this area.

## Re-sweep results

`local_scripts/v4_predicate_audit.py tpch tpcds` — **4 suspects → 1**:

- tpcds q11 not-null — **fixed**.
- tpcds q11 `channel in (...)` / `year in (...)` — still no-ops, and correctly
  so: inheriting them would narrow the aggregates' input (see the dual-scope
  section). They remain semantically redundant, so this is a missed pruning
  opportunity, not a defect. **Do not "fix" these** without solving the
  dual-scope interaction first.
- tpch q18 `order.id is not null` — the surviving suspect, and still NOT a bug:
  the plan's `INNER JOIN ... ON order_id = order_id` cannot match NULL. Fragile
  rather than wrong; if that join ever becomes LEFT, nothing enforces it.

Also: 4 redundant-under-both, 1 v4-stricter, 0 errors.

**The suggestion to extend the sweep to `tests/modeling/gcat`, `faa`, and
`the_look` is a dead end — don't spend a session on it.** Those directories are
pure model definitions: across all 25 `.preql` files there are **zero**
WHERE-bearing top-level statements. Note the audit only globs
`query*.preql`/`adhoc*.preql`, so running it there prints a clean `0 suspects`
summary that means "nothing was examined", not "nothing was wrong". Widening the
glob to `*.preql` (done, throwaway copy) confirms 0 statements to audit.

## Follow-up worth considering (not done)

`gen_root` re-planning a derived condition arg from scratch duplicates work the
group graph already did — the conditioned aggregate node it built is discarded
and an equivalent one is searched for again. Correctness is now right either
way, but teaching `gen_root` to *reuse* the matching `parents` node would cut a
redundant search. Filed as an idea only; it touches the ROOT/parents contract
and would need its own snapshot pass.

## The other open threads, for context

- `handoff_v4_shape_debt.md` — the passthrough / split-aggregate coupling.
  Biggest measured size win, multi-session, touches v3-shared optimizer code.
- `handoff_v4_search_cost.md` — A1/A4 and the test gaps are DONE (search 125 s
  → 18 s). Branch-and-bound is still open, with a corrected diagnosis: q05's
  4,096 covers reduce to exactly one source set, so a cost lower bound would
  prune nothing; the real target is arm-vs-union branching.
