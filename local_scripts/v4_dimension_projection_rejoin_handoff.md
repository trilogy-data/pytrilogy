# Size: dimension projection re-sourced through the fact (q81, q30.alt)

Status: OPEN, `_TPCDS_SIZE` (rows correct, SQL too verbose). Affects `test_eighty_one`
and `test_thirty_alt` — **identical structural fingerprint**, so one fix should clear
both. Not a correctness bug; full v4 sweep is 0 failed.

This is the *dimension* half of the q47 "join-stream" family. The 2026-06-25
join-stream spike fixed aggregate REUSE (the coarser-grain `avg` now reads the
existing aggregate CTE). This issue is the still-open DIMENSION sourcing half.

## Symptom

q81/q30.alt generate ~1.2× the SQL of v3 with nearly double the joins, all from how
the wide output-dimension projection is sourced:

| | CTEs | total JOINs | dim-stream JOINs | GROUPs |
| --- | ---: | ---: | ---: | ---: |
| v3 | 4 | 5 | **1** | 2 |
| v4 | 5 | 9 | **4** | 4 |

Measure: `python -m local_scripts.v4_size_compare` (q81 9163 / ceiling 8000; q30.alt
9006 / 12000 in the proxy — real-test verdict is the gate). Diagnose with
`local_scripts/discovery_v4.py --query 81 --diagnostics --no-sql`.

## Root cause

q81 selects ~16 dimension columns all functionally determined by one key,
`cr.billing_customer.id`: `cr.billing_customer.{text_id,salutation,first_name,
last_name}` + `cr.billing_customer.address.{street_number,…,location_type}`. The plan
also has an aggregate `customer_state = sum(... ?year=2000) by (return_address.state,
billing_customer.id)` and its regrouped `avg`. Both the aggregate and the dimension
projection share the `billing_customer.id` key.

**v3** sources the dimension columns **standalone at customer grain**, then joins on
the key:

```sql
wakeful AS (                       -- one row per customer, 1 join
  SELECT … customer + address cols
  FROM "customer"
  INNER JOIN "customer_address" ON cr_billing_customer_address … )
-- final: aggregate(questionable) ⋈ wakeful ON billing_customer.id
```

**v4** re-derives the same columns **by starting from the fact** and re-joining the
whole graph, producing them at fact grain and then grouping to dedup:

```sql
cooperative AS (                   -- fact grain, 4 joins
  SELECT … customer + address cols
  FROM "catalog_returns"
  INNER JOIN "date_dim"            ON CR_RETURNED_DATE_SK …     -- not in output
  INNER JOIN "customer_address"    ON CR_RETURN_ADDR (return_address)  -- not in output
  INNER JOIN "customer"            ON CR_RETURNING_CUSTOMER …
  INNER JOIN "customer_address"    ON billing_customer.address … )
questionable AS ( SELECT … FROM cooperative )   -- redundant passthrough
young AS ( SELECT … FROM aggregate ⋈ cooperative … GROUP BY … )   -- extra group to dedup
```

So v4:
1. **Re-roots the dimension projection on the fact** (`catalog_returns`) instead of
   sourcing the billing-customer dims from `customer ⋈ customer_address` directly. This
   drags in the fact + `date_dim` + `return_address` join (none of which appear in the
   output) — 4 joins where v3 needs 1.
2. Carries the dims at **fact grain** (many rows per customer) and adds a **GROUP** to
   dedup back to customer grain — work v3 avoids by sourcing at customer grain to begin
   with.
3. Wraps the result in a **redundant passthrough CTE** (`questionable` ← `cooperative`)
   — the passthrough-fold issue (see the q76 handoff / `_fold_passthrough_parents`)
   compounding here.

The dims are FD by `billing_customer.id`, which the aggregate stream already produces,
so the correct plan (v3's) sources them from their own tables keyed by that id and
joins on it — never re-touching the fact.

## Where to look

- **Dimension-group source selection** (the main lever): why does the
  billing_customer dimension projection get a fact-rooted source/bridge instead of a
  standalone `customer ⋈ customer_address` scan keyed by `billing_customer.id`?
  `source_planning.py` (bridge / ROOT datasource choice) and the group-graph ROOT
  bucket for the dimension concepts. The aggregate already exposes
  `billing_customer.id`; the dimension group should source independently and the FINAL
  merge should join on that key.
- **Projection grain**: the dim projection lands at fact grain then groups to dedup —
  related to `_project_dimension_parents_to_group_grain` (see
  `project_v4_q76_dimension_projection_crash`). Here the grain should be the dims'
  own key (`billing_customer.id`), not the fact grain.
- **Passthrough fold**: `_fold_passthrough_parents` should absorb `questionable`
  (single-source projection of `cooperative`); track with the q76 passthrough work.

## Diagnostic starting point

```bash
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query 81 \
  --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
```

Check `<stem>_strategy.md` for the dimension contributor's datasource (expect to see
`catalog_returns` wrongly rooting the dim projection) and `<stem>_groups.md` for the
dimension group's grain and input contract (expect fact grain where it should be
`billing_customer.id`).

## Acceptance

- q81 dimension stream sources from `customer ⋈ customer_address` at customer grain
  (1 join, no fact re-scan), matching v3's `wakeful` shape; total joins ~5, not 9.
- q30.alt improves identically (same fingerprint).
- Rows unchanged; full v4 sweep stays 0 failed; no regression in the join-stream
  aggregate-reuse win (q47/q57) or the dimension-projection crash lock
  (`test_v4_dimension_projection_group.py`).

## 2026-06-25 investigation — refined root cause + why the naive fix regresses q65

A full attempt got q81 to **5 joins / 7934 chars (under 8000) and q30.alt to a clean
`wakeful`** — but it REGRESSED `test_sixty_five` (a passing test). Reverted. The
problem is bigger than "dimension sourcing": it is THREE coupled sub-problems.

**(A) The dims get sourced through the fact because the cover picks the wrong
contributor.** The dim columns are produced as INCIDENTAL ride-through outputs of
the fact-grain virt-filter group (`grp:filter:d*`), and `_cover_groups_for_mandatory`
(strategy_builder) prefers the most-DOWNSTREAM built group, which is that fact-grain
filter — not the standalone `grp:root:root:∅` that declares them. Levers that worked:
  - `_projection_root_concepts` must NOT expand an identifier's `keys` when the
    concept is its own grain key (`billing_customer.id` carries `keys={item.id,
    order_number}` — the fact grain it was a grouping key over; expanding drags the
    fact into a pure dim scan).
  - cover should prefer a group that *declares* the concept (`attrs.output_concepts`)
    over one that only exposes it incidentally in the materialized scan.
  - `_compute_concept_sets`: a non-grouping group should not ride a dimension
    through to FINAL when another group owns it as a primary member — BUT only when
    the riding group is FINER-grained than the dim's own grain (else it splits a
    single natural contributor and 1=1 cross-joins it — cross-datasource subselect).
  - the re-sourced dim contributor must still apply its WHERE filter columns even
    when no output names them (`billing_customer.address.state='GA'` with no address
    column selected — q30.alt). Add the root's condition row-args to `group_concepts`
    gated on `build_fd_determines(preserve_keys, arg)` so only TRUE dimension
    filters join the dim tables, never a fact-grain filter (`return_address.state`).

**(B) The d0-aggregate re-scans the fact (q81 `abundant`).** Even with (A), the
post-condition `customer_state` (d0) keeps a fact-rooted ROOT parent because its
`needed` set pulls the aggregate's raw recompute inputs even though it REUSES its
pre-condition twin (d1). Skipping raw inputs for a reused aggregate + dropping the
redundant ROOT parent (when a sibling GROUPING node at the same grain covers it)
removes `abundant`. This part is correct but leaves no-op GroupNode passthroughs.

**(C) Collapsing the no-op GroupNodes is unsound the easy way.** Making
`gen_aggregate` return `passthrough_if_materialized` skips the GROUP-BY *dedup*,
fanning out any non-unique parent (broke `global_aggregate_filter` + ~ a dozen
tpc_ds). A SAFE collapse must be "GroupNode over a same-grain GroupNode parent
(already unique)" only.

**THE REGRESSION TRAP (q65).** (A) does not generalize. q65's dims
(`item.desc`, `store.name`) are FD by TWO different entities (`item.id`, `store.id`)
that only co-occur in the fact. Routing them to one standalone dim contributor
re-scans the fact to connect the keys (`store_sales` twice). v3's unifying shape:
**dims source ONLY their own tables and join the AGGREGATE on the keys**
(`abundant = wakeful ⋈ item ⋈ store`); the keys come from the aggregate, never
re-scanned. q81 happens to be sourceable standalone (single entity `bc.id` → one
`customer⋈customer_address` cluster) so v3 sources it standalone — but the robust
rule is the q65 one.

**Required robust fix:** dimension routing must be PER-ENTITY / FD-cluster aware —
single-entity FD dims sourceable from a connected dim-table cluster without the
fact may go standalone (q81); multi-entity dims must ride the aggregate's keys,
sourcing only their dim tables (q65). Plus (B) + the SAFE (C) collapse. This is a
substantial, cross-join-risky change to `partition_roots` / cover / source
selection and needs a full v4 sweep diffed against this WIP branch's baseline (16
pre-existing tpc_ds fails — NOT all green; compare net-new only).
