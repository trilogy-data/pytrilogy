# Size: dimension projection re-sourced through the fact (q81, q30.alt)

Status: PARTIAL (2026-06-27). The dimension-split landed; q73 FIXED (xpass,
promoted out of the registry). q10/q81/q30.alt now source dims STANDALONE (rows
correct, no fact re-root) but stay `_TPCDS_SIZE` xfail on the residual redundant
fact-scan feeder (q81 8987>8000; q30.alt 4904 but `web_returns`==2). Zero net-new
regressions across the full tpc_ds sweep AND the engine/tpc_h/optimization/complex
sweep. q65 trap avoided.

## 2026-06-27 landed — the split + its gates (read this first)

Three coupled changes, all principled, all shipped:

1. **`_split_root_dimension_clusters` (group_graph.py)** — the core fix. After
   `_assign_groups`/`_fold_rollup_key_dims`, peel a single-entity FD dimension
   cluster out of the keyed `grp:root:root:∅` bucket into its own
   `grp:root:root:dim:<entity_key>` ROOT bucket. The dims then source standalone
   (`customer ⋈ customer_address`) and FINAL joins on the entity key. FD is
   resolved with `build_fd_determines` against the FULL environment (NOT
   concept_attrs — the intermediate FK `customer.id → current_addr → address.city`
   isn't a concept-graph node, so the side-table closure stops at the name dims).
   FOUR gates, each guards a real regression found empirically:
   - member must be a SELECTED output (else a filter-only dim like q30.alt's
     `address.state` is peeled and the FINAL re-source drops its WHERE — wrong rows);
   - member must NOT be a grouping key (q24 regroups by `customer.first_name,
     store.name` — peeling those breaks the GROUP BY → fan-out);
   - member must NOT be a pre-aggregate filter arg (`_pre_aggregate_filter_args`:
     a WHERE clause with no aggregate term; q98's `category` must narrow the
     class-total window pre-aggregate, not post-join);
   - entity key must NOT sit inside a multi-key non-aggregate filter grain
     (`_finer_filter_grains`; q20's `available_quantity` at `{part.id,supplier.id}`
     needs supplier rows at partsupp grain → peeling strands the condition → crash).
2. **`_projection_root_concepts` keys-skip (strategy_builder.py)** — don't expand
   a self-grained identifier's `keys` (q81's `billing_customer.id` carries
   `keys={item.id,order_number}`, the fact grain it was a grouping key over;
   expanding drags the fact back into the pure dim scan). Property keys (address
   FK) still expand.
3. **(B) twin-reuse drop (strategy_builder.py)** — `_aggregate_reused_from_twin`
   skips the recompute lineage in `needed` when a same-grain grouping twin already
   produced the value; the parent loop then drops the now-redundant ROOT fact
   re-scan (`covers_as_grouping_sibling`). ROOT-only — broadening to FILTER feeders
   drops a row-reducing WHERE the twin doesn't replicate (q30.alt rows).

## Remaining to fully clear q81/q30.alt/q10 (NOT done — unsafe so far)

The standalone-dim sourcing works; the size gap is the SECOND fact scan feeding the
d0 aggregate through `filter:d*` (q81 catalog_returns ×2) and the no-op passthrough
CTEs. The broadened (B) (drop FILTER feeders too) gets q81 to 8275/×1 but BREAKS
q30.alt rows — a FILTER feeder applies a WHERE the grouping twin does not, so the
subset check is unsound for FILTER. A safe version must compare condition atoms
(drop the feeder only when the twin's accumulated conditions ⊇ the feeder's). Plus
the (C) passthrough fold. Both deferred.

---

## ORIGINAL HANDOFF (below) — superseded diagnosis, kept for context

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

## 2026-06-27 investigation — root cause CONFIRMED + code-located + scope

Re-confirmed by instrumentation (q10 and q81 share the SAME mechanism; q73/q30.alt too):

- **`plan_source` in ISOLATION already does the right thing.** Calling `plan_source`
  with `outputs={purchasing_customer.id, demographics.*}` (+ even the full county/gender/
  buyer-subselect conditions) sources cleanly from `customers ⋈ customer_demographics ⋈
  customer_address` — NO fact union, NO `date_dim`. So source-planning is NOT the bug.
- **The bug is the GROUP GRAPH.** q10's single `grp:root:root:∅` declares BOTH the
  customer-grain dims (FD by `purchasing_customer.id`) AND fact-grain feed columns
  (`channel`, `date.month_of_year`, `date.year`) that only the buyer `filter:d1` needs.
  q81 is identical: `grp:root:root:∅` lists the 16 `billing_customer.*` dims (all FD by
  `billing_customer.id`) as ROOT **primary members**, conflated with `item.id`,
  `order_number`, `date.year`, `return_address.state`, `return_amt_inc_tax`. Because the
  dims and the fact-grain columns are ONE group, materialization sources them together
  through the fact, and the demographics/dim consumer can't separate them.
- **Why the existing `_synthetic_dimension_regraft_parent` (group_graph.py:1544) doesn't
  fire:** it only regrafts a `Derivation.BASIC` group whose inputs are all FD on its
  grain key onto a synthetic `grp:root:root:dim:<key>` bucket. In q10/q81 the dims are not
  a BASIC group — they are ROOT **primary members** consumed directly by the FINAL merge.
  So the single-entity FD-cluster machinery exists but doesn't cover the root-member case.

**The fix (core, not yet landed):** split a single-entity FD dimension cluster OUT of the
root group's primary members into its own `grp:root:root:dim:<entity_key>` bucket (entity
key = the id the dims are FD by, which is ALSO a root member and a downstream aggregate's
grouping key, so the FINAL join key is available). Source that bucket standalone
(`customer ⋈ customer_address [⋈ customer_demographics]`) and have FINAL join it on the
entity key. Gate HARD to single-entity FD clusters (q65 trap: multi-entity dims must ride
the aggregate keys, never split). This extends `_synthetic_dimension_regraft_parent` to the
root-member case and is coupled with (B)+(C) below. SUBSTANTIAL group-graph surgery (new
bucket + FINAL contributor + join-key + cover routing) — needs full-sweep diff + q65 guard.

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
