# Keyless-join guard cluster: aliased outputs defeat the root co-source bailout

Filed 2026-08-20 against HEAD c40ef023b, from run
`results/20260820-031800_ingest_deepseek_deepseek-v4-flash` (37 guard firings)
and `..._enriched_...` (q54, 3 firings). Token sinks: q25 burned 2.9M tokens
(7 firings), q84 2.6M (13 firings). Every firing body extracted from the agent
logs reproduces byte-for-byte on clean HEAD via the scoring engine. One root
cause; not fixed here (READ-ONLY probe session).

## Symptom

The `Planner emitted a keyless join between row-bearing sources that share a
join axis` guard (trilogy/core/processing/join_resolution.py:944) fires on
trivially small row-grain probes, e.g. this complete file:

```trilogy
import root.store_returns as sr;

select
    sr.ticket_number as t,
    sr.return_amt as a,
    sr.customer.customer_id as c
limit 3;
```

The agent sees a hard "planner bug" error on a three-column select, concludes
its own query is malformed, and spirals through dozens of rewrites (q84: 13
firings in one session), each burning a full probe cycle. Hence the >500k-token
signatures.

## The trigger is output ALIASING, not the model

| variant of the repro above | result |
|---|---|
| all three outputs aliased (`as t, as a, as c`) | GUARD FIRES |
| no aliases at all | OK, correct FK join |
| only the dim attr aliased | OK |
| only the fact columns aliased | OK |
| aliased, but dim KEY selected instead of attr (`sr.customer.customer_sk`) | OK |
| aliased, dim attr WITHOUT any fact-local key beside it (`return_amt` + `customer_id`) | passes the guard but still renders `FULL JOIN ... on 1=1`, silent cartesian |

Same firing on the enriched curated model (`raw.store_sales` +
`ss.customer.id`), so the ingest models are NOT the cause. The ingest leg
over-represents (37 vs 4) because undocumented models force the agent to write
many row-grain exploration probes, and the eval prompt idiom makes agents alias
every output. The corpus has zero footprint because corpus queries select bare
concepts, which take the safe path below.

The rowset shapes (q05, q25 final, q54) reduce to the same trigger. Minimal:

```trilogy
import root.store_sales as ss;
import root.store_returns as sr;
import root.item as item;

rowset ss_rows <- select ss.ticket_number as ticket, ss.item.item_sk as item_sk, ss.net_profit as net_profit;
rowset sr_rows <- select sr.ticket_number as ticket, sr.item.item_sk as item_sk, sr.net_loss as net_loss;
rowset matched <- select ss_rows.ticket as ticket, ss_rows.item_sk as item_sk, ss_rows.net_profit as a, sr_rows.net_loss as b
union join ss_rows.ticket = sr_rows.ticket and ss_rows.item_sk = sr_rows.item_sk;

select matched.ticket as ticket, matched.item_sk as item_sk, item.item_id as item_id
subset join matched.item_sk = item.item_sk
limit 3;
```

FIRES, despite the authored `subset join` naming the axis. Drop the
`as item_id` alias (3-arm variant verified) and it plans. Even a single-fact
rowset plus one subset-joined dim fires when the finals are aliased.

## Root cause

`_cosource_component_groups`, trilogy/core/processing/v4_helper/group_rules.py:524
(called from `partition_roots`, group_rules.py:660). Two interacting defects:

1. **The zero-reach bailout keys on the wrong signal** (group_rules.py:552-556,
   `can_split = bool(main_items) and all(reaches)`). The conservative
   one-bucket fallback engages only when some root has zero forward lineage
   reach. A bare `select sr.ticket_number` output has zero reach, so bare
   selects stay in one bucket and plan correctly. `select sr.ticket_number as
   t` mints a BASIC rename, giving the root a reach of exactly `{local.t}`.
   With every output aliased, every root has non-empty reach, the bailout is
   skipped, and splitting proceeds. Instrumented proof on the repro:
   - no aliases: `reaches: [(sr.ticket_number, []), (sr.return_amt, []), (sr.customer.customer_id, [])]` -> single bucket -> OK
   - aliased: `reaches: [... ['local.t']], [... ['local.a']], [... ['local.c']]` -> split path -> guard

2. **The output-convergence connectivity test cannot see one-FK-hop
   relatedness** (group_rules.py:591-654). Roots that converge at the SELECT
   projection are co-sourced only when weakly connected in
   `concept_graph.to_undirected()` patched with (a) property-to-key edges,
   added only when the KEY is itself a requested root (node_by_addr /
   node_by_pseudonym over `main_items`, :600-615), and (b) shared
   `datasource_bindings` edges between PROPERTY roots (:631-643).
   `sr.customer.customer_id` (PROPERTY, keys={sr.customer.customer_sk}, bound
   only on the customer table) has neither: its FK `customer_sk` is bound on
   the fact and FD-determined by the fact grain, but it is not requested, so
   no edge exists. Instrumented neighbors on the repro:
   `sr.ticket_number: [local.t, sr.return_amt]`,
   `sr.return_amt: [local.a, sr.ticket_number]`,
   `sr.customer.customer_id: [local.c]` (an island).

The dim attribute then lands in its own `split:` bucket
(group_rules.py:747), each bucket is emitted as an independent
GEN_ROOT_MERGE request holding only its member concepts (debug trace:
`normals: [sr.return_amt, sr.ticket_number]` and separately
`normals: [sr.customer.customer_id]`), each side is group-wrapped to exactly
its outputs, dropping the FK, and the FINAL merge has no shared column. The
guard correctly refuses. The rowset/subset-join shape is the same loss at the
same site: the aliased dim attr forms a root bucket whose request never
includes the authored join key, and the rowset island is not visible to the
co-source test at all.

Per the repo doctrine, the fix belongs in this demand construction (co-source
connectivity should follow the concept `keys` closure through unrequested
concepts and datasource bindings, or split buckets must demand their
inter-island connector axes), never in late key re-injection and never by
relaxing the guard.

## Not a recent regression; the guard converted silent wrong rows into errors

PYTHONPATH-shadow A/B of the repro across clean checkouts:

| commit | result |
|---|---|
| a6161b981 (08-08, v4 default flip) .. 0e6c33f2e (08-15) | "plans", but renders `FULL JOIN ... on 1=1` with each side GROUPed to its own outputs: silent cartesian of distinct (t,a) x distinct customer_id |
| a65b13c9c (#645, 08-16) .. HEAD c40ef023b | guard fires (hard error) |

So #645's FD-aware guard is doing its job; the wrong plan predates it. The
no-fact-key variant (`return_amt` + `customer_id`, aliased) still ships the
silent cartesian TODAY because neither side carries the axis even in FD
closure, so the guard cannot see the pair. Any fix should be validated against
that variant too.

## Affected queries / firings (all reproduce on HEAD)

Counts are guard firings found in the agent logs (extraction script below);
the run-report counts in the task differ slightly because some firings share a
tool call with other errors.

- ingest q84: 13 (fact key + dim attr probes; incl. 2-hop `cu.household_demographics.income_band.*` and `sr.customer.customer_demographics.demo_sk`)
- ingest q25: 7 (6 probes + the 3-arm union-join final)
- ingest q05: 5 (union-join rowsets + `ws.web_site.site_id` dim attr)
- ingest q54: 3, enriched q54: 3 (customer address attrs beside fact keys)
- ingest q64: 2, q67: 1, q80: 1

All firing bodies, repro/minimization scripts, the instrumented tree, and
workspace copies live in the session scratchpad under `probe_keyless/`
(`bodies/`, `bodies2/`, `repro.py`, `classify*.py`, `min91*.py`,
`aliastest*.py`, `tree_instr/`). Engine harness used:
`evals/common/scoring.make_scoring_engine(ws/'warehouse.duckdb', ws, 'tpcds')`
against a COPY of the run workspace (DuckDB file locks).

Cross-reference: `bug_q05_where_breaks_cross_model_aggregates.md` mentions the
guard for a different (WHERE + cross-model aggregate) shape; the closed
`bug_keyless_join_axis_lost_guard_fires.md` shapes (q04 pivot, q05
rollup-union) are distinct and stay fixed, verified by their regression tests'
shapes not overlapping these repros.

## Update 2026-08-20 (run 20260820-153007): first graded WRONG ANSWER from this cluster

`enriched_docs` q81 (915k raw / 182k cache-adjusted, 18 turns) is the same
root cause and escalates the severity: previously the cluster only burned
tokens; here it converted a would-have-passed query into a graded FAIL.

- The agent's FIRST write was the clean nested-aggregate formulation
  (pinned `sum(...) by customer, state`, `avg(...) by state`, threshold in
  WHERE) with every output aliased per the inlined docs' "alias every new
  expression with `as`" instruction. The guard fired (3 answer attempts +
  2 of 7 isolation probes = 5 firings). A de-aliased but otherwise
  byte-identical twin plans AND returns rows exactly equal to the TPC-DS
  reference on the current tree, so the rejected query was correct.
- Forced into a rowset workaround, the agent introduced
  `cs.return_customer.sk is not null` into the state-average rowset's WHERE.
  That predicate is pushed into the aggregate population, drops the
  NULL-customer groups the reference includes in the per-state average,
  lowers one state's 1.2x threshold, and admits exactly one extra customer
  (limit-100 window then sheds the last reference row). Graded
  "result set differs from reference" while the sibling `enriched` leg
  passed the identical question in 6 turns with the same formulation, bare
  refs, zero firings.
- New trigger data point, consistent with defect 1 (the `all(reaches)`
  bailout): with a pinned-aggregate select over two dim namespaces
  (`return_customer` attrs + `return_customer.current_address` attrs), the
  guard fires only when BOTH attr groups are aliased. Stripping either
  group's aliases leaves zero-reach bare roots, the one-bucket bailout
  engages, and the query plans.
- Amplifier: the `enriched_docs` category inlines the language reference,
  which instructs "alias every new expression with `as`" (and "Alias every
  reused expression" for rowsets). Agents in that category systematically
  produce the alias-everything style this bug is keyed on, so the docs leg
  is disproportionately exposed.

Firing bodies, the bisect variants, and the scoring-engine repro live in the
2026-08-20 triage scratchpad under `triage_q79_q81/` (`q81_attempt1.preql`,
`q81_probe4.preql`, `q81_variant_A/B.preql`, `run_diff.py`). All reproduce on
working tree fb75a7182 via `make_scoring_engine` against a copy of the run
workspace.
