# TPC-DS enriched churn diagnosis — 2026-07-15

Run: `results/20260715-153056_enriched` (in progress at diagnosis; targets q05–q77 all complete).
Method: one read-only probe per >500k sink (reproduce via `generate_sql`/`score_query`, trigger matrix,
classify A=framework / B=affordance / C=question / D=agent-idiom). 11 queries probed.

## Verdict table

| q | tokens | status | bucket | one-line |
|---|--------|--------|--------|----------|
| 05 | 1.22M | FAIL | **A** | `0::float` union placeholder → REAL(4-byte) → silent money drift; `0.0` fixes. Known-open no-DOUBLE. |
| 54 | 1.63M | PASS | D+B | **NOT a framework bug** (corrected): `concept ? pred` is a per-row filter (CASE..ELSE NULL), not a scalar; `auto` inlines it, so `month_seq >= (month_seq?Dec98)+1` = self-referential `x>=x+1` → 0 rows correctly. Idiom = aggregate it (`max/min`). Agent used a filter where it needed an aggregate. |
| 14 | 833k | FAIL | B | `intersect()` NULL=NULL identity qualifies NULL keys → rollup subtotal rows collide → non-unique keys. Codegen correct; needs `is not null` guards. No diagnostic to catch it. |
| 16 | 876k | FAIL | D+B | agent baked core filters into order-constraint rowset (question said not to). B: scope-diagnostic drops inner WHERE/HAVING of membership subquery → looked like no-op self-membership. Not a regression (repro'd ref on HEAD). |
| 17 | 644k | FAIL | D | ignored purpose-built `catalog_store_returns` model; hand-rolled raw store-returns join (customer-agnostic) → 6 spurious rows. |
| 29 | 1.01M | FAIL | D+B | reached correct 1-row answer, rejected it as "too restrictive", submitted wrong 15-row union-join. B: agent lacked a compact intersection idiom (steered to full-outer `union join` default); prompt's "0 rows = issue" backfires on low-row results. |
| 41 | 625k | FAIL | **C** | question word "**other**" → agent added self-exclusion → 638 vs 680. Drop "other". |
| 64 | 527k | PASS | D | false-positive: 514k prompt / 12k completion, wide 10-model discovery re-sent per turn. Perf cliff didn't bite; prior fix held. |
| 66 | 1.18M | FAIL** | D | churn = nested macro missing `@` sigil + NULL-combine thrash. **FAIL is a scoring false-negative** (see harness bug). |
| 76 | 817k | PASS | D | false-positive: 508/508 multiset-identical to ref; 16 unfiltered explore dumps. |
| 77 | 982k | PASS | D | false-positive: 43/43 exact; explore-v2 verbosity + no caching. |

\** q66 "fails" only because of the scoring carve-out below; its computed answer is correct.

## A — framework bugs (1 confirmed)

1. **q05 float32 union-placeholder drift** — `bug_q05_float32_union_placeholder_drift.md`.
   `DataType.FLOAT`→DuckDB `REAL` (4-byte), `trilogy/dialect/base.py:359`. `SUM()` over `0::float`
   placeholders drifts money sums (7/100 rows, $0.79). Trigger: `0::float`→FAIL, `0.0`→PASS.
   Re-confirms known-open `project_float32_union_placeholder_drift_no_double_type`. (Even here the eval
   impact is partly the scoring carve-out below — some drift "FAILs" are scoring, not correctness.)

**q54 is NOT a framework bug** (I twice mis-filed it; corrected). Two correct-behavior pitfalls drove its
churn: (a) bare `concept ? predicate` is a per-row filter (`CASE..ELSE NULL`), not a scalar, so a bound over
the same concept expands to `x>=x+1` → 0 rows; (b) bare `max(...)` is **responsive-grain** — in
`select key, … where …max…` it resolves as `max BY key`, which correctly disconnects from an unrelated table.
The broadcast idiom is `max(expr) by *` (persistently grainless — resolves + cross-joins everywhere, verified),
or aggregate off a connected role. Real issue = agent idiom (D/B). See `bug_q54_grainless_scalar_filter_bound.md`.

## Eval-harness bugs (high leverage — affect the metric itself)

- **Scoring false-negative — `evals/common/scoring.py:332-333`.** The exact-integer carve-out
  (`if v == int(v): return float(int(v))  # keep precise`) keeps a whole-dollar reference cell at full
  precision while the drifted candidate is `_sig_round`-ed, so a correct-to-8-sig-figs money sum ≥10⁶
  false-fails (q66). **Fix:** sig-round both sides symmetrically (or apply the same relative tolerance to
  both) so the carve-out doesn't defeat the float32 tolerance it sits next to. This also masks how often
  the no-DOUBLE drift is *harmless* — some "FAILs" are scoring, not correctness.

- **Token metric ignores prompt caching.** Per-turn usage records only `prompt/completion/total_tokens`;
  DeepSeek's `prompt_cache_hit_tokens`/`miss` are never captured. Reported per-query tokens =
  Σ(full prompt each turn), which grows monotonically and is re-billed uncached every turn (q66:
  1.16M prompt / 87k final context / 13.5k completion). **This inflates the >500k detector** for
  wide-discovery queries (q64/q76/q77 are clean passes flagged only by this artifact). **Fix:** capture
  the cache fields so "tokens" reflects real billed cost; token-minimization is an explicit eval goal.

- **Prompt line "A zero-row result means the query has an issue" backfires** (q29): nudged the agent to
  reject a correct low-cardinality answer. Consider softening for legitimate multi-fact intersections.

## B — cross-cutting affordance themes

**Theme 1 — no self-verification diagnostic → confident silent wrong answers** (q14, q16, q29, q17):
the agent produced/《saw》a malformed result, was even suspicious (q14, q29), but had no tool to confirm
the malformation and rationalized submitting.
  - q14: emit a diagnostic when `by rollup/cube/grouping sets(...)` groups over a dimension with genuine
    NULLs (subtotal rows collide with NULL leaf rows → non-unique output keys); suggest `is not null` /
    `grouping(X)`.
  - q16: scope-diagnostic renderer drops a membership subquery's inner WHERE/HAVING — include it in
    `input_row_filters`/`normalized_input_row_filters` instead of collapsing to a bare column ref.
  - q29: the agent needed a compact fact-intersection idiom. NOTE: do NOT recommend `left join` — `left`
    is a DEPRECATED alias for `subset join` (the canonical `query29.preql:28-29` still uses the old
    `left join` spelling, worth updating). The current idioms are `subset join` and the `is not null`
    membership the agent actually used; document one clearly and note low counts are legitimate for
    multi-fact intersections (the agent had the right 1-row answer and threw it away).

**Theme 2 — discovery verbosity × no caching → token burn** (q64, q76, q77; contributes to q05/q54/q14/q66):
unfiltered `explore` on the consolidated mega-fact models (`store/web/catalog/all_sales`) emits full
grouped decls + imported dims (explore-v2 ~2x), re-sent every turn. Agents re-dump the same models 5x
instead of `--regex`. Levers: more compact default `explore` output for wide models; capture caching so
the metric stops rewarding narrow-discovery only.

**Purpose-built consolidated models are being ignored** (q17, q29): `catalog_store_returns` (customer-inclusive
`is_returned`) is exactly the right tool and is described in the file list, yet agents hand-roll the raw joins.
Consider surfacing these bespoke models more prominently (naming/description or an agent-info hint).

## C — question defects

- **q41**: drop the word "**other**" from question 1663418777 (reference counts the item itself).
- **q17** (minor): question doesn't state the returner must be the buyer; the bespoke model assumes it.

## Not investigated
q23 (known question-interpretation, per user), q80 (still running at diagnosis time),
passing-friction q51/q56/q58/q59 (same discovery-verbosity class expected).
