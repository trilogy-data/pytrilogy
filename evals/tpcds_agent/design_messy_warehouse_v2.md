# Design: messy-warehouse eval v2 — confusable noise, replicates, cache-aware tokens

**For an implementing agent.** Follows
[`handoff_messy_warehouse_first20.md`](handoff_messy_warehouse_first20.md) and
the token decomposition of run `20260810-211903`. The v1 conclusion this
design responds to: the treatment was constructed correctly, but the
experiment as shaped **cannot** detect the hypothesized effect.

## 1. Why v1 could not show a SQL degradation

Token cost decomposes as (validated to a few percent on all four v1 cells):

```
T ≈ N·P₁ + Σᵢ sᵢ·(N − tᵢ)/r + Σⱼ cⱼ·(N−1−j)
```

N = LLM turns, P₁ = initial prompt, sᵢ = a tool payload injected at turn tᵢ
(re-billed on every later turn), cⱼ = completions, r = chars/token (~2.6 for
SQL payloads, ~3.5 for JSON explore payloads).

For the SQL path the schema payload S enters once at turn ~2, so each schema
char costs (N−2)/r ≈ **2.7 tokens per query**. The v1 noise treatment added
2,604 chars → predicted **+5k tokens/query ≈ +5% per cell** — *if the agent's
behavior is otherwise unchanged*. It was unchanged: the 12 noise tables are
obviously irrelevant (HR, fleet, payroll), the model never audited them, and
only 1 of 40 SQL candidates ever touched even the aggregate tables. Meanwhile
the enriched control pair — byte-identical agent-visible inputs — swung 18.6%
(1.42M tokens) on run variance alone. A +5% predicted effect under a ±20%
noise floor is undetectable by construction.

Corollary for framing: the hypothesis "SQL cost grows with messiness while
Trilogy stays flat" is about the **slope** (2.7 tokens/schema-char/query vs
~0 for enriched — enriched explore payloads were byte-identical across noise
cells, which v1 did validate). At current overheads the enriched intercept is
~280k tokens/query higher, so the naive break-even needs the schema to grow
~6x (~200+ clean tables). Clean-table noise moves the slope term only. To
show degradation at realistic scale, the noise must change agent
**behavior** — force auditing turns (each worth a full context re-charge) or
wrong table choices (accuracy).

## 2. Treatment redesign: confusable noise

Replace (or add as a third arm to) the unrelated-domain tables with noise that
a diligent analyst must actually disambiguate. All variants remain answerable
correctly — the reference results stay valid — but the wrong choice produces
wrong numbers, not errors:

1. **Stale near-duplicates**: `fact_store_sales_v2_bak`, `fact_store_sales_2023`
   — same schema as the real fact, 70–90% row subset (e.g. missing the last
   date quarter). Picking them yields plausibly wrong aggregates.
2. **Grain traps**: `fact_store_sales_daily` named like a line-grain fact but
   actually pre-aggregated (the v1 `fact_agg_*` tables renamed to drop the
   `agg` marker). Naming no longer discloses the grain; the agent must check.
3. **Ambiguous dimensions**: `dim_customer_current` vs `dim_customer` (SCD
   full history) — the classic pick-the-wrong-one for point-in-time questions.
4. **Misleading columns**: a `net_profit` column on a near-duplicate that is
   actually gross value (matches the real warehouse's documented
   `profit`-vs-`net_profit` distinction, physically materialized).
5. Keep 3–5 unrelated-domain tables as the "cheap chaff" floor — they proved
   inert in v1, which is itself worth confirming at higher counts.

Grade the dose: cells at +0, +6, +12, +24 confusable tables. The hypothesis
predicts a monotone SQL token/accuracy dose-response; enriched cells stay
flat because none of these objects are modeled.

The enriched path's honest counterpart: the semantic layer *absorbs* this
messiness at modeling time. That cost is real but paid once — worth a
sentence in every writeup so the comparison is not read as free insulation.

## 3. Measurement fixes

1. **Cache-aware token metric.** The current metric sums raw
   `prompt_tokens`, which double-counts the stable prefix that providers
   (DeepSeek included) serve from cache at ~1/10 price. Record per-call
   `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens` when the provider
   returns them (DeepSeek does) and report **both** raw tokens and
   cache-adjusted cost. This matters directionally here: the enriched path's
   overhead is dominated by exactly the re-billed stable context that caching
   discounts, so raw tokens overstate the enriched cost gap. (Known open
   defect: `project_eval_scoring_carveout_and_token_metric_defects`.)
2. **Per-question paired reporting.** Report per-question deltas across
   cells, tagged {candidate error, compiler error, harness/scoring error} —
   v1's q08 scorer timeout and q06 compiler bug moved cell totals by a pass
   each. The tagging protocol exists informally in the v1 memo; make it a
   column in `report.md`.
3. **Behavioral markers, not just totals.** For SQL cells, count references
   to noise/trap tables in (a) any executed probe, (b) the final candidate;
   for enriched cells, record compiled datasource selection (from the
   generated SQL, not agent-visible). These are the mechanism variables the
   hypothesis is actually about.
4. **Replicates before breadth.** 3+ replicates of the two enriched cells
   first — their between-run dispersion IS the noise floor estimate (v1
   showed 18.6% on one draw). Then power the SQL comparison against that
   floor; 20 questions × 1 run per cell is known-insufficient. Respect the
   harness concurrency limit (~2 per leg, ~8 total).
5. **Predeclare the decision rule.** Example: proceed to all-99 only if the
   +24 confusable SQL cell shows a token increase > 2× the enriched
   replicate σ AND a non-zero trap-table selection rate, with enriched deltas
   centered on zero.

## 4. Prediction table (falsifiable)

With N ≈ 7 turns and r ≈ 2.6, schema growth ΔS predicts ΔT ≈ 2.7·ΔS
tokens/query on the SQL path *plus* whatever behavior change appears:

| Cell | ΔS (chars) | Slope-only ΔT/query | Behavioral signal expected |
|---|---:|---:|---|
| +6 confusable | ~2.5k | ~7k (+7%) | probe queries against traps; occasional wrong pick |
| +12 confusable | ~5k | ~14k (+14%) | audit turns appear (ΔN > 0) |
| +24 confusable | ~10k | ~27k (+27%) | ΔN and accuracy drop, or the hypothesis is wrong |

If the +24 cell shows only the slope term (no ΔN, no wrong picks, accuracy
flat), that is a *real finding*: modern models do not audit visible-but-
checkable noise, and the semantic layer's insulation value at this scale is
the slope difference only — worth knowing before building the pitch on it.

## 5. Implementation map (from the v1 analysis session)

- **Treatments/DDL:** `warehouse_variants.py` (per-cell setup functions
  referenced by `spec.py`), `warehouse/aggregates.sql`, `warehouse/noise.sql`
  — add `warehouse/confusable.sql` alongside. The v1 physical rename
  (`dim_*`/`fact_*`) lives in the variant setup; confusable tables must go
  through the same rename so names stay in-family.
- **Cells:** `spec.py` defines categories (name, setup fn, agent-visible
  interface); `run_eval.py --categories a,b,c` selects them. Enriched cells
  seed models from `tests/modeling/tpc_ds_duckdb` (`default_enriched_dir`).
- **Agent invocation:** `evals/common/agent_runner.py::run_agent` spawns
  `trilogy agent`; per-call usage lands in `agent_log.qNN.jsonl` as
  `llm_response.usage`. DeepSeek returns `prompt_cache_hit_tokens` /
  `prompt_cache_miss_tokens` — check whether the provider layer
  (`trilogy/ai/providers/`) already passes them through; if not, that is the
  first change, then `evals/common/scoring.py` aggregates them.
- **Scoring/reporting:** `evals/common/scoring.py` (metrics structs +
  aggregation, `parse_agent_log`), `evals/common/report.py`. The per-question
  tagging column and trap-table reference counts go here; trap-table counting
  is a regex over saved candidate SQL + probe `run_query` args in the logs.
- **Concurrency:** replicates at `--concurrency 2` per leg (~8 total) — the
  established re-baseline limit. Never run two pytest processes concurrently
  against `tests/modeling` (shared memory-duckdb).
- **Version note:** v1 ran the *installed* Trilogy (0.3.320). The token-fix
  re-baseline requires the eval environment to run the current tree (editable
  install or version bump) — verify `pip show pytrilogy` / the venv before
  trusting any new numbers.

## 6. Out of scope here, already in flight

Trilogy-side token overhead (the other half of v1's surprise) is being fixed
independently: language-reference doc cut 30.4k→18.6k chars, session-scoped
`explore` dedup (`already_shown` stubs), `file write --run/--run-and-delete`
probe collapsing. Re-baseline enriched cells AFTER those land, since they cut
the enriched intercept by an estimated 25–40% and change the v1 comparison.
