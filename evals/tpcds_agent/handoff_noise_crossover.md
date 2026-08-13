# Handoff: noise-dose crossover — where the semantic layer pays for itself

**2026-08-12.** Follows [`handoff_messy_warehouse_first20.md`](handoff_messy_warehouse_first20.md)
and [`design_messy_warehouse_v2.md`](design_messy_warehouse_v2.md). Three questions:
(1) at what warehouse-noise level does the SQL path's token cost cross the
enriched path's, (2) does resolving to pre-aggregated tables buy latency, and
(3) what moves the crossover lower.

## 1. Break-even estimate (from run `20260811-145002`, deepseek-v4-flash, 20q)

Per-leg raw totals, plus cache hit/miss *estimated* from per-turn usage in the
JSONL logs (turn *i*'s cached prefix ≈ turn *i−1*'s prompt + completion;
adjusted cost weights cache-hit input at 0.1 — DeepSeek's published ratio):

| leg | pass | total tokens | est. cache hit | adjusted cost |
|---|---:|---:|---:|---:|
| sql_schema_aggregates | 17 | 3,729,773 | 3,170,297 | 876,506 |
| sql_schema_noise | 18 | 3,410,857 | 2,877,055 | 821,508 |
| enriched_aggregates | 16 | 10,300,910 | 9,413,657 | 1,828,619 |
| enriched_noise | 17 | 9,928,497 | 9,106,981 | 1,732,214 |

A live two-turn DeepSeek probe and a one-query x12 dry run (84% measured cache
hit) both validate the estimator to within a few points.

The measured cell-vs-cell slope is **negative** this run (the noise cell used
fewer tokens) — one more confirmation that a 3.5k-char treatment is
undetectable under the ±20% run-variance floor, and that crossover estimates
must come from the validated rebilling model (`T ≈ N·P₁ + Σs·(N−t)/r`), not
cell deltas. Model slope at this run's iteration counts: **48.5 raw
tokens/schema-char per 20q** (≈2.4/char/query); cache-adjusted: **11.8**
(the schema payload is a miss once, then a discounted hit on every later turn).

Crossover (schema size where SQL total = enriched total; current schema is
28,920 chars; one noise table ≈ 294 schema chars):

| metric | crossover schema | vs current | ≈ clean noise tables |
|---|---:|---:|---:|
| raw tokens | 157–165k chars | **5.4–5.7x** | ~435–460 |
| cache-adjusted cost | 102–110k chars | **3.5–3.8x** | ~250–275 |

Two readings:

- On **raw tokens**, clean (never-inspected) noise can't plausibly close the
  gap — no real warehouse has 450 irrelevant tables per 24 real ones. The
  token pitch cannot rest on clean-schema bloat alone.
- On **cost**, caching compresses the enriched premium from 2.76x to 2.09x
  (the enriched overhead is exactly the re-billed stable context that caching
  discounts), pulling break-even to ~3.5x schema — large-but-real warehouse
  territory. **Report cache-adjusted cost, not raw tokens, from now on**
  (now automatic — see §3).

## 2. Noise-dose sweep (in flight)

New categories `sql_schema_noise_x{4,12,24,48}` scale the 12-table noise set
by stamping regional-subsidiary replicas (`dim_hr_employee_emea`,
`fact_payroll_entry_jp_legacy`, …) — see `warehouse_variants.NOISE_DOSES` /
`noise_replica_sql`. Per-category `tool_output_limit` overrides keep
`read_file('schema.md')` untruncated (at the default 32,768 cap, x12+ would
silently middle-delete real tables — the alphabetical listing interleaves
them with noise).

Predictions vs actuals (run `20260812-133647`, 20q, deepseek-v4-flash — cache
fields now measured, not estimated):

| cell | pred raw | **actual raw** | pred adj | **actual adj** | pass | iters |
|---|---:|---:|---:|---:|---:|---:|
| x4 | 4.4M | 4.05M | 1.04M | 0.92M | 18 | 167 |
| x12 | 5.8M | 5.71M | 1.37M | 1.23M | 16 | 166 |
| x24 | 7.9M | 7.09M | 1.89M | 1.65M | 16 | 162 |
| x48 | 12.1M | **11.98M** | 2.92M | 2.71M | 16 | 166 |
| enriched ctrl | — | 12.45M | — | 2.08M | **20** | 312 |

**The linear full-injection model is confirmed** — x48 within 1% raw, and
iterations dead flat (162–167) from 42 to 576 tables: zero behavioral
response, pure schema rebilling. Clean noise never induces audits, at any
dose. Measured crossover against the two enriched replicates (10.3M/1.83M
and 12.45M/2.08M): **raw ~5–7x schema; cache-adjusted ~4.5–5.2x (~150k
chars)**. The between-slope from x24→x48 is 12.2 adj-tokens/char — the model
said 11.8.

The enriched control also hit **20/20** this run vs 16/20 yesterday on
byte-identical inputs — a four-pass control swing that re-confirms per-cell
accuracy comparisons at 20 questions are meaningless. Aggregate usage: 1–3
SQL candidates per dose cell picked a `fact_agg_*` table (visible in the new
funnel column); 0 compiled enriched candidates did, as diagnosed.

The one-query x12 dry run already sits on the line (190k raw for q01 vs ~80k
at base schema). If accuracy holds flat and iteration counts stay ~8/query
across doses, the slope model is confirmed and the crossover lands where
predicted; extra audit turns or accuracy drops at high dose would pull it
earlier (and would be the first *behavioral* noise effect we've observed —
clean noise has never induced one). Risk to watch at x48: the ~78k-token
schema payload plus conversation may brush the provider context limit;
`crashed`/`length` statuses there are a harness artifact, not a treatment
effect.

### Two regimes — the linear model is conditional on behavior

The linear slope describes the **full-injection regime**: one schema payload,
constant N (empirically true so far — both SQL cells sat at 163→166
iterations while schema grew). The dose cells deliberately preserve that
regime (raised `tool_output_limit`) so the slope term is measured cleanly;
iterations-vs-dose is the in-run diagnostic — growing N means behavioral
compounding has appeared and the linear extrapolation is dead.

The **discovery regime** is different: with no schema handed over, each probe
result is rebilled on every later turn AND turn count grows with probes —
~O(probes²), not exponential (nothing grows payloads proportionally to
accumulated context), but plenty to move a crossover. It can also bend the
other way: a selective agent at 600 tables lists names and describes only the
plausible ones, going *sub*-linear. `sql_bare_noise_x{4,12,24,48}` cells
(same noisy db, NO schema.md, default output cap — at x48 even SHOW TABLES
exceeds the 25-row result cap, so the agent must filter/paginate
`information_schema` itself) measure this regime directly; run them after the
injection sweep so the two curves share the enriched reference line.

### Pretraining-recall discriminator (`sql_schema_colrename`)

The 40 SQL sessions show a suspicious shape: zero `DESCRIBE`/introspection
calls ever — the agent reads schema.md once and spends its 1–17 pre-answer
`run_query` calls polishing drafts, never investigating. That is consistent
with either genuine schema-reading or TPC-DS recall, and table renames can't
separate them: the recognition surface is the canonical *column* names
(`ss_sold_date_sk`, `d_moy`, …) plus parameter fingerprints. (The question
texts are NOT a surface — `query_prompts.json` is already heavily
paraphrased operational prose, so a paraphrase cell would measure nothing.)

`sql_schema_colrename` = `sql_schema_aggregates` with all 495 canonical
columns mechanically renamed to honest warehouse names (strip abbreviation
prefix, `_sk`→`_key`, keyword stems entity-qualified: `s_state`→`store_state`;
one global map keeps fact/agg/dim joins consistent; COMMENT text rewritten to
match; reference SQL untouched — it runs on the separate unrenamed reference
db). The A/B against `sql_schema_aggregates` measures the
**identifier-mapping subsidy**. It does NOT remove deep recognition (the
model can still spot q2's 8-channel pivot from the question shape) — full
escape needs a novel synthetic schema; this cell decides whether that
investment is necessary. If tokens/accuracy hold: the model does identifier
translation cheaply and TPC-DS stays valid for within-path treatments. If the
cell degrades: the measured delta IS the pretraining subsidy, and the
SQL-vs-enriched comparison has been flattering SQL by that much (Trilogy gets
no such subsidy — the language isn't in pretraining).

### Wave 2 results (run `20260812-144241`, 20q each)

**Discovery regime is FLAT — the crossover question is settled.**

| cell | pass | raw | adj | iters | vs injection at same dose |
|---|---:|---:|---:|---:|---|
| bare x4 | 19 | 3.96M | 0.82M | 271 | ≈ equal (4.05M) |
| bare x12 | 18 | 5.51M | 1.06M | 295 | −3% |
| bare x24 | 18 | 4.07M | 0.85M | 271 | **−43%** |
| bare x48 | 18 | 4.17M | 0.91M | 276 | **−65%** |

No schema.md at all: the agent does selective discovery (~65% more
iterations than injection legs, but each turn carries a small context) and
total cost is ~O(1) in noise dose — neither quadratic compounding nor even
linear. Accuracy is *better* than injection at high dose (18–19 vs 16). Two
consequences: (a) whole-schema injection is a liability at scale, not a
baseline — a competent agent self-discovers more cheaply; (b) against a
discovery agent, clean-noise token crossover **never** happens at any
warehouse size. The layer's economic case must rest on confusable-mess
accuracy, latency/aggregates, and authoring ergonomics — not raw tokens.

(Enriched reference for this table: the same-day control, run
`20260812-133647_enriched_aggregates` — 12.45M raw / 2.08M adj / 20/20 /
312 iters — dose-independent by construction, so it is one horizontal line
against all four bare cells.)

**Recall discriminators: no smoking gun — TPC-DS survives.**

| cell | pass | raw | iters |
|---|---:|---:|---:|
| baseline (canonical) | 17 | 3.73M | 166 |
| colrename | 17 | 4.31M | 198 |
| paramshift | 17 | 4.21M | 177 |
| colrename+paramshift | 15 | 2.89M | 159 |

Accuracy flat within the (±2–4 pass) control noise floor; token deltas
(±16%) inside the replicate range. The paramshift-only failures (q02, q16,
q14) looked like a recall signature, but the candidates **use the shifted
values correctly** — ordinary errors on chronically hard queries, not
canonical-value leakage. Verdict: this model's TPC-DS performance is genuine
schema-reading, not identifier/parameter recall; the pretraining subsidy on
surfaces we can treat is ≲15% tokens and ~0 accuracy. Question-structure
recognition remains untreatable in-benchmark, but it evidently doesn't need
the surface forms. A novel-schema benchmark is deprioritized accordingly.

Bonus observation: the paramshift q02 candidate answered from
`fact_agg_web_sales_daily`/`fact_agg_catalog_sales_daily` — SQL agents use
the visible aggregates opportunistically when the query shape invites it.

### Wave 3 — confusable noise (built 2026-08-12, launches after wave 2)

The dose sweep proved clean noise is answerable by reading (schema.md carries
full columns/comments/row counts — probing is redundant, hence flat N). Wave 3
makes reading insufficient: in-domain traps derived from the REAL tables.

- `sql_schema_confusable_x{1,2,3}` — cumulative: x1 = 7 fact `_v2` samples
  (~88%, deterministic bernoulli); x2 adds 7 `_bak` (~93%) + 6 `_daily` grain
  traps (real daily rollups with measures under line-grain column names — at
  the 8-key grain they're 99.995% of line rows, so numbers come out *almost*
  right, the most dangerous kind); x3 adds 7 `_staging` (~80%) + 5 dim
  `_snapshot` copies (~90%). Real tables stay complete (references hold);
  traps carry NO comments (realistic, and the doc asymmetry is a fair signal).
- `enriched_confusable` — same physical traps at x3, and the curated layer is
  *plausibly curated* (decided with the user): the genuinely useful daily
  summaries are modeled as own clearly-described files
  (`raw/<channel>_daily_summary.preql`, public `daily_*` concepts, generated
  from the private binding snippets by `daily_summary_model()`); `_bak`-class
  junk stays unmodeled, as a real curator would leave it. Note this arm is no
  longer an invisibility control — its explore surface grows too; that's the
  honest price of the treatment.
- Hypothesis: SQL cells show ΔN (audit turns) and/or wrong-table picks with
  accuracy loss scaling with dose; enriched stays flat on both. Either
  failure mode is the first *behavioral* noise effect — the thing clean noise
  provably cannot produce.

**Wave 3 results (run `20260812-153111`; launcher externally stopped at
enriched q18, SQL legs complete): the hypothesis is REFUTED as constructed.**

| cell | pass | raw | iters | trap refs in candidates |
|---|---:|---:|---:|---:|
| confusable x1 | 18 | 3.74M | 175 | 0 |
| confusable x2 | **20** | 4.40M | 166 | 0 |
| confusable x3 | 19 | 5.04M | 171 | 0 |
| enriched (17/20 run) | 15/17 | — | — | n/a |

Zero trap-table references across all 60 SQL candidates (every `_daily` hit
is the legitimate `fact_agg_*`); x2 is the first perfect SQL score of the
whole experiment; iterations flat; token growth = schema-size slope again.
The model's canonical-name prior ("use the plain-named table") plus the
comment asymmetry resolves even in-domain near-duplicates by reading. Both
enriched fails (q12, q14) are chronic queries — q12 manually re-scored:
compiled to raw `fact_web_sales`, no summary/trap involvement.

Three escalating treatments, one conclusion: **any treatment resolvable by
reading will not move a modern model.** To induce the failure mode the trap
must remove the canonical-name signal itself — e.g. two equally-plain
co-plausible tables (`fact_store_sales` vs `fact_store_transactions` at
different grains), or semantic column traps (a materialized `net_profit`
that is actually gross, matching the documented profit-vs-net_profit
distinction). That is the only remaining in-benchmark design lever; short of
it, the layer's measurable value lives in aggregate routing (engine fix now
live), latency at scale, and authoring ergonomics — not agent-side
insulation.

### Probe-count Pareto analysis (2026-08-12, post-wave-3)

Token spend is only one axis; the other is **how many queries the agent
fires at the warehouse** (probe load — at SF=1 probes are milliseconds, but
against a real warehouse 400 probes/20 questions is real latency and real
compute spend). Counted from `tool_calls_by_name` in each `report.json`
(SQL legs: `run_query` + `run_file`; enriched: `trilogy run` — its
`explore`/`agent-info`/`file` calls read the model, not the database):

| leg | pass | adj tokens | DB queries | per question |
|---|---:|---:|---:|---:|
| bare x4 | 19 | 0.82M | 388 | 19.4 |
| bare x12 | 18 | 1.06M | 445 | 22.2 |
| bare x24 | 18 | 0.85M | 397 | 19.9 |
| bare x48 | 18 | 0.91M | 418 | 20.9 |
| inject x4 | 18 | 0.92M | 156 | 7.8 |
| inject x12 | 16 | 1.23M | 176 | 8.8 |
| inject x24 | 16 | 1.65M | 147 | 7.3 |
| inject x48 | 16 | 2.71M | 147 | 7.3 |
| enriched ctrl | 20 | 2.08M | 101 | 5.0 |
| enriched 08-11 | 16 | (pre-cache-fields) | 94 | 4.7 |
| enriched confusable (18q logs) | 15/17 | 1.87M | 91 | 5.1 |

Three regime signatures, all dose-flat on probes:

- **Bare discovery buys its token flatness with probes**: ~20 DB
  queries/question at every dose — 4x the enriched rate. Discovery IS
  querying; the warehouse is its schema document.
- **Injection sits in the middle** (7.3–8.8/question): the schema payload
  eliminates introspection probes but the agent still validation-loops
  drafts.
- **Enriched is probe-minimal** (4.7–5.1/question, stable across three runs
  including the confusable workspace): the model answers exploration
  questions that would otherwise be probes, and `trilogy run` fires only to
  validate near-final drafts.

**Pareto composition** (axes: adjusted tokens × DB probes × accuracy):
bare anchors the token-minimal end at every dose; enriched anchors the
probe-minimal end at every dose; **at x48 injection is strictly dominated
by enriched on all three axes** (2.71M vs 2.08M adj, 147 vs 101 probes,
16 vs 20 pass). At x4–x24 injection stays on the frontier only via its
middling probe count. So the user-facing claim is now measured: the
enriched path is on the Pareto frontier as the *probe-and-accuracy*
optimum — you pay ~2.3x the discovery agent's adjusted tokens to cut
warehouse queries 4x (and injection stops being defensible at all past
~x24). Caveats: enriched accuracy swings 16↔20 across byte-identical
replicates, so treat the accuracy edge as directional; and per-probe
*weight* differs — bare's probes skew to cheap `information_schema`
pagination, while each enriched `run` carries engine compile (~1.6s p50
measured from log timestamps, client-side, not warehouse load).

**q17 hang localized**: 3,854s inside one `trilogy run` tool call — but the
saved candidate compiles in 0.3s to raw facts (planner and the new aggregate
matching are exonerated). The hour was DuckDB *executing* an agent draft
with a runaway join. Harness gap, not engine bug: the agent's `run` tool has
no execution timeout, so one bad draft can eat the whole per-query wall
clock. Worth a bounded-execution knob before long unattended runs.
- The 2026-08-12 engine fix (aggregate signature matching,
  [`handoff_aggregate_selection_gap.md`](handoff_aggregate_selection_gap.md))
  is live: compiled enriched aggregates now resolve to summary tables, so the
  funnel's `agg used` column measures Trilogy aggregate selection for real.

## 3. Instrumentation landed with this memo

- **Cache-aware tokens end-to-end**: `UsageDict.cached_prompt_tokens`
  populated for DeepSeek/OpenAI/OpenRouter/Anthropic/Google
  (`trilogy/ai/providers/*`), logged per turn by the agent, aggregated in
  `AgentMetrics.cached_prompt_tokens`, reported as `tokens.cached_prompt` +
  `tokens.cache_adjusted` in `report.json`, shown in `report.md` and both
  funnel outputs. Closes the metric half of
  `project_eval_scoring_carveout_and_token_metric_defects`.
- **Candidate latency**: `QueryResult.cand_ms` / `ref_ms` time the scored
  candidate and reference executions; per-query columns in `report.md`, p50
  in the funnel. This is the aggregate-payoff metric — at SF=1 the deltas are
  tens of ms, so treat direction, not magnitude.
- **Aggregate usage**: `QueryResult.used_aggregate` flags `fact_agg_*` in the
  *executed* SQL — final-candidate table choice for SQL legs, compiled
  datasource selection for Trilogy legs (invisible to the agent). Funnel
  shows `agg used n/20`.

## 4. Why Trilogy never picks the aggregates (diagnosis, 2026-08-12)

0/40 compiled enriched candidates selected a `fact_agg_*` datasource. Full
record: [`handoff_aggregate_selection_gap.md`](handoff_aggregate_selection_gap.md).
**Direction decided 2026-08-12: bindings stay underscore-hidden; the fix is
engine-side matching, not exposing named metrics.** Summary (workspace
`results/20260811-145002_enriched_aggregates/workspace`):

- **The bindings are correct.** A query naming the bound concept
  (`ss._warehouse_sum_sales_price`) compiles to a proper SUM-rollup over
  `fact_agg_store_sales_daily`.
- **The planner can't link agent-authored aggregates to them.** Discovery is
  address-keyed; the rollup gate
  (`concept_strategies_v4.py:320`) requires the queried concept's *address*
  among the datasource outputs before the capable signature matcher
  (`aggregate_rollup.py`) ever runs. The canonical-hash bridge fails for
  imported namespaces (`local._virt_agg_…` vs `ss._virt_agg_…` — same hash,
  different prefix) and for grain-abstract vs grain-pinned virt-agg
  registration, so an inline `sum(x) as alias` in `ss.`/`cs.`/`ws.` imports
  can never match. Root-marking alone doesn't fix it (verified by
  monkeypatch) — group-graph coverage and `renders_materialized_canonical`
  are address-keyed too.
- **Existence-only subqueries lose a tie-break**: both sources bind the keys,
  and `grain_score` prefers the raw fact; no size/cost notion exists in
  datasource selection.
- **Ceiling**: 9 of 19 saved candidates were strictly servable from the aggs
  (11 with a `sum(a−b) → sum_a−sum_b` linearity rewrite); 6 are principled
  non-matches (row-level filters, measure products, count-distinct grains).
  The 0/40 is entirely the matching gap, not workload shape.
- **Fix path (per the hidden-binding requirement)**, in order of leverage:
  namespace-free canonical identity for `_virt_*` concepts; grain-pinned
  canonical registration; alias-as-pseudonym rewrite at rollup root-marking;
  size-aware datasource tie-break. (Exposing named metrics would work today
  with zero engine changes, but surrenders the private-implementation-detail
  claim — rejected.)

## 5. Moving the crossover lower (ranked)

1. **Price it right (done)**: cache-adjusted cost alone moves break-even from
   ~5.5x to ~3.6x schema. Every future comparison should lead with it.
2. **Cut the enriched intercept.** At a realistic 2x-noise warehouse (~58k
   chars) the SQL path costs ~1.22M adjusted; enriched needs ~1.2M — a **~33%
   adjusted-cost cut** from today's 1.83M. The decomposition says the
   intercept is `N·P₁` + per-turn rebilling of explore/help payloads, so the
   levers are: fewer iterations (enriched used 282–295 vs SQL's 165–166 —
   the single biggest term), smaller first-prompt + help corpus (the 2026-08-10
   trim already bought 12%), and tighter explore payloads (session dedup
   landed; `already_shown` stubs cut rebilling directly).
3. **Named metrics in the enriched model.** Exposing the aggregate-bound
   metrics kills two birds: compiled queries hit `fact_agg_*` (latency story
   becomes measurable) and agents skip re-deriving common sums (fewer
   exploration/validation turns → intercept cut).
4. **Confusable noise (design v2)** changes the *slope* story to an
   *accuracy* story: stale near-duplicates and grain traps degrade SQL
   correctness at realistic table counts, where clean noise provably doesn't.
   Accuracy break-even arrives far earlier than token break-even — that's the
   experiment that can justify the layer at 1–2x schema.
5. **Engine aggregate matching** (§4) — the durable version of (3): agents
   and humans write naive aggregates; the compiler quietly serves them from
   summaries. That's a competitive claim no SQL-agent baseline can replicate.

## Artifacts

- Break-even script: session scratchpad `breakeven.py` (reads
  `results/20260811-145002_*/report.json`; re-run against any 4-leg run).
- Dose cells: `spec.py`, `warehouse_variants.py`
  (`NOISE_DOSES`, `noise_replica_sql`, `noise_dose_output_limit`).
- Instrumentation: `evals/common/scoring.py`, `report.py`, `analyze_run.py`,
  `categories.py`, `agent_runner.py`; `trilogy/ai/models.py`,
  `trilogy/ai/providers/*`, `trilogy/scripts/agent.py`.
- Sweep results: `results/<ts>_{enriched_aggregates,sql_schema_noise_x*}/`
  once the 2026-08-12 launch lands; funnel at `charts/funnel_v2.png`.
