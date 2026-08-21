# Handoff: enriched vs enriched_docs A/B (run 20260820-153007)

For the evening session. Everything here is from run `20260820-153007`
(99q x 2 legs, sf=1, deepseek-v4-flash, concurrency 3/leg), the first run on
the post-probe-wave tree: explore json v3 outline, the q17 Syntax[225] fix,
the q47 codegen fixes, and the new `enriched_docs` category all landed earlier
the same day. Do NOT compare this run's numbers to any pre-08-20 run without
remembering all four changed at once; the only clean comparison is the two
legs of THIS run against each other.

## Results

| | enriched | enriched_docs |
|---|---|---|
| pass | 95/99 | 91/99 |
| raw tokens med / mean | 147.7k / 222.2k | 157.0k / 230.5k |
| cache-adj med / mean | 42.1k / 52.9k | 38.6k / 50.9k |
| LLM calls med / mean | 8 / 9.5 | 7 / 8.2 |
| agent-info directory calls | 99 | 9 |
| agent-info subcalls total | 409 | 126 |
| explore calls / avg chars | 438 / 5,705 | 452 / 5,347 |
| explore payloads w/ dedup stubs | 236 | 215 |
| first-turn prompt med | 2,182 | 6,670 (94% median cached share) |
| fails | q14 (timeout), q17 (timeout), q28, q66 | q20, q35, q41, q66, q67, q79, q81, q98 (all wrong rows) |

Funnel: `charts/funnel_deepseek_deepseek-v4-flash.{png,md}` (regenerated after
fixing a gap: the tpcds spec pins its own `funnel_order`, which had not gained
`enriched_docs`; fixed in `spec.py` same day, so future funnels include it).

## Findings

1. **Docs preload works mechanically and pays modestly.** The inline
   reference collapses discovery (agent-info directory calls 99 to 9,
   subcalls 409 to 126), saves 1.3 turns and ~8% cache-adjusted tokens
   (42.1k to 38.6k median), and the shared task prefix cache-hits at 94%
   median from query 2 on. Raw tokens go UP ~6% because every turn re-carries
   the ~4.5k-token preamble; adjusted cost is the billing-real metric.
2. **The docs leg dropped 4 net passes and that needs triage before believing
   it.** 8 wrong-row fails, mostly disjoint from enriched's fails (only q66
   shared); at one run this is ~1.8 sigma, i.e. weak evidence, but the shape
   is worth checking: did docs-leg agents skip syntax-example drilldowns they
   needed (126 vs 409 subcalls) and author subtly wrong idioms? Start with
   q81 (sink AND fail, 181k adj) via
   `repeat_query.py --query-id 81 --repeats 10 --scale-factor 1 --category enriched_docs`,
   and read its `agent_log.q81.conversation.txt` against the enriched leg's
   passing q81.
3. **Explore v3 halved per-call size but the drill-down protocol doubled the
   call count, netting roughly a wash on this run.** Per-call chars 12.0k to
   5.7k, dedup now fires (54% of payloads carry `already_shown` stubs), but
   calls/query went 2.4 to 4.4 and mean LLM calls rose ~1 on the enriched
   leg. Paired against yesterday's enriched leg: raw med +12k / mean -5.6k,
   adj med +1.5k / mean -2.1k. Tuning candidates, in order: (a) guidance to
   pass SEVERAL `--ns` in ONE call (the flag is repeatable; agents issue
   sequential single-`--ns` calls today, each rebilling the context); (b)
   auto-expand the 1-2 dims the `--regex`/question terms touch; (c) check
   whether `TRILOGY_EXPLORE_COMPACT=0` on one leg of a future run isolates
   the outline's pass-rate effect.

## Top cache-adjusted sinks this run (adj basis per the q29 lesson; raw is
reasoning-replay inflated ~5-10x)

- enriched: q05 218k, q64 207k, q14 183k (timeout), q44 148k, q84 137k,
  q25 128k. All but q14's timeout are known open reports
  (`INDEX_probe_wave_2026_08_20.md`): keyless cluster (q05/q25/q64/q84),
  q44 empty-error, q14 values-list binder.
- enriched_docs: q84 294k, q81 182k (FAIL, new), q75 141k, q14 121k,
  q86 118k, q23 115k, q05 115k, q17 112k.

q17 no longer syntax-loops (fix verified in the docs leg: passed at 112k adj)
but TIMED OUT on the enriched leg; check whether that is the q64-style perf
cliff or a new obstacle before filing anything.

## Deep dive: the preload theory holds; leg medians were the misleading view

Paired per-query comparison (same run, same tree) instead of leg medians:

- **Sign test: 68/99 queries are cheaper (cache-adj) under docs preload.**
  Paired deltas: raw med -4.1k, adj med -4.7k (-11%), turns med -1.0.
- **The saving is almost exactly the predicted mechanism and nothing more**:
  the enriched leg pays full-rate arrival for the guide+index each query
  (~5.4k tokens) plus two discovery turns; the docs leg pays ~10% cache rate
  on the preamble (~0.5k) - predicted delta ~-4.9k, observed -4.7k. The docs
  leg fetched the query guide ZERO times (instruction respected; index 9
  times, syntax examples 117 vs enriched's 155+47+99+99 drilldowns).
- **Why the leg medians hid it**: the two legs' sink tails differ (q84 blew
  up on docs, q05/q64 on enriched), and the preamble inflates RAW on every
  turn while pricing at 10% - raw-median comparisons across legs are the
  wrong lens. An apparent "savings grow with trajectory length" pattern is
  regression-to-the-mean (bucketing by the other leg's length inverts it).
- **Why it is only -11% and not the floor model's 3x**: the floor scenario
  assumed 2-4 call trajectories where language discovery is half the cost.
  At today's 7-8+ calls, discovery is ~12% of spend; preloading removes most
  of that 12% and cannot touch the rest (explore, authoring loops, sinks).
  The lever that unlocks the rest is turn count, i.e. the bug backlog.
- **Failure shapes are mixed, not uniformly overconfident**: q41 failed in 4
  turns/1 explore (vs enriched pass at 7), q67 in 5 (vs 12), q20 in 6 (vs 7)
  - the wrote-fast-and-wrong shape; but q79 (13) and q81 (18) failed long,
  and all 8 fails DID fetch syntax examples ('query-structure' in 6 of 8).
  Content-level triage still required before blaming the preamble.

## RESOLVED (evening triage, all 8 docs-leg fails root-caused)

Verdict: **no systemic preamble problem; do not revert or rework the docs
cell.** Per-query, with executable proof (single-edit flips to pass unless
noted):

| q | verdict | preamble-caused? |
|---|---|---|
| q20 | Coinflip: NULL-class denominator pooling the question never specifies; both legs deliberated it, enriched won on TPC-DS recall | no |
| q35 | Drafting slip (21 cols vs its own correct 18-col plan), self-check rubber-stamped | no |
| q41 | Careless question read + skipped the naive-vs-staged compare probe the enriched twin ran (which returned the exact 23 wrong rows docs shipped) | no |
| q67 | Dropped the rank<=100 HAVING; dismissed the `window_filter_needs_having` validator that named the fix and a rnk max of 51,491 | no |
| q79 | Unrequested `is_returned = false` filter, seeded by the model doc comment on is_returned (reword it: "this sale line was later returned") | no |
| q81 | FRAMEWORK: keyless-guard cluster rejected the agent's CORRECT first write 3x; workaround's defensive null filter flipped one row. First graded wrong answer from the cluster; the cluster was FIXED 2026-08-20, so the docs-cell over-exposure below no longer applies | indirect: the guide's "alias every new expression" style is the cluster's trigger, so the docs cell is over-exposed until the cluster is fixed |
| q98 | Same NULL-class ambiguity as q20 (2 of 2521 rows); enriched won on reference recall | no |
| q66 | FRAMEWORK, both legs, NEW BUG: union() TVF output drops arm nullability -> NULL warehouse group silently dropped. `bug_q66_union_output_drops_nullable.md` (FIXED 2026-08-20, report deleted; see `INDEX_probe_wave_2026_08_20.md`) | no (fails every leg) |

Score: 2 framework (q81, q66), 2 one shared question ambiguity (q20/q98),
3 individual comprehension/verification lapses, 1 model-doc trap. The
population stats support noise for the lapses: the docs leg probes MORE
overall (3.0 vs 2.4 probe-writes/q) and its fail set is not
uniformly fast-and-wrong.

Cheap hardening before the rerun: (a) fix the keyless cluster (now carries a
graded wrong answer and the docs cell is over-exposed to it); (b) fix or
accept q66's union nullability bug; (c) DONE `f035ed3b2`: q20/q98 now specify
name-identified classes with a single shared no-recorded-class group, q79 now
states later-returned lines stay in scope, and the `is_returned` model doc
comment no longer invites excluding them; (d) then rerun the A/B and expect
the docs leg to recover 3-4 of the 8. Comparability caveat: q20/q79/q98
results from runs before `f035ed3b2` are not prompt-comparable to runs after
it - judge those three queries within a run, not across the boundary.

## Suggested evening order

1. Triage the docs-leg fails (finding 2). If systematic, the fix is likely a
   preamble tweak (e.g. keep pointing at syntax examples) not a revert.
2. q81 + q84 sink probes (both legs' q84 grew; the keyless-cluster report
   likely explains it, confirm before new filings).
3. Explore v3 tuning 3a (one-line guidance change), then a 10x repeat on a
   deliberation-heavy query (q49-class) to check the call-count effect.
4. Bugfix backlog: `INDEX_probe_wave_2026_08_20.md` and `README.md` stack
   rank (keyless cluster is the highest-leverage open engine bug; q44 and
   q14 next).

House rules that bit today: never run two pytest processes at once; results
comparisons on adj basis; a >500k raw query is only a detector lead after
cache adjustment; `repeat_query.py` for any pass-rate claim; the tree may
have another session working in parallel, so never git stash/checkout/reset.
