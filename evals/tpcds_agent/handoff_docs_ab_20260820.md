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
