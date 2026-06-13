# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 12/57 | q01, q27, q44, q45, q47, q58, q59, q65, q70, q83, q90, q95 | — |
| enriched | 22/57 | q06, q16, q34, q49, q53, q60, q62, q88, q89, q93, q94, q96, q97 | q44, q83, q95 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.21 | 56,971,110 |
| enriched | 0.39 | 35,522,841 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ❌ exhausted | ❌ fail |
| q05 | ❌ fail | ❌ exhausted |
| q06 | ❌ fail | ✅ |
| q14 | ❌ fail | ❌ fail |
| q15 | ❌ fail | ❌ fail |
| q16 | ❌ fail | ✅ |
| q18 | ❌ fail | ❌ fail |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ❌ fail |
| q27 | ✅ | ✅ |
| q30 | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q34 | ❌ fail | ✅ |
| q35 | ❌ fail | ❌ fail |
| q38 | ❌ fail | ❌ fail |
| q41 | ❌ fail | ❌ fail |
| q44 | ✅ | ❌ exhausted |
| q45 | ✅ | ✅ |
| q46 | ❌ fail | ❌ fail |
| q47 | ✅ | ✅ |
| q49 | ❌ fail | ✅ |
| q50 | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail |
| q53 | ❌ fail | ✅ |
| q54 | ❌ fail | ❌ fail |
| q56 | ❌ fail | ❌ fail |
| q58 | ✅ | ✅ |
| q59 | ✅ | ✅ |
| q60 | ❌ fail | ✅ |
| q62 | ❌ fail | ✅ |
| q64 | ❌ exhausted | ❌ fail |
| q65 | ✅ | ✅ |
| q66 | ❌ exhausted | ❌ fail |
| q67 | ❌ fail | ❌ fail |
| q69 | ❌ fail | ❌ fail |
| q70 | ✅ | ✅ |
| q73 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ❌ fail |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ exhausted | ❌ fail |
| q78 | ❌ fail | ❌ fail |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ exhausted | ❌ fail |
| q81 | ❌ fail | ❌ fail |
| q83 | ✅ | ❌ fail |
| q84 | ❌ fail | ❌ fail |
| q87 | ❌ exhausted | ❌ fail |
| q88 | ❌ fail | ✅ |
| q89 | ❌ fail | ✅ |
| q90 | ✅ | ✅ |
| q93 | ❌ fail | ✅ |
| q94 | ❌ fail | ✅ |
| q95 | ✅ | ❌ fail |
| q96 | ❌ fail | ✅ |
| q97 | ❌ fail | ✅ |
| q99 | ❌ fail | ❌ fail |
