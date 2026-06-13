# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 17/67 | q04, q10, q25, q29, q32, q39, q41, q47, q49, q52, q56, q68, q72, q74, q94, q95, q97 | — |
| enriched | 27/67 | q02, q06, q16, q24, q27, q34, q44, q45, q46, q58, q60, q70, q88, q89, q90, q96, q99 | q41, q47, q49, q56, q94, q95, q97 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.25 | 66,115,434 |
| enriched | 0.40 | 38,495,918 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ❌ fail | ❌ fail |
| q02 | ❌ exhausted | ✅ |
| q04 | ✅ | ✅ |
| q05 | ❌ timeout | ❌ timeout |
| q06 | ❌ fail | ✅ |
| q10 | ✅ | ✅ |
| q14 | ❌ fail | ❌ fail |
| q15 | ❌ fail | ❌ fail |
| q16 | ❌ fail | ✅ |
| q18 | ❌ fail | ❌ fail |
| q23 | ❌ fail | ❌ fail |
| q24 | ❌ fail | ✅ |
| q25 | ✅ | ✅ |
| q27 | ❌ fail | ✅ |
| q29 | ✅ | ✅ |
| q30 | ❌ fail | ❌ fail |
| q31 | ❌ fail | ❌ fail |
| q32 | ✅ | ✅ |
| q34 | ❌ fail | ✅ |
| q35 | ❌ fail | ❌ fail |
| q38 | ❌ fail | ❌ fail |
| q39 | ✅ | ✅ |
| q41 | ✅ | ❌ fail |
| q44 | ❌ fail | ✅ |
| q45 | ❌ fail | ✅ |
| q46 | ❌ fail | ✅ |
| q47 | ✅ | ❌ fail |
| q49 | ✅ | ❌ fail |
| q50 | ❌ fail | ❌ fail |
| q51 | ❌ fail | ❌ fail |
| q52 | ✅ | ✅ |
| q53 | ❌ fail | ❌ fail |
| q54 | ❌ fail | ❌ fail |
| q56 | ✅ | ❌ exhausted |
| q58 | ❌ timeout | ✅ |
| q59 | ❌ fail | ❌ fail |
| q60 | ❌ fail | ✅ |
| q62 | ❌ fail | ❌ fail |
| q64 | ❌ timeout | ❌ fail |
| q65 | ❌ error | ❌ fail |
| q66 | ❌ fail | ❌ fail |
| q67 | ❌ error | ❌ fail |
| q68 | ✅ | ✅ |
| q69 | ❌ fail | ❌ fail |
| q70 | ❌ fail | ✅ |
| q72 | ✅ | ✅ |
| q73 | ❌ fail | ❌ fail |
| q74 | ✅ | ✅ |
| q75 | ❌ exhausted | ❌ fail |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ exhausted | ❌ fail |
| q78 | ❌ fail | ❌ fail |
| q79 | ❌ fail | ❌ fail |
| q80 | ❌ timeout | ❌ fail |
| q81 | ❌ error | ❌ fail |
| q83 | ❌ timeout | ❌ fail |
| q84 | ❌ timeout | ❌ fail |
| q87 | ❌ timeout | ❌ fail |
| q88 | ❌ timeout | ✅ |
| q89 | ❌ fail | ✅ |
| q90 | ❌ fail | ✅ |
| q93 | ❌ fail | ❌ fail |
| q94 | ✅ | ❌ fail |
| q95 | ✅ | ❌ fail |
| q96 | ❌ fail | ✅ |
| q97 | ✅ | ❌ exhausted |
| q99 | ❌ fail | ✅ |
