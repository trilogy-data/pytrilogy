# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 19/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q13, q14, q15, q16, q17, q18, q20, q21 | — |
| enriched | 20/22 | q12, q19 | q08 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.86 | 3,060,886 |
| enriched | 0.91 | 2,115,815 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ✅ | ✅ |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ✅ | ✅ |
| q06 | ✅ | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ❌ fail |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ❌ fail | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ✅ |
| q15 | ✅ | ✅ |
| q16 | ✅ | ✅ |
| q17 | ✅ | ✅ |
| q18 | ✅ | ✅ |
| q19 | ❌ fail | ✅ |
| q20 | ✅ | ✅ |
| q21 | ✅ | ✅ |
| q22 | ❌ fail | ❌ fail |
