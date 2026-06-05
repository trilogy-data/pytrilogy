# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 21/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q18, q19, q20, q21, q22 | — |
| db+schema | 21/22 | — | — |
| ingest | 18/22 | q17 | q12, q13, q21, q22 |
| enriched | 19/22 | — | q11, q13, q22 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.95 | 504,769 |
| db+schema | 0.95 | 454,100 |
| ingest | 0.82 | 3,268,781 |
| enriched | 0.86 | 4,426,419 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ❌ fail |
| q12 | ✅ | ✅ | ❌ fail | ✅ |
| q13 | ✅ | ✅ | ❌ fail | ❌ fail |
| q14 | ✅ | ✅ | ✅ | ✅ |
| q15 | ✅ | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ❌ fail | ❌ fail | ✅ | ✅ |
| q18 | ✅ | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ✅ | ✅ | ✅ | ✅ |
| q21 | ✅ | ✅ | ❌ fail | ✅ |
| q22 | ✅ | ✅ | ❌ fail | ❌ fail |
