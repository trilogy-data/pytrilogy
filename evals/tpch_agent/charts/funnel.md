# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 21/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19, q21, q22 | — |
| db+schema | 21/22 | q20 | q17 |
| ingest | 21/22 | — | q12 |
| enriched | 20/22 | — | q12, q15 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.95 | 532,080 |
| db+schema | 0.95 | 396,089 |
| ingest | 0.95 | 1,732,269 |
| enriched | 0.91 | 2,064,465 |

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
| q11 | ✅ | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ❌ fail | ❌ fail |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ✅ | ✅ |
| q15 | ✅ | ✅ | ✅ | ❌ fail |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ❌ fail | ✅ | ✅ |
| q18 | ✅ | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ✅ | ✅ | ✅ |
| q21 | ✅ | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ✅ | ✅ |
