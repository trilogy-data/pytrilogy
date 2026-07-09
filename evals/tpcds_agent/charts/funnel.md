# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 18/20 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q19 | — |
| db+schema | 17/20 | — | q17 |
| enriched | 14/20 | — | q01, q02, q14, q17 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.90 | 4,908,595 |
| db+schema | 0.85 | 3,966,430 |
| enriched | 0.70 | 7,979,586 |

## Per-query matrix

| query | db-only | db+schema | enriched |
|---|---|---|---|
| q01 | ✅ | ✅ | ❌ fail |
| q02 | ✅ | ✅ | ❌ fail |
| q03 | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail |
| q15 | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ✅ |
| q17 | ✅ | ❌ fail | ❌ fail |
| q18 | ❌ fail | ❌ fail | ❌ fail |
| q19 | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ❌ fail |
