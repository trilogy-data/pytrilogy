# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 6/10 | q03, q04, q05, q07, q09, q10 | — |
| enriched | 5/10 | q08 | q04, q05 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.60 | 2,403,807 |
| enriched | 0.50 | 6,446,328 |

## Per-query matrix

| query | db-only | enriched |
|---|---|---|
| q01 | ❌ fail | ❌ fail |
| q02 | ❌ fail | ❌ fail |
| q03 | ✅ | ✅ |
| q04 | ✅ | ❌ error |
| q05 | ✅ | ❌ fail |
| q06 | ❌ fail | ❌ fail |
| q07 | ✅ | ✅ |
| q08 | ❌ fail | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
