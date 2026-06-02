# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 8/10 | q02, q03, q04, q05, q06, q07, q09, q10 | — |
| db+schema | 9/10 | q08 | — |
| ingest | 6/10 | q01 | q02, q05, q06, q10 |
| enriched | 7/10 | — | q01, q05, q10 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.80 | 1,603,915 |
| db+schema | 0.90 | 1,330,809 |
| ingest | 0.60 | 9,636,427 |
| enriched | 0.70 | 13,868,407 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ❌ fail | ❌ fail | ✅ | ❌ fail |
| q02 | ✅ | ✅ | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ❌ exhausted | ❌ fail |
| q06 | ✅ | ✅ | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ❌ fail | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ❌ fail | ❌ fail |
