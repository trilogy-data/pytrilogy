# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 7/10 | q01, q03, q04, q06, q07, q09, q10 | — |
| db+schema | 6/10 | — | q01 |
| ingest | 4/10 | — | q01, q06, q10 |
| enriched | 6/10 | q02 | q06, q10 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.70 | 2,507,452 |
| db+schema | 0.60 | 1,917,372 |
| ingest | 0.40 | 16,721,581 |
| enriched | 0.60 | 12,652,992 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ❌ fail | ❌ fail | ✅ |
| q02 | ❌ fail | ❌ fail | ❌ exhausted | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ❌ fail | ❌ fail | ❌ exhausted | ❌ fail |
| q06 | ✅ | ✅ | ❌ fail | ❌ fail |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ❌ fail | ❌ fail |
