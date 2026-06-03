# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 7/10 | q03, q04, q06, q07, q08, q09, q10 | — |
| db+schema | 7/10 | q05 | q04 |
| ingest | 7/10 | q01 | q05, q06 |
| enriched | 7/10 | — | q01, q05 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.70 | 1,552,117 |
| db+schema | 0.70 | 4,120,974 |
| ingest | 0.70 | 9,514,474 |
| enriched | 0.70 | 8,442,749 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ❌ fail | ❌ fail | ✅ | ❌ fail |
| q02 | ❌ fail | ❌ exhausted | ❌ fail | ❌ exhausted |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ❌ fail | ✅ | ✅ |
| q05 | ❌ fail | ✅ | ❌ exhausted | ❌ fail |
| q06 | ✅ | ✅ | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
