# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 5/10 | q01, q03, q07, q08, q09 | — |
| enriched | 5/10 | q02 | q01 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.50 | 16,557,302 |
| enriched | 0.50 | 14,916,695 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ✅ | ❌ fail |
| q02 | ❌ fail | ✅ |
| q03 | ✅ | ✅ |
| q04 | ❌ fail | ❌ fail |
| q05 | ❌ fail | ❌ exhausted |
| q06 | ❌ fail | ❌ fail |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ❌ fail | ❌ fail |
