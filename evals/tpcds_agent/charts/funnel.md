# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 7/10 | q03, q04, q06, q07, q08, q09, q10 | — |
| db+schema | 6/10 | — | q06 |
| ingest | 6/10 | q01 | q04, q06 |
| enriched | 7/10 | — | q04 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.70 | 2,157,210 |
| db+schema | 0.60 | 2,441,484 |
| ingest | 0.60 | 12,129,707 |
| enriched | 0.70 | 7,544,550 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ❌ fail | ❌ fail | ✅ | ✅ |
| q02 | ❌ fail | ❌ fail | ❌ timeout | ❌ fail |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ❌ timeout | ❌ fail |
| q05 | ❌ fail | ❌ fail | ❌ timeout | ❌ timeout |
| q06 | ✅ | ❌ fail | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
