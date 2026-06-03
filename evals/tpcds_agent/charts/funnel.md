# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 0/1 | — | — |
| db+schema | 0/1 | — | — |
| ingest | 0/1 | — | — |
| enriched | 0/1 | — | — |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.00 | 508,810 |
| db+schema | 0.00 | 916,148 |
| ingest | 0.00 | 9,446,857 |
| enriched | 0.00 | 2,438,081 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q05 | ❌ fail | ❌ fail | ❌ exhausted | ❌ fail |
