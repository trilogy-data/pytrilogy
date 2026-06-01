# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 5/10 | q01, q03, q07, q08, q09 | — |
| db+schema | 5/10 | — | — |
| ingest | 5/10 | q04 | q09 |
| enriched | 5/10 | — | q08 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.50 | 2,993,812 |
| db+schema | 0.50 | 2,591,097 |
| ingest | 0.50 | 15,396,468 |
| enriched | 0.50 | 9,753,206 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ❌ fail | ❌ fail | ❌ exhausted | ❌ fail |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ❌ fail | ❌ fail | ✅ | ✅ |
| q05 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q06 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ❌ fail |
| q09 | ✅ | ✅ | ❌ fail | ✅ |
| q10 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
