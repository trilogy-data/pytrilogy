# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 21/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q20, q21, q22 | — |
| db+schema | 21/22 | — | — |
| ingest | 17/22 | — | q11, q12, q16, q22 |
| enriched | 18/22 | — | q02, q11, q22 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.95 | 587,589 |
| db+schema | 0.95 | 487,297 |
| ingest | 0.77 | 4,254,710 |
| enriched | 0.82 | 2,352,736 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ✅ | ❌ fail |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ❌ fail | ❌ fail |
| q12 | ✅ | ✅ | ❌ fail | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ✅ | ✅ |
| q15 | ✅ | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ❌ fail | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ |
| q18 | ✅ | ✅ | ✅ | ✅ |
| q19 | ❌ fail | ❌ fail | ❌ fail | ❌ fail |
| q20 | ✅ | ✅ | ✅ | ✅ |
| q21 | ✅ | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ❌ fail | ❌ fail |
