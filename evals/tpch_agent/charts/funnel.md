# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db-only | 21/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q20, q21, q22 | — |
| db+schema | 22/22 | q19 | — |
| ingest | 17/22 | — | q02, q11, q12, q19, q22 |
| enriched | 17/22 | — | q11, q12, q15, q17, q22 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db-only | 0.95 | 624,748 |
| db+schema | 1.00 | 569,781 |
| ingest | 0.77 | 5,860,192 |
| enriched | 0.77 | 3,000,076 |

## Per-query matrix

| query | db-only | db+schema | ingest | enriched |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ✅ | ❌ fail | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ❌ fail | ❌ fail |
| q12 | ✅ | ✅ | ❌ fail | ❌ fail |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ✅ | ✅ |
| q15 | ✅ | ✅ | ✅ | ❌ fail |
| q16 | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ | ❌ fail |
| q18 | ✅ | ✅ | ✅ | ✅ |
| q19 | ❌ fail | ✅ | ❌ fail | ✅ |
| q20 | ✅ | ✅ | ✅ | ✅ |
| q21 | ✅ | ✅ | ✅ | ✅ |
| q22 | ✅ | ✅ | ❌ exhausted | ❌ fail |
