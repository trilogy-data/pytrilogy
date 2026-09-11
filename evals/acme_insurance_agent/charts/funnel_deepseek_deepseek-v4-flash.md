# ACME Insurance category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only | 10/11 | q02, q08 | q01, q04, q05, q06, q07, q09, q10, q11 |
| ingest | 8/11 | — | q01, q04, q05, q06, q07, q09, q10, q11 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only | 0.91 | 384,446 | 95,524 | 0/11 | 4 |
| ingest | 0.73 | 1,640,253 | 371,671 | 0/11 | 4 |

## Per-query matrix

| query | db-only | ingest |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ✅ | ❌ fail |
| q03 | ❌ fail | ❌ fail |
| q04 | ✅ | ✅ |
| q05 | ✅ | ✅ |
| q06 | ✅ | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ❌ fail |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
