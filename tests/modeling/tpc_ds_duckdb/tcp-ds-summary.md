# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 123,980 chars vs 182,368 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -219 | +114 |
| PreQL vs Reference SQL | -60.9% | -22.7% | +12.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,966 | 1,459 | +507 |
| 30 | 1,997 | 1,507 | +490 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 47/99 queries. Total Trilogy execution time is 16.369s vs 85.416s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.137s | +0.002s | +0.084s |
| Trilogy vs Reference SQL | -52.3% | +1.1% | +144.7% |
| Trilogy / Reference SQL | 0.48x | 1.01x | 2.45x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 94 | 1.036s | 0.025s | +1.011s |
| 05 | 0.735s | 0.102s | +0.632s |
| 73 | 0.441s | 0.043s | +0.398s |
| 23 | 0.693s | 0.319s | +0.374s |
| 78 | 0.532s | 0.321s | +0.211s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,906 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +278 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +18.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,997 | 1,507 | +490 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.990s vs 0.368s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.022s | +0.012s | +1.334s |
| Trilogy vs Reference SQL | -22.2% | +26.3% | +2199.4% |
| Trilogy / Reference SQL | 0.78x | 1.26x | 22.99x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.422s | 0.061s | +1.361s |
| 97.2 | 1.353s | 0.060s | +1.293s |
| 30.alt | 0.058s | 0.046s | +0.012s |
