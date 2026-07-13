# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 123,557 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,432 | -219 | +135 |
| PreQL vs Reference SQL | -61.2% | -21.5% | +10.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,941 | 1,459 | +482 |
| 29 | 1,526 | 1,089 | +437 |
| 35 | 2,175 | 1,745 | +430 |

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 17.246s vs 85.265s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.144s | +0.004s | +0.081s |
| Trilogy vs Reference SQL | -62.1% | +6.5% | +251.0% |
| Trilogy / Reference SQL | 0.38x | 1.07x | 3.51x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 1.343s | 0.405s | +0.938s |
| 05 | 0.761s | 0.084s | +0.678s |
| 73 | 0.520s | 0.048s | +0.472s |
| 76 | 0.386s | 0.070s | +0.316s |
| 28 | 0.274s | 0.051s | +0.223s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,571 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +77 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +4.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,662 | 1,507 | +155 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.593s vs 0.371s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.026s | +0.042s | +1.626s |
| Trilogy vs Reference SQL | -30.5% | +117.4% | +2027.2% |
| Trilogy / Reference SQL | 0.70x | 2.17x | 21.27x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.742s | 0.077s | +1.665s |
| 97.1 | 1.653s | 0.086s | +1.567s |
| 30.alt | 0.077s | 0.036s | +0.042s |
