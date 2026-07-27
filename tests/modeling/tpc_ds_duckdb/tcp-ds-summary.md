# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 125,872 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +171 |
| PreQL vs Reference SQL | -60.0% | -21.0% | +13.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 29 | 1,545 | 1,089 | +456 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 15.591s vs 44.613s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.049s | +0.001s | +0.148s |
| Trilogy vs Reference SQL | -41.1% | +1.0% | +197.5% |
| Trilogy / Reference SQL | 0.59x | 1.01x | 2.97x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 76 | 0.659s | 0.103s | +0.556s |
| 05 | 0.671s | 0.197s | +0.474s |
| 83 | 0.503s | 0.074s | +0.429s |
| 23 | 0.748s | 0.403s | +0.345s |
| 70 | 0.349s | 0.054s | +0.294s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,643 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +89 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +5.7% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.181s vs 0.374s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.008s | +0.041s | +0.874s |
| Trilogy vs Reference SQL | +12.3% | +71.5% | +958.2% |
| Trilogy / Reference SQL | 1.12x | 1.72x | 10.58x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.966s | 0.091s | +0.875s |
| 97.1 | 0.964s | 0.092s | +0.873s |
| 30.alt | 0.098s | 0.057s | +0.041s |
| 2.2 | 0.079s | 0.066s | +0.013s |
| 2.1 | 0.074s | 0.069s | +0.005s |
