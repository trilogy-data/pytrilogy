# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,341 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.0% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 19.979s vs 83.440s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.071s | +0.003s | +0.180s |
| Trilogy vs Reference SQL | -44.1% | +5.7% | +254.6% |
| Trilogy / Reference SQL | 0.56x | 1.06x | 3.55x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 10 | 1.121s | 0.128s | +0.993s |
| 11 | 1.233s | 0.315s | +0.918s |
| 05 | 0.887s | 0.219s | +0.667s |
| 23 | 1.020s | 0.491s | +0.530s |
| 83 | 0.446s | 0.065s | +0.381s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.365s vs 1.298s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.009s | +0.029s | +0.594s |
| Trilogy vs Reference SQL | +9.1% | +41.0% | +759.6% |
| Trilogy / Reference SQL | 1.09x | 1.41x | 8.60x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.015s | 0.079s | +0.936s |
| 30.alt | 0.152s | 0.071s | +0.081s |
| 2.1 | 0.100s | 0.071s | +0.029s |
| 2.2 | 0.093s | 0.077s | +0.017s |
| 97.1 | 1.004s | 1.001s | +0.003s |
