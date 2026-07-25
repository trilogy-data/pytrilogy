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

Trilogy execution is faster than the reference SQL for 37/99 queries. Total Trilogy execution time is 18.721s vs 61.937s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.207s | +0.006s | +0.151s |
| Trilogy vs Reference SQL | -54.8% | +5.1% | +229.7% |
| Trilogy / Reference SQL | 0.45x | 1.05x | 3.30x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 1.130s | 0.589s | +0.541s |
| 05 | 0.689s | 0.152s | +0.537s |
| 70 | 0.411s | 0.116s | +0.294s |
| 83 | 0.323s | 0.069s | +0.254s |
| 73 | 0.270s | 0.041s | +0.229s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 4.479s vs 0.488s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.020s | +0.032s | +2.006s |
| Trilogy vs Reference SQL | -18.8% | +52.5% | +1815.1% |
| Trilogy / Reference SQL | 0.81x | 1.53x | 19.15x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 2.141s | 0.108s | +2.032s |
| 97.1 | 2.079s | 0.114s | +1.965s |
| 30.alt | 0.092s | 0.060s | +0.032s |
