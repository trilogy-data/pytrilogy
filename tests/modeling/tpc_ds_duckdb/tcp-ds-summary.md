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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 21.324s vs 72.012s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.182s | +0.006s | +0.160s |
| Trilogy vs Reference SQL | -53.4% | +7.7% | +233.5% |
| Trilogy / Reference SQL | 0.47x | 1.08x | 3.33x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 1.320s | 0.628s | +0.692s |
| 05 | 0.724s | 0.147s | +0.577s |
| 70 | 0.482s | 0.129s | +0.353s |
| 83 | 0.365s | 0.073s | +0.291s |
| 73 | 0.336s | 0.045s | +0.291s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 5.169s vs 0.543s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.022s | +0.040s | +2.318s |
| Trilogy vs Reference SQL | -19.8% | +61.5% | +1822.6% |
| Trilogy / Reference SQL | 0.80x | 1.62x | 19.23x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 2.458s | 0.125s | +2.333s |
| 97.1 | 2.426s | 0.130s | +2.296s |
| 30.alt | 0.106s | 0.066s | +0.040s |
