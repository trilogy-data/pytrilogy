# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 128,936 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -214 | +186 |
| PreQL vs Reference SQL | -60.7% | -18.9% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 29 | 1,618 | 1,089 | +529 |
| 81 | 1,976 | 1,459 | +517 |

Trilogy execution is faster than the reference SQL for 28/99 queries. Total Trilogy execution time is 17.454s vs 61.608s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.078s | +0.016s | +0.147s |
| Trilogy vs Reference SQL | -44.5% | +17.6% | +249.3% |
| Trilogy / Reference SQL | 0.56x | 1.18x | 3.49x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 0.892s | 0.151s | +0.740s |
| 73 | 0.610s | 0.038s | +0.572s |
| 05 | 0.593s | 0.088s | +0.505s |
| 47 | 0.822s | 0.465s | +0.357s |
| 09 | 0.362s | 0.038s | +0.324s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,436 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,249 | -17 | +477 |
| PreQL vs Reference SQL | -48.7% | -1.5% | +36.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |
| 97.2 | 1,602 | 1,159 | +443 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.193s vs 0.388s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.024s | +0.001s | +1.095s |
| Trilogy vs Reference SQL | -28.2% | +1.7% | +1157.3% |
| Trilogy / Reference SQL | 0.72x | 1.02x | 12.57x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.871s | 0.099s | +1.772s |
| 30.alt | 0.118s | 0.040s | +0.079s |
| 97.2 | 0.081s | 0.080s | +0.001s |
