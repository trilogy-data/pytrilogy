# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 130,112 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -213 | +186 |
| PreQL vs Reference SQL | -59.5% | -18.9% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 29 | 1,618 | 1,089 | +529 |
| 81 | 1,976 | 1,459 | +517 |

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 28.330s vs 99.911s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.208s | +0.003s | +0.216s |
| Trilogy vs Reference SQL | -52.4% | +5.7% | +134.0% |
| Trilogy / Reference SQL | 0.48x | 1.06x | 2.34x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 19 | 2.175s | 0.848s | +1.327s |
| 22 | 2.702s | 1.565s | +1.137s |
| 37 | 1.679s | 0.999s | +0.680s |
| 68 | 0.690s | 0.162s | +0.528s |
| 78 | 0.741s | 0.325s | +0.417s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.402s vs 0.545s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.051s | +0.003s | +1.171s |
| Trilogy vs Reference SQL | -36.7% | +2.8% | +1110.4% |
| Trilogy / Reference SQL | 0.63x | 1.03x | 12.10x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.049s | 0.106s | +1.944s |
| 30.alt | 0.067s | 0.056s | +0.011s |
| 97.2 | 0.109s | 0.106s | +0.003s |
