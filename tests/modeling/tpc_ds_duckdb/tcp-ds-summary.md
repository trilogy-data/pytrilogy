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

Trilogy execution is faster than the reference SQL for 43/99 queries. Total Trilogy execution time is 14.289s vs 58.728s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.081s | +0.002s | +0.066s |
| Trilogy vs Reference SQL | -48.1% | +4.3% | +110.0% |
| Trilogy / Reference SQL | 0.52x | 1.04x | 2.10x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.732s | 0.163s | +0.568s |
| 78 | 0.510s | 0.233s | +0.277s |
| 65 | 0.407s | 0.165s | +0.242s |
| 76 | 0.250s | 0.043s | +0.207s |
| 67 | 1.208s | 1.019s | +0.189s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.840s vs 0.368s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.026s | +0.003s | +0.911s |
| Trilogy vs Reference SQL | -30.5% | +3.3% | +1143.7% |
| Trilogy / Reference SQL | 0.70x | 1.03x | 12.44x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.593s | 0.080s | +1.513s |
| 30.alt | 0.047s | 0.039s | +0.008s |
| 97.2 | 0.082s | 0.079s | +0.003s |
