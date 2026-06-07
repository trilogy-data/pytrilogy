# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 128,891 chars vs 182,502 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 24.523s vs 71.633s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.106s | +0.003s | +0.130s |
| Trilogy vs Reference SQL | -47.8% | +8.2% | +149.5% |
| Trilogy / Reference SQL | 0.52x | 1.08x | 2.49x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 65 | 4.740s | 1.115s | +3.624s |
| 66 | 3.212s | 0.320s | +2.892s |
| 67 | 1.730s | 1.133s | +0.597s |
| 05 | 0.604s | 0.075s | +0.529s |
| 78 | 0.583s | 0.275s | +0.308s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.149s vs 0.411s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.024s | +0.004s | +1.067s |
| Trilogy vs Reference SQL | -28.2% | +3.6% | +1086.4% |
| Trilogy / Reference SQL | 0.72x | 1.04x | 11.86x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.871s | 0.099s | +1.772s |
| 30.alt | 0.054s | 0.045s | +0.009s |
| 97.2 | 0.102s | 0.098s | +0.004s |
