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

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 19.096s vs 71.904s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.143s | +0.003s | +0.122s |
| Trilogy vs Reference SQL | -45.8% | +3.8% | +110.8% |
| Trilogy / Reference SQL | 0.54x | 1.04x | 2.11x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.928s | 0.095s | +0.834s |
| 78 | 0.581s | 0.277s | +0.305s |
| 76 | 0.296s | 0.054s | +0.242s |
| 25 | 0.245s | 0.066s | +0.179s |
| 67 | 1.402s | 1.225s | +0.177s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.350s vs 0.550s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.055s | +0.003s | +1.142s |
| Trilogy vs Reference SQL | -37.2% | +3.0% | +1126.5% |
| Trilogy / Reference SQL | 0.63x | 1.03x | 12.26x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.999s | 0.102s | +1.897s |
| 30.alt | 0.056s | 0.048s | +0.008s |
| 97.2 | 0.110s | 0.107s | +0.003s |
