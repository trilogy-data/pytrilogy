# TPC-H Result Summary

Timing fingerprint: `x86_64-x86_64-4`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,573 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -449 | -195 | -65 |
| PreQL vs Reference SQL | -50.8% | -36.4% | -16.7% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 3/22 queries. Total Trilogy execution time is 0.898s vs 0.645s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.006s | +0.025s |
| Trilogy vs Reference SQL | -13.6% | +25.7% | +108.4% |
| Trilogy / Reference SQL | 0.86x | 1.26x | 2.08x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 09 | 0.159s | 0.079s | +0.080s |
| 13 | 0.088s | 0.045s | +0.043s |
| 04 | 0.050s | 0.024s | +0.026s |
| 05 | 0.036s | 0.017s | +0.019s |
| 16 | 0.040s | 0.021s | +0.019s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 356 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -484 | -484 | -484 |
| PreQL vs Reference SQL | -57.6% | -57.6% | -57.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.026s vs 0.011s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.015s | +0.015s | +0.015s |
| Trilogy vs Reference SQL | +143.9% | +143.9% | +143.9% |
| Trilogy / Reference SQL | 2.44x | 2.44x | 2.44x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.026s | 0.011s | +0.015s |
