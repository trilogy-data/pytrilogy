# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

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

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 0.708s vs 0.704s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.008s | +0.001s | +0.013s |
| Trilogy vs Reference SQL | -26.7% | +4.8% | +39.1% |
| Trilogy / Reference SQL | 0.73x | 1.05x | 1.39x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.042s | 0.022s | +0.020s |
| 03 | 0.041s | 0.027s | +0.014s |
| 11 | 0.059s | 0.046s | +0.013s |
| 02 | 0.038s | 0.027s | +0.011s |
| 21 | 0.097s | 0.089s | +0.008s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 356 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -484 | -484 | -484 |
| PreQL vs Reference SQL | -57.6% | -57.6% | -57.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.041s vs 0.024s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.018s | +0.018s | +0.018s |
| Trilogy vs Reference SQL | +75.7% | +75.7% | +75.7% |
| Trilogy / Reference SQL | 1.76x | 1.76x | 1.76x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.041s | 0.024s | +0.018s |
