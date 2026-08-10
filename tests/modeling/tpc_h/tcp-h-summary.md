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

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 0.380s vs 0.373s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.001s | +0.007s |
| Trilogy vs Reference SQL | -17.5% | +6.3% | +43.9% |
| Trilogy / Reference SQL | 0.83x | 1.06x | 1.44x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.025s | 0.013s | +0.012s |
| 03 | 0.024s | 0.016s | +0.007s |
| 02 | 0.021s | 0.014s | +0.007s |
| 21 | 0.051s | 0.046s | +0.006s |
| 11 | 0.022s | 0.017s | +0.005s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 356 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -484 | -484 | -484 |
| PreQL vs Reference SQL | -57.6% | -57.6% | -57.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.022s vs 0.013s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.010s | +0.010s | +0.010s |
| Trilogy vs Reference SQL | +77.2% | +77.2% | +77.2% |
| Trilogy / Reference SQL | 1.77x | 1.77x | 1.77x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.022s | 0.013s | +0.010s |
