# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,604 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -67 |
| PreQL vs Reference SQL | -55.6% | -33.5% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 0.663s vs 0.488s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.005s | +0.026s |
| Trilogy vs Reference SQL | -6.1% | +24.5% | +116.3% |
| Trilogy / Reference SQL | 0.94x | 1.24x | 2.16x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 21 | 0.091s | 0.042s | +0.049s |
| 17 | 0.053s | 0.012s | +0.041s |
| 01 | 0.068s | 0.040s | +0.028s |
| 20 | 0.023s | 0.012s | +0.011s |
| 10 | 0.044s | 0.034s | +0.010s |
