# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,642 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -65 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -13.2% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 1.012s vs 0.792s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.006s | +0.002s | +0.033s |
| Trilogy vs Reference SQL | -24.9% | +6.1% | +88.4% |
| Trilogy / Reference SQL | 0.75x | 1.06x | 1.88x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.080s | 0.014s | +0.065s |
| 16 | 0.111s | 0.060s | +0.050s |
| 21 | 0.115s | 0.081s | +0.033s |
| 20 | 0.052s | 0.026s | +0.027s |
| 05 | 0.065s | 0.042s | +0.023s |
