# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,606 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -80 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -17.0% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 0.666s vs 0.506s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.003s | +0.018s |
| Trilogy vs Reference SQL | -7.1% | +15.6% | +93.1% |
| Trilogy / Reference SQL | 0.93x | 1.16x | 1.93x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.069s | 0.015s | +0.054s |
| 17 | 0.061s | 0.009s | +0.052s |
| 16 | 0.048s | 0.030s | +0.018s |
| 05 | 0.042s | 0.031s | +0.011s |
| 04 | 0.022s | 0.012s | +0.010s |
