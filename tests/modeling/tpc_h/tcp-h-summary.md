# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,586 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -67 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 3/22 queries. Total Trilogy execution time is 1.059s vs 0.672s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.010s | +0.044s |
| Trilogy vs Reference SQL | -2.7% | +38.8% | +135.7% |
| Trilogy / Reference SQL | 0.97x | 1.39x | 2.36x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 10 | 0.156s | 0.045s | +0.112s |
| 17 | 0.083s | 0.014s | +0.069s |
| 16 | 0.116s | 0.070s | +0.047s |
| 20 | 0.053s | 0.027s | +0.026s |
| 04 | 0.038s | 0.016s | +0.022s |
