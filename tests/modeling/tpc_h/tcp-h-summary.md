# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,616 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -80 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -17.0% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 2/22 queries. Total Trilogy execution time is 0.897s vs 0.693s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.004s | +0.029s |
| Trilogy vs Reference SQL | +0.1% | +17.4% | +71.2% |
| Trilogy / Reference SQL | 1.00x | 1.17x | 1.71x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.087s | 0.027s | +0.060s |
| 17 | 0.069s | 0.013s | +0.056s |
| 16 | 0.081s | 0.051s | +0.030s |
| 05 | 0.055s | 0.037s | +0.018s |
| 21 | 0.087s | 0.074s | +0.013s |
