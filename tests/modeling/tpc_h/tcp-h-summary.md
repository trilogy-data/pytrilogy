# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,588 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -66 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 0.580s vs 0.458s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.001s | +0.013s |
| Trilogy vs Reference SQL | -5.0% | +5.0% | +128.9% |
| Trilogy / Reference SQL | 0.95x | 1.05x | 2.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.050s | 0.009s | +0.041s |
| 20 | 0.024s | 0.011s | +0.014s |
| 21 | 0.053s | 0.040s | +0.013s |
| 13 | 0.039s | 0.026s | +0.013s |
| 05 | 0.039s | 0.029s | +0.010s |
