# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,606 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -66 |
| PreQL vs Reference SQL | -55.6% | -33.5% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 0.680s vs 0.491s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.005s | +0.017s |
| Trilogy vs Reference SQL | -3.7% | +16.9% | +105.9% |
| Trilogy / Reference SQL | 0.96x | 1.17x | 2.06x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.054s | 0.010s | +0.044s |
| 21 | 0.085s | 0.041s | +0.044s |
| 10 | 0.047s | 0.030s | +0.017s |
| 13 | 0.039s | 0.025s | +0.014s |
| 04 | 0.025s | 0.013s | +0.013s |
