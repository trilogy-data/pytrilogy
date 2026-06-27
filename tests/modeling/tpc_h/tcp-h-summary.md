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

Trilogy execution is faster than the reference SQL for 3/22 queries. Total Trilogy execution time is 0.865s vs 0.627s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.005s | +0.029s |
| Trilogy vs Reference SQL | -6.9% | +24.0% | +87.7% |
| Trilogy / Reference SQL | 0.93x | 1.24x | 1.88x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.098s | 0.022s | +0.076s |
| 17 | 0.066s | 0.011s | +0.055s |
| 16 | 0.075s | 0.045s | +0.030s |
| 21 | 0.084s | 0.065s | +0.019s |
| 05 | 0.051s | 0.033s | +0.018s |
