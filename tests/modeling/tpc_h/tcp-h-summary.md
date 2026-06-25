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

Trilogy execution is faster than the reference SQL for 9/22 queries. Total Trilogy execution time is 1.139s vs 0.858s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.003s | +0.050s |
| Trilogy vs Reference SQL | -8.0% | +9.1% | +86.0% |
| Trilogy / Reference SQL | 0.92x | 1.09x | 1.86x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.115s | 0.038s | +0.077s |
| 17 | 0.093s | 0.016s | +0.077s |
| 16 | 0.120s | 0.069s | +0.051s |
| 21 | 0.132s | 0.096s | +0.035s |
| 04 | 0.044s | 0.024s | +0.021s |
