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

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 0.865s vs 0.663s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.003s | +0.030s |
| Trilogy vs Reference SQL | -9.7% | +7.3% | +94.9% |
| Trilogy / Reference SQL | 0.90x | 1.07x | 1.95x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.070s | 0.011s | +0.059s |
| 16 | 0.098s | 0.052s | +0.046s |
| 21 | 0.104s | 0.073s | +0.030s |
| 20 | 0.049s | 0.025s | +0.024s |
| 05 | 0.055s | 0.037s | +0.018s |
