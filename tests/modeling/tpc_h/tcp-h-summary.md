# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,465 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -65 |
| PreQL vs Reference SQL | -55.6% | -35.7% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 1.000s vs 0.760s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.002s | +0.007s | +0.034s |
| Trilogy vs Reference SQL | -5.3% | +27.5% | +112.6% |
| Trilogy / Reference SQL | 0.95x | 1.27x | 2.13x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 16 | 0.110s | 0.055s | +0.055s |
| 20 | 0.061s | 0.024s | +0.038s |
| 11 | 0.056s | 0.022s | +0.034s |
| 05 | 0.080s | 0.049s | +0.031s |
| 21 | 0.090s | 0.068s | +0.022s |
