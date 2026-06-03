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

Trilogy execution is faster than the reference SQL for 4/22 queries. Total Trilogy execution time is 0.750s vs 0.584s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.003s | +0.026s |
| Trilogy vs Reference SQL | -13.1% | +8.6% | +95.7% |
| Trilogy / Reference SQL | 0.87x | 1.09x | 1.96x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.064s | 0.011s | +0.053s |
| 21 | 0.086s | 0.056s | +0.029s |
| 16 | 0.071s | 0.045s | +0.026s |
| 20 | 0.038s | 0.018s | +0.019s |
| 04 | 0.028s | 0.014s | +0.014s |
