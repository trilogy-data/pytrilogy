# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,604 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -198 | -67 |
| PreQL vs Reference SQL | -55.6% | -33.5% | -16.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 1.090s vs 0.585s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.007s | +0.052s |
| Trilogy vs Reference SQL | -2.0% | +39.0% | +249.8% |
| Trilogy / Reference SQL | 0.98x | 1.39x | 3.50x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 21 | 0.275s | 0.060s | +0.215s |
| 10 | 0.134s | 0.037s | +0.098s |
| 17 | 0.065s | 0.011s | +0.054s |
| 16 | 0.077s | 0.045s | +0.032s |
| 01 | 0.062s | 0.040s | +0.022s |
