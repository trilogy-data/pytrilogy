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

Trilogy execution is faster than the reference SQL for 4/22 queries. Total Trilogy execution time is 1.019s vs 0.736s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.002s | +0.003s | +0.038s |
| Trilogy vs Reference SQL | -7.5% | +19.3% | +95.2% |
| Trilogy / Reference SQL | 0.92x | 1.19x | 1.95x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.085s | 0.014s | +0.072s |
| 10 | 0.087s | 0.044s | +0.042s |
| 21 | 0.126s | 0.088s | +0.038s |
| 20 | 0.063s | 0.029s | +0.034s |
| 16 | 0.082s | 0.054s | +0.028s |
