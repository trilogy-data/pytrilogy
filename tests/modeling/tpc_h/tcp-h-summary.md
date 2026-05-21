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

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 0.657s vs 0.468s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.004s | +0.022s |
| Trilogy vs Reference SQL | -5.5% | +18.9% | +111.2% |
| Trilogy / Reference SQL | 0.95x | 1.19x | 2.11x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 21 | 0.090s | 0.043s | +0.047s |
| 17 | 0.050s | 0.009s | +0.041s |
| 01 | 0.062s | 0.040s | +0.022s |
| 10 | 0.044s | 0.028s | +0.017s |
| 20 | 0.024s | 0.012s | +0.013s |
