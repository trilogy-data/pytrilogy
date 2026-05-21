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

Trilogy execution is faster than the reference SQL for 4/22 queries. Total Trilogy execution time is 1.245s vs 0.672s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.008s | +0.054s |
| Trilogy vs Reference SQL | -1.7% | +34.3% | +233.3% |
| Trilogy / Reference SQL | 0.98x | 1.34x | 3.33x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 21 | 0.315s | 0.072s | +0.243s |
| 10 | 0.148s | 0.043s | +0.105s |
| 17 | 0.068s | 0.013s | +0.055s |
| 16 | 0.102s | 0.057s | +0.044s |
| 01 | 0.066s | 0.042s | +0.024s |
