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

Trilogy execution is faster than the reference SQL for 3/22 queries. Total Trilogy execution time is 0.740s vs 0.546s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.003s | +0.020s |
| Trilogy vs Reference SQL | -1.7% | +21.3% | +91.2% |
| Trilogy / Reference SQL | 0.98x | 1.21x | 1.91x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.074s | 0.011s | +0.063s |
| 20 | 0.075s | 0.018s | +0.056s |
| 16 | 0.053s | 0.033s | +0.021s |
| 21 | 0.073s | 0.056s | +0.016s |
| 04 | 0.028s | 0.014s | +0.013s |
