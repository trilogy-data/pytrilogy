# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,616 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -80 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -17.0% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 4/22 queries. Total Trilogy execution time is 1.306s vs 0.938s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.019s | +0.009s | +0.060s |
| Trilogy vs Reference SQL | -28.4% | +22.7% | +155.8% |
| Trilogy / Reference SQL | 0.72x | 1.23x | 2.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.127s | 0.042s | +0.085s |
| 11 | 0.117s | 0.045s | +0.072s |
| 16 | 0.137s | 0.076s | +0.061s |
| 17 | 0.070s | 0.016s | +0.054s |
| 05 | 0.092s | 0.049s | +0.043s |
