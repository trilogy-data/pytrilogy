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

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 0.849s vs 0.597s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.006s | +0.032s |
| Trilogy vs Reference SQL | -3.2% | +26.7% | +90.0% |
| Trilogy / Reference SQL | 0.97x | 1.27x | 1.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.089s | 0.021s | +0.068s |
| 17 | 0.069s | 0.011s | +0.058s |
| 16 | 0.078s | 0.046s | +0.032s |
| 21 | 0.086s | 0.059s | +0.028s |
| 05 | 0.053s | 0.032s | +0.021s |
