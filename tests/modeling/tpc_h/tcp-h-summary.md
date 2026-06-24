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

Trilogy execution is faster than the reference SQL for 4/22 queries. Total Trilogy execution time is 1.154s vs 0.860s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.004s | +0.054s |
| Trilogy vs Reference SQL | -13.9% | +7.7% | +96.6% |
| Trilogy / Reference SQL | 0.86x | 1.08x | 1.97x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.092s | 0.017s | +0.075s |
| 20 | 0.116s | 0.040s | +0.075s |
| 16 | 0.140s | 0.083s | +0.058s |
| 05 | 0.068s | 0.042s | +0.026s |
| 21 | 0.129s | 0.104s | +0.025s |
