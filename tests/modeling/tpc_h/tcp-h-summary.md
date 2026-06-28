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

Trilogy execution is faster than the reference SQL for 2/22 queries. Total Trilogy execution time is 0.964s vs 0.627s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.004s | +0.040s |
| Trilogy vs Reference SQL | +1.1% | +19.0% | +94.1% |
| Trilogy / Reference SQL | 1.01x | 1.19x | 1.94x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.147s | 0.021s | +0.126s |
| 17 | 0.119s | 0.013s | +0.106s |
| 21 | 0.114s | 0.072s | +0.042s |
| 13 | 0.058s | 0.044s | +0.014s |
| 15 | 0.025s | 0.013s | +0.012s |
