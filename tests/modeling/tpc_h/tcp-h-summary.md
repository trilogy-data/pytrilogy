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

Trilogy execution is faster than the reference SQL for 3/22 queries. Total Trilogy execution time is 1.079s vs 0.771s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.005s | +0.061s |
| Trilogy vs Reference SQL | -0.4% | +15.0% | +119.5% |
| Trilogy / Reference SQL | 1.00x | 1.15x | 2.19x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.096s | 0.014s | +0.081s |
| 20 | 0.100s | 0.031s | +0.069s |
| 16 | 0.130s | 0.064s | +0.066s |
| 04 | 0.039s | 0.017s | +0.021s |
| 21 | 0.127s | 0.106s | +0.021s |
