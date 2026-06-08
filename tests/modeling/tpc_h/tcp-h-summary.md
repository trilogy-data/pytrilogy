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

Trilogy execution is faster than the reference SQL for 9/22 queries. Total Trilogy execution time is 1.492s vs 1.090s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.017s | +0.005s | +0.085s |
| Trilogy vs Reference SQL | -30.0% | +8.7% | +95.7% |
| Trilogy / Reference SQL | 0.70x | 1.09x | 1.96x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.156s | 0.041s | +0.115s |
| 17 | 0.130s | 0.023s | +0.106s |
| 16 | 0.182s | 0.092s | +0.090s |
| 21 | 0.166s | 0.120s | +0.046s |
| 13 | 0.084s | 0.057s | +0.027s |
