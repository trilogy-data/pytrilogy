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

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 1.105s vs 0.788s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.015s | +0.008s | +0.045s |
| Trilogy vs Reference SQL | -29.8% | +36.4% | +142.5% |
| Trilogy / Reference SQL | 0.70x | 1.36x | 2.42x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.107s | 0.030s | +0.077s |
| 17 | 0.075s | 0.015s | +0.059s |
| 11 | 0.078s | 0.032s | +0.046s |
| 16 | 0.108s | 0.063s | +0.044s |
| 15 | 0.050s | 0.021s | +0.029s |
