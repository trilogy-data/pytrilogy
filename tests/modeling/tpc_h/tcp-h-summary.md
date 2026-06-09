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

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 1.492s vs 1.146s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.021s | +0.005s | +0.046s |
| Trilogy vs Reference SQL | -21.0% | +8.0% | +88.3% |
| Trilogy / Reference SQL | 0.79x | 1.08x | 1.88x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.163s | 0.042s | +0.121s |
| 17 | 0.123s | 0.025s | +0.098s |
| 16 | 0.137s | 0.090s | +0.048s |
| 05 | 0.093s | 0.057s | +0.035s |
| 21 | 0.142s | 0.113s | +0.029s |
