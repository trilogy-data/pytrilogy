# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,642 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -65 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -13.2% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.876s vs 0.680s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.001s | +0.031s |
| Trilogy vs Reference SQL | -19.3% | +8.6% | +88.0% |
| Trilogy / Reference SQL | 0.81x | 1.09x | 1.88x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 0.075s | 0.013s | +0.063s |
| 16 | 0.097s | 0.054s | +0.043s |
| 21 | 0.102s | 0.070s | +0.033s |
| 20 | 0.047s | 0.025s | +0.022s |
| 04 | 0.036s | 0.019s | +0.017s |
