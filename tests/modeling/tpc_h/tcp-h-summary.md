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

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 1.257s vs 0.925s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.005s | +0.003s | +0.055s |
| Trilogy vs Reference SQL | -13.4% | +6.9% | +90.1% |
| Trilogy / Reference SQL | 0.87x | 1.07x | 1.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.135s | 0.034s | +0.100s |
| 17 | 0.103s | 0.018s | +0.085s |
| 16 | 0.124s | 0.067s | +0.058s |
| 21 | 0.133s | 0.099s | +0.034s |
| 05 | 0.073s | 0.050s | +0.023s |
