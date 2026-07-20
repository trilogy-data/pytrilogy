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

Trilogy execution is faster than the reference SQL for 5/22 queries. Total Trilogy execution time is 1.435s vs 1.032s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.018s | +0.009s | +0.066s |
| Trilogy vs Reference SQL | -27.7% | +24.3% | +170.6% |
| Trilogy / Reference SQL | 0.72x | 1.24x | 2.71x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.154s | 0.055s | +0.099s |
| 11 | 0.120s | 0.044s | +0.077s |
| 16 | 0.175s | 0.108s | +0.067s |
| 17 | 0.077s | 0.019s | +0.057s |
| 04 | 0.055s | 0.025s | +0.030s |
