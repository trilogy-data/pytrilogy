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

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 1.557s vs 1.105s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.022s | +0.011s | +0.084s |
| Trilogy vs Reference SQL | -29.0% | +28.4% | +151.0% |
| Trilogy / Reference SQL | 0.71x | 1.28x | 2.51x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.164s | 0.058s | +0.106s |
| 16 | 0.204s | 0.115s | +0.088s |
| 11 | 0.141s | 0.054s | +0.086s |
| 17 | 0.088s | 0.023s | +0.065s |
| 13 | 0.088s | 0.054s | +0.034s |
