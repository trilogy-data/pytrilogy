# thelook Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 17/22 queries. Total PreQL length is 3,421 chars vs 5,760 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -364 | -61 | +7 |
| PreQL vs Reference SQL | -63.4% | -29.9% | +12.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 15 | 173 | 112 | +61 |
| 12 | 58 | 51 | +7 |
| 11 | 55 | 48 | +7 |
| 10 | 175 | 170 | +5 |
| 02 | 198 | 194 | +4 |

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 0.119s vs 0.113s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.000s | +0.001s |
| Trilogy vs Reference SQL | -30.9% | +4.7% | +28.4% |
| Trilogy / Reference SQL | 0.69x | 1.05x | 1.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02 | 0.012s | 0.004s | +0.007s |
| 10 | 0.009s | 0.004s | +0.005s |
| 21 | 0.011s | 0.009s | +0.001s |
| 05 | 0.004s | 0.004s | +0.001s |
| 13 | 0.004s | 0.003s | +0.001s |
