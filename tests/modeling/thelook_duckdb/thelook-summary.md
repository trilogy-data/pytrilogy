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

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.099s vs 0.091s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.000s | +0.001s |
| Trilogy vs Reference SQL | -9.3% | +11.9% | +33.9% |
| Trilogy / Reference SQL | 0.91x | 1.12x | 1.34x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02 | 0.004s | 0.003s | +0.002s |
| 21 | 0.011s | 0.009s | +0.001s |
| 19 | 0.015s | 0.013s | +0.001s |
| 10 | 0.004s | 0.003s | +0.001s |
| 16 | 0.004s | 0.003s | +0.001s |
