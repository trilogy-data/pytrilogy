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

Trilogy execution is faster than the reference SQL for 11/22 queries. Total Trilogy execution time is 0.270s vs 0.283s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.007s | +0.000s | +0.002s |
| Trilogy vs Reference SQL | -45.7% | +0.1% | +16.5% |
| Trilogy / Reference SQL | 0.54x | 1.00x | 1.16x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02 | 0.026s | 0.011s | +0.014s |
| 10 | 0.020s | 0.009s | +0.011s |
| 04 | 0.012s | 0.010s | +0.002s |
| 21 | 0.025s | 0.024s | +0.001s |
| 03 | 0.014s | 0.013s | +0.001s |
