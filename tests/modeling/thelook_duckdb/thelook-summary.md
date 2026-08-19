# thelook Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 18

PreQL is shorter than the reference SQL for 13/18 queries. Total PreQL length is 2,672 chars vs 3,479 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -99 | -46 | +7 |
| PreQL vs Reference SQL | -46.6% | -16.9% | +14.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 15 | 173 | 112 | +61 |
| 12 | 58 | 51 | +7 |
| 11 | 55 | 48 | +7 |
| 10 | 175 | 170 | +5 |
| 02 | 198 | 194 | +4 |

Trilogy execution is faster than the reference SQL for 7/18 queries. Total Trilogy execution time is 0.126s vs 0.879s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.002s | +0.000s | +0.002s |
| Trilogy vs Reference SQL | -25.2% | +7.3% | +101.0% |
| Trilogy / Reference SQL | 0.75x | 1.07x | 2.01x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02 | 0.012s | 0.004s | +0.008s |
| 10 | 0.007s | 0.002s | +0.004s |
| 08 | 0.003s | 0.002s | +0.001s |
| 16 | 0.003s | 0.003s | +0.001s |
| 03 | 0.005s | 0.005s | +0.001s |
