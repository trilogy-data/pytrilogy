# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,573 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -449 | -195 | -65 |
| PreQL vs Reference SQL | -50.8% | -36.4% | -16.7% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.567s vs 0.554s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.005s | +0.001s | +0.011s |
| Trilogy vs Reference SQL | -19.2% | +3.4% | +39.1% |
| Trilogy / Reference SQL | 0.81x | 1.03x | 1.39x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.035s | 0.019s | +0.016s |
| 21 | 0.080s | 0.068s | +0.012s |
| 11 | 0.044s | 0.032s | +0.012s |
| 03 | 0.034s | 0.023s | +0.010s |
| 02 | 0.031s | 0.023s | +0.008s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 356 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -484 | -484 | -484 |
| PreQL vs Reference SQL | -57.6% | -57.6% | -57.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.035s vs 0.019s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.016s | +0.016s | +0.016s |
| Trilogy vs Reference SQL | +82.4% | +82.4% | +82.4% |
| Trilogy / Reference SQL | 1.82x | 1.82x | 1.82x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.035s | 0.019s | +0.016s |
