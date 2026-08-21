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

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.574s vs 0.556s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.007s | +0.001s | +0.012s |
| Trilogy vs Reference SQL | -29.5% | +7.4% | +48.5% |
| Trilogy / Reference SQL | 0.71x | 1.07x | 1.48x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.042s | 0.024s | +0.017s |
| 03 | 0.042s | 0.027s | +0.015s |
| 02 | 0.036s | 0.024s | +0.012s |
| 16 | 0.064s | 0.055s | +0.009s |
| 11 | 0.042s | 0.034s | +0.008s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 356 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -484 | -484 | -484 |
| PreQL vs Reference SQL | -57.6% | -57.6% | -57.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.034s vs 0.020s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.014s | +0.014s | +0.014s |
| Trilogy vs Reference SQL | +67.3% | +67.3% | +67.3% |
| Trilogy / Reference SQL | 1.67x | 1.67x | 1.67x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.034s | 0.020s | +0.014s |
