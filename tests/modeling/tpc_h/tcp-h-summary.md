# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 21/22 queries. Total PreQL length is 9,139 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -449 | -173 | -32 |
| PreQL vs Reference SQL | -49.6% | -31.9% | -11.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 17 | 448 | 328 | +120 |

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.608s vs 0.603s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.010s | +0.001s | +0.011s |
| Trilogy vs Reference SQL | -33.3% | +5.5% | +32.0% |
| Trilogy / Reference SQL | 0.67x | 1.06x | 1.32x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.036s | 0.019s | +0.017s |
| 21 | 0.084s | 0.071s | +0.013s |
| 02 | 0.035s | 0.024s | +0.011s |
| 03 | 0.033s | 0.025s | +0.008s |
| 16 | 0.074s | 0.069s | +0.005s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 390 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -450 | -450 | -450 |
| PreQL vs Reference SQL | -53.6% | -53.6% | -53.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.039s vs 0.021s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.019s | +0.019s | +0.019s |
| Trilogy vs Reference SQL | +90.4% | +90.4% | +90.4% |
| Trilogy / Reference SQL | 1.90x | 1.90x | 1.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.039s | 0.021s | +0.019s |
