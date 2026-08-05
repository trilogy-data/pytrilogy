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

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 0.782s vs 0.777s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.008s | +0.001s | +0.014s |
| Trilogy vs Reference SQL | -32.8% | +6.9% | +27.5% |
| Trilogy / Reference SQL | 0.67x | 1.07x | 1.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.052s | 0.028s | +0.025s |
| 02 | 0.051s | 0.035s | +0.016s |
| 21 | 0.102s | 0.088s | +0.015s |
| 03 | 0.043s | 0.033s | +0.010s |
| 11 | 0.051s | 0.047s | +0.004s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 390 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -450 | -450 | -450 |
| PreQL vs Reference SQL | -53.6% | -53.6% | -53.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.058s vs 0.029s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.029s | +0.029s | +0.029s |
| Trilogy vs Reference SQL | +102.8% | +102.8% | +102.8% |
| Trilogy / Reference SQL | 2.03x | 2.03x | 2.03x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.058s | 0.029s | +0.029s |
