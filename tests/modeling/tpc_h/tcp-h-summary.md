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

Trilogy execution is faster than the reference SQL for 7/22 queries. Total Trilogy execution time is 0.248s vs 0.249s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.002s | +0.000s | +0.005s |
| Trilogy vs Reference SQL | -17.6% | +7.9% | +43.4% |
| Trilogy / Reference SQL | 0.82x | 1.08x | 1.43x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.018s | 0.010s | +0.008s |
| 03 | 0.018s | 0.012s | +0.006s |
| 02 | 0.014s | 0.010s | +0.005s |
| 21 | 0.024s | 0.022s | +0.002s |
| 16 | 0.022s | 0.020s | +0.002s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 390 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -450 | -450 | -450 |
| PreQL vs Reference SQL | -53.6% | -53.6% | -53.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.014s vs 0.009s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.005s | +0.005s | +0.005s |
| Trilogy vs Reference SQL | +57.7% | +57.7% | +57.7% |
| Trilogy / Reference SQL | 1.58x | 1.58x | 1.58x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.014s | 0.009s | +0.005s |
