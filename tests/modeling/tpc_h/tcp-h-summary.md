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

Trilogy execution is faster than the reference SQL for 8/22 queries. Total Trilogy execution time is 0.770s vs 0.774s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.016s | +0.000s | +0.015s |
| Trilogy vs Reference SQL | -36.5% | +2.1% | +36.0% |
| Trilogy / Reference SQL | 0.64x | 1.02x | 1.36x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 04 | 0.055s | 0.028s | +0.027s |
| 02 | 0.053s | 0.037s | +0.017s |
| 21 | 0.115s | 0.099s | +0.015s |
| 03 | 0.046s | 0.034s | +0.013s |
| 05 | 0.032s | 0.026s | +0.005s |

## Alternative Queries

Queries: 1

PreQL is shorter than the reference SQL for 1/1 queries. Total PreQL length is 390 chars vs 840 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -450 | -450 | -450 |
| PreQL vs Reference SQL | -53.6% | -53.6% | -53.6% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 0/1 queries. Total Trilogy execution time is 0.065s vs 0.034s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.031s | +0.031s | +0.031s |
| Trilogy vs Reference SQL | +91.6% | +91.6% | +91.6% |
| Trilogy / Reference SQL | 1.92x | 1.92x | 1.92x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 02.region | 0.065s | 0.034s | +0.031s |
