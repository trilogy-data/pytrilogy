# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 128,542 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,354 | -231 | +251 |
| PreQL vs Reference SQL | -58.2% | -18.8% | +14.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 76 | 2,344 | 1,708 | +636 |
| 89 | 1,566 | 965 | +601 |
| 64 | 4,256 | 3,783 | +473 |
| 29 | 1,537 | 1,089 | +448 |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 17.344s vs 64.860s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.108s | +0.003s | +0.098s |
| Trilogy vs Reference SQL | -42.1% | +7.0% | +150.8% |
| Trilogy / Reference SQL | 0.58x | 1.07x | 2.51x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 14 | 1.290s | 0.353s | +0.937s |
| 10 | 0.803s | 0.244s | +0.559s |
| 84 | 0.339s | 0.060s | +0.279s |
| 78 | 0.522s | 0.295s | +0.227s |
| 65 | 0.364s | 0.161s | +0.203s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,272 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,202 | -44 | +356 |
| PreQL vs Reference SQL | -46.8% | -3.8% | +28.5% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1,563 | 1,159 | +404 |
| 30.alt | 1,791 | 1,507 | +284 |

Trilogy execution is faster than the reference SQL for 3/5 queries. Total Trilogy execution time is 2.541s vs 0.497s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.009s | -0.000s | +1.235s |
| Trilogy vs Reference SQL | -5.5% | -0.3% | +1217.8% |
| Trilogy / Reference SQL | 0.94x | 1.00x | 13.18x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.157s | 0.102s | +2.055s |
| 30.alt | 0.047s | 0.043s | +0.005s |
