# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 128,367 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,354 | -231 | +251 |
| PreQL vs Reference SQL | -58.2% | -19.9% | +14.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 76 | 2,344 | 1,708 | +636 |
| 89 | 1,566 | 965 | +601 |
| 64 | 4,256 | 3,783 | +473 |
| 29 | 1,537 | 1,089 | +448 |

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 11.975s vs 51.603s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.096s | +0.002s | +0.059s |
| Trilogy vs Reference SQL | -45.9% | +5.6% | +112.0% |
| Trilogy / Reference SQL | 0.54x | 1.06x | 2.12x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 84 | 0.329s | 0.050s | +0.279s |
| 76 | 0.218s | 0.039s | +0.179s |
| 78 | 0.385s | 0.208s | +0.177s |
| 28 | 0.188s | 0.042s | +0.146s |
| 67 | 1.008s | 0.901s | +0.107s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.842s vs 0.332s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.002s | +0.904s |
| Trilogy vs Reference SQL | -0.4% | +5.0% | +1163.1% |
| Trilogy / Reference SQL | 1.00x | 1.05x | 12.63x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.580s | 0.078s | +1.502s |
| 97.2 | 0.080s | 0.074s | +0.006s |
| 30.alt | 0.033s | 0.031s | +0.002s |
| 2.2 | 0.075s | 0.075s | +0.000s |
