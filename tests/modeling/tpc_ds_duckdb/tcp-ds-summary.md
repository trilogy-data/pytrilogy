# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 128,187 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,354 | -231 | +251 |
| PreQL vs Reference SQL | -59.5% | -19.9% | +14.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 76 | 2,344 | 1,708 | +636 |
| 89 | 1,566 | 965 | +601 |
| 64 | 4,256 | 3,783 | +473 |
| 29 | 1,537 | 1,089 | +448 |

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 18.868s vs 72.299s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.142s | +0.002s | +0.119s |
| Trilogy vs Reference SQL | -51.5% | +7.1% | +128.8% |
| Trilogy / Reference SQL | 0.49x | 1.07x | 2.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 85 | 1.767s | 0.958s | +0.810s |
| 65 | 0.679s | 0.222s | +0.458s |
| 78 | 0.680s | 0.377s | +0.303s |
| 66 | 0.516s | 0.232s | +0.284s |
| 84 | 0.312s | 0.058s | +0.254s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 2.534s vs 0.463s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.003s | +1.241s |
| Trilogy vs Reference SQL | -2.9% | +3.1% | +1057.8% |
| Trilogy / Reference SQL | 0.97x | 1.03x | 11.58x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.178s | 0.118s | +2.060s |
| 30.alt | 0.060s | 0.049s | +0.011s |
| 97.2 | 0.116s | 0.113s | +0.003s |
| 2.2 | 0.091s | 0.088s | +0.003s |
