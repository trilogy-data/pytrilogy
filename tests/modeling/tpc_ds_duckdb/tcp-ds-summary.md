# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 128,464 chars vs 184,304 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 13.886s vs 54.420s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.092s | +0.002s | +0.057s |
| Trilogy vs Reference SQL | -45.6% | +3.8% | +126.5% |
| Trilogy / Reference SQL | 0.54x | 1.04x | 2.26x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 14 | 1.088s | 0.401s | +0.687s |
| 84 | 0.320s | 0.054s | +0.266s |
| 78 | 0.412s | 0.211s | +0.201s |
| 76 | 0.226s | 0.040s | +0.185s |
| 28 | 0.189s | 0.045s | +0.145s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.855s vs 0.359s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.004s | +0.891s |
| Trilogy vs Reference SQL | +2.7% | +4.8% | +1079.6% |
| Trilogy / Reference SQL | 1.03x | 1.05x | 11.80x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.564s | 0.083s | +1.481s |
| 30.alt | 0.041s | 0.034s | +0.007s |
| 2.1 | 0.081s | 0.078s | +0.004s |
| 2.2 | 0.085s | 0.082s | +0.003s |
| 97.2 | 0.084s | 0.082s | +0.002s |
