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

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 15.762s vs 61.176s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.108s | +0.002s | +0.080s |
| Trilogy vs Reference SQL | -41.8% | +4.2% | +130.7% |
| Trilogy / Reference SQL | 0.58x | 1.04x | 2.31x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 66 | 0.636s | 0.217s | +0.418s |
| 65 | 0.551s | 0.201s | +0.350s |
| 84 | 0.328s | 0.061s | +0.266s |
| 69 | 0.491s | 0.243s | +0.248s |
| 78 | 0.490s | 0.245s | +0.245s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 2.524s vs 0.427s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.007s | +1.253s |
| Trilogy vs Reference SQL | -0.5% | +6.7% | +1175.1% |
| Trilogy / Reference SQL | 1.00x | 1.07x | 12.75x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.190s | 0.107s | +2.083s |
| 97.2 | 0.117s | 0.110s | +0.007s |
| 30.alt | 0.047s | 0.040s | +0.007s |
| 2.1 | 0.084s | 0.082s | +0.002s |
