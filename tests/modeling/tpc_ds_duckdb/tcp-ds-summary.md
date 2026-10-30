# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,156 chars vs 182,755 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -210 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,098 | 1,459 | +639 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 12.406s vs 11.815s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.056s | +0.001s | +0.053s |
| Trilogy vs Reference SQL | -43.6% | +0.7% | +60.8% |
| Trilogy / Reference SQL | 0.56x | 1.01x | 1.61x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.763s | 0.078s | +0.685s |
| 78 | 0.755s | 0.269s | +0.486s |
| 75 | 0.337s | 0.211s | +0.126s |
| 16 | 0.156s | 0.043s | +0.113s |
| 48 | 0.160s | 0.077s | +0.083s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,602 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +64 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +4.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,629 | 1,507 | +122 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.312s vs 0.282s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.008s | +0.035s | +0.509s |
| Trilogy vs Reference SQL | -12.1% | +75.6% | +929.0% |
| Trilogy / Reference SQL | 0.88x | 1.76x | 10.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.582s | 0.057s | +0.525s |
| 97.2 | 0.537s | 0.052s | +0.485s |
| 30.alt | 0.081s | 0.046s | +0.035s |
