# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 127,662 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -216 | +147 |
| PreQL vs Reference SQL | -60.8% | -18.9% | +17.9% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 29 | 1,620 | 1,089 | +531 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 15.468s vs 65.734s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.153s | +0.001s | +0.058s |
| Trilogy vs Reference SQL | -50.4% | +0.8% | +105.0% |
| Trilogy / Reference SQL | 0.50x | 1.01x | 2.05x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.526s | 0.088s | +0.438s |
| 78 | 0.552s | 0.296s | +0.257s |
| 28 | 0.233s | 0.048s | +0.185s |
| 16 | 0.193s | 0.018s | +0.175s |
| 76 | 0.216s | 0.047s | +0.169s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,976 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -21 | +293 |
| PreQL vs Reference SQL | -48.6% | -1.8% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 3.944s vs 0.440s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.013s | +0.018s | +1.745s |
| Trilogy vs Reference SQL | +20.1% | +21.1% | +1589.1% |
| Trilogy / Reference SQL | 1.20x | 1.21x | 16.89x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.919s | 0.113s | +1.805s |
| 97.1 | 1.758s | 0.104s | +1.654s |
| 2.2 | 0.103s | 0.086s | +0.018s |
| 2.1 | 0.104s | 0.087s | +0.017s |
| 30.alt | 0.060s | 0.049s | +0.010s |
