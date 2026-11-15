# TPC-DS Result Summary

Timing fingerprint: `x86_64-x86_64-4`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,749 chars vs 182,755 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -59.5% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,098 | 1,459 | +639 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 20/99 queries. Total Trilogy execution time is 4.287s vs 3.247s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.004s | +0.004s | +0.034s |
| Trilogy vs Reference SQL | -17.9% | +16.2% | +190.0% |
| Trilogy / Reference SQL | 0.82x | 1.16x | 2.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.278s | 0.150s | +0.129s |
| 23 | 0.207s | 0.135s | +0.072s |
| 28 | 0.139s | 0.068s | +0.071s |
| 83 | 0.081s | 0.013s | +0.068s |
| 99 | 0.078s | 0.013s | +0.065s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.263s vs 0.139s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.014s | +0.560s |
| Trilogy vs Reference SQL | -2.1% | +71.9% | +1543.5% |
| Trilogy / Reference SQL | 0.98x | 1.72x | 16.43x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.616s | 0.037s | +0.579s |
| 97.2 | 0.568s | 0.036s | +0.532s |
| 30.alt | 0.033s | 0.019s | +0.014s |
| 2.2 | 0.024s | 0.023s | +0.001s |
