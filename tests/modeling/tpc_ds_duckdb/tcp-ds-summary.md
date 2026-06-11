# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 70/99 queries. Total PreQL length is 129,080 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -216 | +186 |
| PreQL vs Reference SQL | -60.7% | -18.9% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 29 | 1,620 | 1,089 | +531 |
| 81 | 1,976 | 1,459 | +517 |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 24.013s vs 90.419s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.188s | +0.002s | +0.204s |
| Trilogy vs Reference SQL | -48.6% | +3.3% | +138.9% |
| Trilogy / Reference SQL | 0.51x | 1.03x | 2.39x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 1.140s | 0.158s | +0.982s |
| 68 | 0.659s | 0.181s | +0.478s |
| 65 | 0.711s | 0.305s | +0.406s |
| 67 | 2.340s | 1.976s | +0.364s |
| 66 | 0.746s | 0.436s | +0.310s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 4.426s vs 0.646s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.015s | +0.029s | +1.871s |
| Trilogy vs Reference SQL | +8.4% | +42.9% | +1691.0% |
| Trilogy / Reference SQL | 1.08x | 1.43x | 17.91x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 2.026s | 0.114s | +1.911s |
| 97.1 | 1.916s | 0.106s | +1.810s |
| 30.alt | 0.095s | 0.066s | +0.029s |
| 2.2 | 0.190s | 0.174s | +0.015s |
| 2.1 | 0.200s | 0.185s | +0.015s |
