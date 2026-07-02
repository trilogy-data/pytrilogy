# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 78/99 queries. Total PreQL length is 124,290 chars vs 182,360 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,352 | -216 | +114 |
| PreQL vs Reference SQL | -61.1% | -22.7% | +13.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,543 | 1,089 | +454 |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 60.945s vs 81.889s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.136s | +0.002s | +0.222s |
| Trilogy vs Reference SQL | -52.7% | +4.8% | +204.8% |
| Trilogy / Reference SQL | 0.47x | 1.05x | 3.05x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 64 | 40.463s | 30.773s | +9.689s |
| 05 | 1.261s | 0.104s | +1.157s |
| 35 | 1.222s | 0.273s | +0.950s |
| 21 | 0.812s | 0.032s | +0.780s |
| 10 | 0.843s | 0.232s | +0.611s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,934 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -48 | +287 |
| PreQL vs Reference SQL | -48.6% | -4.1% | +18.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.592s vs 0.385s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.019s | +0.008s | +1.628s |
| Trilogy vs Reference SQL | -19.8% | +13.4% | +2289.4% |
| Trilogy / Reference SQL | 0.80x | 1.13x | 23.89x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.744s | 0.075s | +1.669s |
| 97.1 | 1.634s | 0.067s | +1.566s |
| 30.alt | 0.064s | 0.056s | +0.008s |
