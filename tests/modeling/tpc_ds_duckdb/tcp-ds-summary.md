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

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 12.717s vs 58.993s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.082s | +0.001s | +0.053s |
| Trilogy vs Reference SQL | -50.3% | +2.4% | +86.8% |
| Trilogy / Reference SQL | 0.50x | 1.02x | 1.87x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.498s | 0.061s | +0.437s |
| 76 | 0.231s | 0.039s | +0.192s |
| 78 | 0.393s | 0.222s | +0.171s |
| 67 | 1.762s | 1.634s | +0.128s |
| 16 | 0.141s | 0.013s | +0.128s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.777s vs 0.357s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.008s | +0.004s | +1.217s |
| Trilogy vs Reference SQL | -10.0% | +13.4% | +1414.4% |
| Trilogy / Reference SQL | 0.90x | 1.13x | 15.14x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.315s | 0.089s | +1.227s |
| 97.1 | 1.286s | 0.084s | +1.203s |
| 30.alt | 0.036s | 0.032s | +0.004s |
