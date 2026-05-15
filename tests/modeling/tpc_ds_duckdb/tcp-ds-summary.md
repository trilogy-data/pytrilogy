# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,365 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +131 |
| PreQL vs Reference SQL | -57.6% | -19.2% | +9.3% |

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 15.675s vs 69.152s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.144s | +0.004s | +0.092s |
| Trilogy vs Reference SQL | -55.6% | +6.2% | +169.3% |
| Trilogy / Reference SQL | 0.44x | 1.06x | 2.69x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 2.031s vs 0.368s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.004s | +0.051s | +1.119s |
| Trilogy vs Reference SQL | +4.6% | +49.0% | +1123.9% |
| Trilogy / Reference SQL | 1.05x | 1.49x | 12.24x |
