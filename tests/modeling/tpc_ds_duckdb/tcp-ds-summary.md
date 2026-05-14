# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,339 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +131 |
| PreQL vs Reference SQL | -57.6% | -19.2% | +9.3% |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 12.779s vs 58.306s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.139s | +0.003s | +0.084s |
| Trilogy vs Reference SQL | -58.4% | +9.0% | +174.4% |
| Trilogy / Reference SQL | 0.42x | 1.09x | 2.74x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 2/4 queries. Total Trilogy execution time is 1.468s vs 0.292s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.002s | +0.029s | +0.802s |
| Trilogy vs Reference SQL | -3.2% | +41.4% | +1165.4% |
| Trilogy / Reference SQL | 0.97x | 1.41x | 12.65x |
