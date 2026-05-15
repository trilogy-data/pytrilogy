# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,342 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +131 |
| PreQL vs Reference SQL | -57.6% | -19.2% | +9.3% |

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 14.478s vs 62.280s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.133s | +0.003s | +0.101s |
| Trilogy vs Reference SQL | -53.7% | +9.3% | +201.9% |
| Trilogy / Reference SQL | 0.46x | 1.09x | 3.02x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 1/4 queries. Total Trilogy execution time is 1.950s vs 0.374s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.001s | +0.041s | +1.069s |
| Trilogy vs Reference SQL | +0.8% | +40.9% | +1054.6% |
| Trilogy / Reference SQL | 1.01x | 1.41x | 11.55x |
