# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,391 chars vs 184,304 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -211 | +131 |
| PreQL vs Reference SQL | -20.3% | -18.9% | +9.0% |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 12.961s vs 59.069s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.466s | +0.003s | +0.084s |
| Trilogy vs Reference SQL | +50.5% | +8.6% | +176.7% |
| Trilogy / Reference SQL | 1.51x | 1.09x | 2.77x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 1/4 queries. Total Trilogy execution time is 1.559s vs 0.293s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.316s | +0.041s | +0.851s |
| Trilogy vs Reference SQL | +439.4% | +56.3% | +1182.8% |
| Trilogy / Reference SQL | 5.39x | 1.56x | 12.83x |
