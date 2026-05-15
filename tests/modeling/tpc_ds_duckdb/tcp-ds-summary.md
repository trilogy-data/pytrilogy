# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,640 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +149 |
| PreQL vs Reference SQL | -57.6% | -20.1% | +11.6% |

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 15.011s vs 63.814s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.152s | +0.002s | +0.094s |
| Trilogy vs Reference SQL | -61.5% | +2.7% | +146.7% |
| Trilogy / Reference SQL | 0.39x | 1.03x | 2.47x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 1.951s vs 0.345s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.001s | +0.055s | +1.079s |
| Trilogy vs Reference SQL | +1.2% | +62.0% | +1265.5% |
| Trilogy / Reference SQL | 1.01x | 1.62x | 13.66x |
