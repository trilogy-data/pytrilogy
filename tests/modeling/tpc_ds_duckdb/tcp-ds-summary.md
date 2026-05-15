# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 80/99 queries. Total PreQL length is 139,846 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -269 | +176 |
| PreQL vs Reference SQL | -57.6% | -20.4% | +11.9% |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 13.184s vs 60.234s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.141s | +0.002s | +0.083s |
| Trilogy vs Reference SQL | -61.0% | +5.4% | +149.1% |
| Trilogy / Reference SQL | 0.39x | 1.05x | 2.49x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 1.417s vs 0.317s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.046s | +0.731s |
| Trilogy vs Reference SQL | +2.7% | +59.4% | +945.5% |
| Trilogy / Reference SQL | 1.03x | 1.59x | 10.45x |
