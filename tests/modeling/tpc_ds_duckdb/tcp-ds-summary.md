# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,322 chars vs 184,304 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -261 | +131 |
| PreQL vs Reference SQL | -20.4% | -20.1% | +9.3% |

Trilogy execution is faster than the reference SQL for 38/99 queries. Total Trilogy execution time is 15.456s vs 58.131s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.431s | +0.003s | +0.106s |
| Trilogy vs Reference SQL | +37.2% | +10.4% | +146.6% |
| Trilogy / Reference SQL | 1.37x | 1.10x | 2.47x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 2/4 queries. Total Trilogy execution time is 1.647s vs 0.385s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.316s | +0.046s | +0.880s |
| Trilogy vs Reference SQL | +403.8% | +52.9% | +1116.3% |
| Trilogy / Reference SQL | 5.04x | 1.53x | 12.16x |
