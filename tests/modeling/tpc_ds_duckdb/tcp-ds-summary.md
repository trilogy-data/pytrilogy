# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,339 chars vs 184,304 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -261 | +131 |
| PreQL vs Reference SQL | -20.4% | -19.2% | +9.3% |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 13.161s vs 60.382s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.477s | +0.003s | +0.093s |
| Trilogy vs Reference SQL | +37.6% | +10.1% | +167.1% |
| Trilogy / Reference SQL | 1.38x | 1.10x | 2.67x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 1/4 queries. Total Trilogy execution time is 1.729s vs 0.323s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.352s | +0.045s | +0.950s |
| Trilogy vs Reference SQL | +452.5% | +56.7% | +1221.9% |
| Trilogy / Reference SQL | 5.52x | 1.57x | 13.22x |
