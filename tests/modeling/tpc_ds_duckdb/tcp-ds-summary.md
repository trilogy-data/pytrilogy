# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,332 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

<<<<<<< HEAD
Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 16.906s vs 17.086s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.086s | +0.001s | +0.083s |
| Trilogy vs Reference SQL | -42.3% | +0.6% | +94.5% |
| Trilogy / Reference SQL | 0.58x | 1.01x | 1.94x |
=======
Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 17.867s vs 93.059s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.060s | +0.002s | +0.154s |
| Trilogy vs Reference SQL | -42.1% | +3.4% | +215.8% |
| Trilogy / Reference SQL | 0.58x | 1.03x | 3.16x |
>>>>>>> d0bc69d63 (partial_work)

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
<<<<<<< HEAD
| 05 | 0.863s | 0.231s | +0.633s |
| 23 | 1.018s | 0.531s | +0.486s |
| 78 | 0.866s | 0.480s | +0.386s |
| 70 | 0.412s | 0.084s | +0.328s |
| 35 | 0.505s | 0.235s | +0.270s |
=======
| 23 | 1.520s | 0.401s | +1.118s |
| 78 | 1.091s | 0.354s | +0.737s |
| 05 | 0.817s | 0.158s | +0.659s |
| 83 | 0.538s | 0.074s | +0.465s |
| 70 | 0.348s | 0.055s | +0.293s |
>>>>>>> d0bc69d63 (partial_work)

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,643 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +89 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +5.7% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

<<<<<<< HEAD
Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.105s vs 0.503s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.021s | +0.058s | +0.760s |
| Trilogy vs Reference SQL | +19.8% | +80.2% | +701.2% |
| Trilogy / Reference SQL | 1.20x | 1.80x | 8.01x |
=======
Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.366s vs 0.357s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.018s | +0.053s | +0.969s |
| Trilogy vs Reference SQL | +27.0% | +87.9% | +1163.9% |
| Trilogy / Reference SQL | 1.27x | 1.88x | 12.64x |
>>>>>>> d0bc69d63 (partial_work)

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
<<<<<<< HEAD
| 97.1 | 0.895s | 0.104s | +0.791s |
| 97.2 | 0.829s | 0.117s | +0.712s |
| 30.alt | 0.130s | 0.072s | +0.058s |
| 2.1 | 0.130s | 0.109s | +0.021s |
| 2.2 | 0.121s | 0.101s | +0.021s |
=======
| 97.1 | 1.094s | 0.088s | +1.006s |
| 97.2 | 0.992s | 0.077s | +0.915s |
| 30.alt | 0.112s | 0.060s | +0.053s |
| 2.2 | 0.083s | 0.062s | +0.020s |
| 2.1 | 0.085s | 0.069s | +0.016s |
>>>>>>> d0bc69d63 (partial_work)
