# TPC-H v3-vs-v4 parity

- match: 24
- mismatch: 3
- v3_error: 1

| Query | Status | v3 rows | v4 rows |
| --- | --- | --- | --- |
| 01 | match | 4 | 4 |
| 02 | match | 44 | 44 |
| 03 | match | 10 | 10 |
| 04 | match | 5 | 5 |
| 05 | match | 5 | 5 |
| 06 | match | 1 | 1 |
| 07 | match | 4 | 4 |
| 08 | match | 2 | 2 |
| 09 | match | 175 | 175 |
| 10 | match | 20 | 20 |
| 11 | match | 2541 | 2541 |
| 12 | match | 2 | 2 |
| 13 | match | 37 | 37 |
| 14 | match | 1 | 1 |
| 15 | match | 1 | 1 |
| 16 | match | 2762 | 2762 |
| 17 | match | 1 | 1 |
| 18 | match | 5 | 5 |
| 19 | match | 1 | 1 |
| 20 | match | 9 | 9 |
| 21 | match | 47 | 47 |
| 22 | mismatch | 7 | 25 |
| adhoc01 | mismatch | 100 | 100 |
| adhoc02 | v3_error | None | None |
| adhoc03 | mismatch | 1 | 1 |
| adhoc04 | match | 122 | 122 |
| adhoc05 | match | 1 | 1 |
| adhoc07 | match | 20 | 20 |