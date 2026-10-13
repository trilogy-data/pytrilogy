# VGI transport benchmark

How fast does a script datasource reach DuckDB over the current transport
compared with VGI? The question is answered separately for python sources and
rust sources. Comparing python against rust is not the point.

Every source streams N rows of `(id int64, value float64, label string)`.

| mode | what runs per query |
| --- | --- |
| current | the engine's `uv_run` macro. On Windows: a shellfs command runs the retry wrapper, which stages the Arrow stream to a temp file, then `read_arrow` scans it. |
| vgi bridge | `trilogy_io.main.script(...)` on a pooled `trilogy.dialect.duckdb_vgi` worker. It spawns the source each scan and streams its stdout, with no staging. |
| vgi resident | the source *is* a pooled VGI worker, so a query is a function call. This is a prototype and is not wired into the engine. |

Scripts:

- **Python:** `bench.py` (current vs bridge, through the trilogy executor) and `resident_worker.py` + `resident_bench.py`.
- **Rust:** `rust_bench.py` and `rust/`.
  - `rust/` builds `source` (a `trilogy-io` crate source; `--limit N` sets the row count) and `resident` (the same rows from a `vgi` crate worker).
  - A compiled source can't be a datasource address yet, so `rust_bench.py` drives DuckDB with the SQL the engine renders.

```
pip install "pytrilogy[vgi]"                      # haybarn + vgi-python, python >= 3.13
(cd local_scripts/vgi_benchmark/rust && cargo build --release)   # toolchain 1.97 pinned
python local_scripts/vgi_benchmark/bench.py --rows 1000 1000000 10000000 --reps 7
python local_scripts/vgi_benchmark/resident_bench.py local_scripts/vgi_benchmark/resident_worker.py 7 1000 1000000 10000000
python local_scripts/vgi_benchmark/rust_bench.py --rows 1000 1000000 10000000 --reps 7
```

## Results

Machine: Windows 11, 16 cores, duckdb 1.5.3 (current), haybarn 1.5.5rc1 + vgi-python 0.43.1 / vgi 0.43.0 (VGI).

Each cell is the median of 7 warm queries.
- `full_scan` reads all 3 columns.
- `one` is `sum(value)`, which shows projection pushdown.
- Raw JSON is in `results_*.json[l]`.

### Python source

| rows | current full / one | vgi bridge full / one | vgi resident full / one |
| --- | --- | --- | --- |
| 1k | 0.325s / 0.283s | 0.129s / 0.128s | 0.007s / 0.007s |
| 1M | 0.416s / 0.470s | 0.297s / 0.176s | 0.076s / 0.025s |
| 10M | 1.603s / 1.635s | 0.682s / 0.440s | 0.483s / 0.135s |

The bridge still pays `uv run` (~0.1-0.5s) per scan, and that dominates small
results.

### Rust source

| rows | current full / one | vgi bridge full / one | vgi resident full / one |
| --- | --- | --- | --- |
| 1k | 0.178s / 0.171s | 0.017s / 0.017s | 0.007s / 0.006s |
| 1M | 0.30-0.37s / 0.41-0.43s | 0.05-0.07s / 0.046s | 0.041s / 0.014s |
| 10M | 1.311s / 1.461s | 0.285s / 0.245s | 0.278s / 0.094s |

A native binary starts in ~13ms, so the bridge almost reaches the resident
worker. The rest of the current path's cost is the transport itself:

- a shellfs pipe on Windows costs ~0.5s even for `echo`;
- the staged file is 29MB at 1M rows and 293MB at 10M rows.

The 1M ranges come from three runs. One run put the bridge at 0.66s, which two
reruns did not reproduce.

### Disk

On Windows, current stages 29MB (1M) / 290MB (10M) of Arrow per scan. Both VGI
modes stage nothing.

## Caveats

- **Noise is large.**
  - `uv run` measured 0.55-4s on this machine.
  - The current path showed occasional 5-9s first queries, possibly
    antivirus scanning the staged file.
  - Medians are fairly stable, but rerun before quoting a small difference.
- **Setup:** VGI pays the extension load plus a worker spawn at connect (~0.2-3.7s).
  A python resident worker's first start also resolves `vgi-python` into the
  script's uv environment, which took 12-28s once and is cached after.
- **POSIX was not measured.** There the current transport pipes directly with
  no staging, so the gap should be smaller than on Windows.
