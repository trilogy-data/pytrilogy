# VGI transport benchmark

How fast does a script datasource reach DuckDB over the current transport
compared with VGI? The question is answered separately for python sources and
rust sources. Comparing python against rust is not the point.

Every source streams N rows of `(id int64, value float64, label string)`.

| mode | what runs per query |
| --- | --- |
| current | the engine's `uv_run` macro. On Windows a shellfs command runs the retry wrapper, which stages the Arrow stream to a temp file, then `read_arrow` scans it. |
| vgi bridge | `trilogy_io.main.script(...)` on a pooled `trilogy.dialect.duckdb_vgi` worker. It spawns the source each scan and streams its stdout, with no staging. |
| vgi resident | the source *is* a pooled VGI worker (`run(fn)` / `Source::run` under `--vgi`), so a query is a function call. The engine picks this automatically for a `trilogy.io` / `trilogy-io` source under the vgi transport. |

Scripts:

- **Python:** `bench.py` goes through the trilogy executor. By default the
  generated sources are raw pyarrow scripts, so `vgi` means the bridge; with
  `--trilogy-io` they are `trilogy.io` sources, so `vgi` means resident.
- **Python prototype:** `resident_worker.py` + `resident_bench.py` is the
  hand-written VGI worker the resident mode was modelled on.
- **Rust:** `rust_bench.py` + `rust/`. `rust/` builds `source` (a `trilogy-io`
  source with the `vgi` feature; `--limit N` sets the row count) and `resident`
  (the same rows from a hand-written `vgi` worker). `trilogy_io_resident` is
  `source --vgi`, which is what the engine attaches for a rust datasource.

```
pip install "pytrilogy[vgi]"                      # haybarn + vgi-python, python >= 3.13
(cd local_scripts/vgi_benchmark/rust && cargo build --release)   # toolchain 1.97 pinned
python local_scripts/vgi_benchmark/bench.py --rows 1000 1000000 10000000 --reps 7
python local_scripts/vgi_benchmark/bench.py --trilogy-io --rows 1000 1000000 10000000 --reps 7
python local_scripts/vgi_benchmark/rust_bench.py --rows 1000 1000000 10000000 --reps 7
```

## Results

Machine: Windows 11, 16 cores, duckdb 1.5.3 (current), haybarn 1.5.5rc1 + vgi-python 0.43.1 / vgi 0.43.0 (VGI).

Each cell is the median of 7 warm queries.
- `full_scan` reads all 3 columns.
- `one` is `sum(value)`, which shows projection pushdown.
- Raw JSON is in `results_*.json[l]`.

### Python source

Bridge figures come from `bench.py` (raw scripts). Current and resident figures
come from `bench.py --trilogy-io`, through the executor.

| rows | current full / one | vgi bridge full / one | vgi resident full / one |
| --- | --- | --- | --- |
| 1k | 0.329s / 0.331s | 0.129s / 0.128s | 0.009s / 0.007s |
| 1M | 0.378s / 0.381s | 0.297s / 0.176s | 0.042s / 0.014s |
| 10M | 1.891s / 0.923s | 0.682s / 0.440s | 0.239s / 0.066s |

The bridge still pays `uv run` (~0.1-0.5s) per scan. The resident worker pays
it once, on the first query, which took 1-3.4s including the attach.

### Rust source

| rows | current full / one | vgi bridge full / one | vgi resident (`source --vgi`) full / one |
| --- | --- | --- | --- |
| 1k | 0.162s / 0.163s | 0.017s / 0.017s | 0.007s / 0.007s |
| 1M | 0.229s / 0.230s | 0.053s / 0.032s | 0.042s / 0.013s |
| 10M | 0.788s / 0.847s | 0.300s / 0.180s | 0.269s / 0.066s |

`source --vgi` matched the hand-written `resident` worker to within noise at
every size, so the contract layer costs nothing measurable. A native binary
starts in ~13ms, so the bridge gets most of the way there too. The current
path's cost is the transport itself:

- a shellfs pipe on Windows costs ~0.5s even for `echo`;
- the staged file is 29MB at 1M rows and 293MB at 10M rows.

### Disk

On Windows, current stages 29MB (1M) / 290MB (10M) of Arrow per scan. Both VGI
modes stage nothing.

## Caveats

- **Noise is large.**
  - `uv run` measured 0.55-4s on this machine.
  - The current path showed occasional 5-9s first queries, possibly
    antivirus scanning the staged file.
  - Medians are fairly stable, but rerun before quoting a small difference.
- **Setup:** VGI pays the extension load plus a worker spawn at connect
  (~0.2-3.7s). A python resident worker's first start also resolves `vgi-python`
  into the script's uv environment, which took 12-28s once and is cached after.
- **POSIX was not measured.** There the current transport pipes directly with
  no staging, so the gap should be smaller than on Windows.
