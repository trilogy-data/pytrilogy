"""Shared reporting for TPC-DS / TPC-H benchmark suites.

Each suite drops `zquery{NN}.log` (per-query SQL + sizes) and
`zquery_timing_{fingerprint}.log` (per-query timings) during test runs;
`analyze(config)` loads them, renders charts (perf, size, flow) into
`{config.root}/...`, and writes a markdown summary.

Conftests call this from session teardown to keep the artifacts fresh.
"""

from __future__ import annotations

import os
import platform
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from os import environ
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tomllib
from matplotlib.ticker import StrMethodFormatter


@dataclass(frozen=True)
class BenchmarkConfig:
    """Per-suite knobs for reporting."""

    name: str  # short label embedded in chart titles, e.g. "TPC-DS"
    file_prefix: str  # output-filename prefix, e.g. "tcp-ds" or "tcp-h"
    root: Path  # directory holding the .log files and where artifacts land
    alt_preql_names: dict[str, str] = field(default_factory=dict)
    # `lang` is "preql" | "sql"; widened here so the per-suite Literal-typed
    # implementations slot in without a cast.
    query_size_fn: Callable[..., int] | None = None

    @property
    def summary_path(self) -> Path:
        return self.root / f"{self.file_prefix}-summary.md"


# Cached so the import-time monkey-patch of TCL_LIBRARY only happens once.
def _ensure_tcl_library() -> None:
    # https://github.com/python/cpython/issues/125235#issuecomment-2412948604
    if environ.get("TCL_LIBRARY"):
        return
    minor = sys.version_info.minor
    if minor == 13:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python313\tcl\tcl8.6"
    elif minor == 12:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python312\tcl\tcl8.6"


def fingerprint() -> str:
    machine = platform.machine()
    cpu_name = platform.processor()
    cpu_count = os.cpu_count()
    return (
        f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
    )


# ---------------------------------------------------------------------------
# loading


def _source_paths(query_id: str, config: BenchmarkConfig) -> tuple[Path, Path]:
    preql_name = config.alt_preql_names.get(query_id, f"query{query_id}.preql")
    base = query_id.split(".", 1)[0]
    sql_name = f"query{base}.sql"
    return config.root / preql_name, config.root / sql_name


def _recompute_sizes(record: dict, config: BenchmarkConfig) -> None:
    if config.query_size_fn is None:
        return
    qid = str(record.get("query_id", ""))
    if not qid:
        return
    preql_path, sql_path = _source_paths(qid, config)
    if preql_path.exists():
        record["preql_size"] = config.query_size_fn(preql_path.read_text(), "preql")
    if sql_path.exists():
        record["comp_size"] = config.query_size_fn(sql_path.read_text(), "sql")
    generated = record.get("generated_sql")
    if isinstance(generated, str):
        record["gen_length"] = config.query_size_fn(generated, "sql")


def load_frames(
    config: BenchmarkConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (all, main, alt) frames. Alt = query_id contains '.'."""
    results: list[dict] = []
    for filename in os.listdir(config.root):
        if not filename.endswith(".log"):
            continue
        # timing log is loaded separately below
        if filename.startswith("zquery_timing_"):
            continue
        with open(config.root / filename, "r") as f:
            raw = f.read()
        if not raw.strip():
            continue
        try:
            loaded = tomllib.loads(raw)
        except tomllib.TOMLDecodeError:
            print(f"Error loading {filename}")
            continue
        _recompute_sizes(loaded, config)
        results.append(loaded)

    timing_path = config.root / f"zquery_timing_{fingerprint()}.log"
    timing: dict = {}
    if timing_path.exists():
        raw = timing_path.read_text()
        if raw.strip():
            try:
                timing = tomllib.loads(raw)
            except tomllib.TOMLDecodeError:
                print(f"Error loading {timing_path.name}")

    final: list[dict] = []
    for record in results:
        q_id = record.get("query_id")
        if q_id is None:
            continue
        timing_key = f"query_{q_id:02d}" if isinstance(q_id, int) else f"query_{q_id}"
        info = timing.get(timing_key)
        if not info:
            continue
        final.append({**record, **info})

    df = pd.DataFrame.from_records(final)
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty
    df["query_id"] = df["query_id"].astype(str)
    is_alt = df["query_id"].str.contains(".", regex=False)
    main_df = df[~is_alt].sort_values("query_id")
    alt_df = df[is_alt].sort_values("query_id")
    return df, main_df, alt_df


# ---------------------------------------------------------------------------
# charts


def _scatter(
    ax,
    positions: list[int],
    series: Iterable[np.ndarray],
    color_for: Callable[[int, np.ndarray], np.ndarray],
    seed: int = 0,
) -> None:
    rng = np.random.default_rng(seed)
    for i, (pos, values) in enumerate(zip(positions, series)):
        if len(values) == 0:
            continue
        jitter = rng.uniform(-0.08, 0.08, size=len(values))
        ax.scatter(
            pos + jitter,
            values,
            s=14,
            alpha=0.55,
            c=color_for(i, values),
            linewidths=0,
        )


def _violin(ax, series: list[np.ndarray], positions: list[int]) -> None:
    parts = ax.violinplot(series, positions=positions, showmedians=True)
    for body in parts["bodies"]:
        body.set_alpha(0.2)
        body.set_facecolor("gray")


def plot_perf(frame: pd.DataFrame, title: str, out_path: Path, show: bool) -> None:
    if frame.empty:
        return
    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.set_xlabel("Generation")
    ax.set_ylabel("Execution time (s, log scale)")
    ax.set_yscale("log")
    exec_times = frame["exec_time"].to_numpy()
    comp_times = frame["comp_time"].to_numpy()
    series = [exec_times, comp_times]
    trilogy_wins = exec_times < comp_times
    duck_wins = comp_times < exec_times
    win_masks = [trilogy_wins, duck_wins]
    counts = [int(trilogy_wins.sum()), int(duck_wins.sum())]
    totals = [float(exec_times.sum()), float(comp_times.sum())]
    labels = [
        f"Trilogy ({counts[0]} wins, {totals[0]:.1f}s total)",
        f"DuckDBDefault ({counts[1]} wins, {totals[1]:.1f}s total)",
    ]
    positions = list(range(1, len(series) + 1))
    _violin(ax, series, positions)

    def color_for(i: int, values: np.ndarray) -> np.ndarray:
        return np.where(win_masks[i], "#2ca02c", "#d62728")

    _scatter(ax, positions, series, color_for)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    if show:
        plt.show()
    else:
        plt.savefig(out_path)
    plt.close(fig)


def plot_sizes(frame: pd.DataFrame, title: str, out_path: Path, show: bool) -> None:
    needed = ["preql_size", "gen_length", "comp_size"]
    if frame.empty or not all(c in frame.columns for c in needed):
        return
    sub = frame.dropna(subset=needed)
    if sub.empty:
        return
    sizes = np.vstack([sub[c].to_numpy(dtype=float) for c in needed])
    mins = sizes.min(axis=0)
    maxs = sizes.max(axis=0)
    series = [sizes[i] for i in range(len(needed))]
    pretty = ["PreQL", "Generated SQL", "Reference SQL"]
    labels: list[str] = []
    for i, name in enumerate(pretty):
        smallest = int((sizes[i] == mins).sum())
        largest = int((sizes[i] == maxs).sum())
        total = int(sizes[i].sum())
        labels.append(f"{name} ({smallest} smallest, {largest} largest, {total} chars)")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(title)
    ax.set_xlabel("Source")
    ax.set_ylabel("Size (chars, symlog scale)")
    ax.set_yscale("symlog", linthresh=1000)
    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    positions = list(range(1, len(series) + 1))
    _violin(ax, series, positions)

    def color_for(i: int, values: np.ndarray) -> np.ndarray:
        is_min = sizes[i] == mins
        is_max = sizes[i] == maxs
        return np.where(is_min, "#2ca02c", np.where(is_max, "#d62728", "#7f7f7f"))

    _scatter(ax, positions, series, color_for)
    ax.set_ylim(0, float(sizes.max()) * 1.08)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=15, ha="right")
    fig.tight_layout()
    if show:
        plt.show()
    else:
        plt.savefig(out_path)
    plt.close(fig)


def plot_flow(frame: pd.DataFrame, title: str, out_path: Path, show: bool) -> None:
    """Parse vs exec stage timings as a violin+dotplot.

    Per-query coloring: a stage's dot is green when *that* stage took less
    time than the other for the same query, red when it took more. Today
    parse usually loses; the goal is for the parse pile to go green.
    """
    needed = ["parse_time", "exec_time"]
    if frame.empty or not all(c in frame.columns for c in needed):
        return
    sub = frame.dropna(subset=needed)
    if sub.empty:
        return
    parse_times = sub["parse_time"].to_numpy(dtype=float)
    exec_times = sub["exec_time"].to_numpy(dtype=float)
    parse_wins = parse_times < exec_times
    exec_wins = exec_times < parse_times
    series = [parse_times, exec_times]
    win_masks = [parse_wins, exec_wins]
    counts = [int(parse_wins.sum()), int(exec_wins.sum())]
    labels = [
        f"parse ({counts[0]} wins, {parse_times.sum():.1f}s total, median {np.median(parse_times):.3f}s)",
        f"exec ({counts[1]} wins, {exec_times.sum():.1f}s total, median {np.median(exec_times):.3f}s)",
    ]
    positions = list(range(1, len(series) + 1))
    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.set_xlabel("Stage")
    ax.set_ylabel("Time (s, log scale)")
    ax.set_yscale("log")
    _violin(ax, series, positions)

    def color_for(i: int, values: np.ndarray) -> np.ndarray:
        return np.where(win_masks[i], "#2ca02c", "#d62728")

    _scatter(ax, positions, series, color_for)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    if show:
        plt.show()
    else:
        plt.savefig(out_path)
    plt.close(fig)


# ---------------------------------------------------------------------------
# summary table


def _numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    typed = frame.copy()
    for column in ("preql_size", "comp_size", "exec_time", "comp_time"):
        typed[column] = pd.to_numeric(typed[column], errors="coerce")
    return typed.dropna(subset=["preql_size", "comp_size", "exec_time", "comp_time"])


def _stats(series: pd.Series) -> tuple[float, float, float]:
    clean = series.dropna()
    return (
        float(clean.quantile(0.1)),
        float(clean.quantile(0.5)),
        float(clean.quantile(0.9)),
    )


def _fmt_chars(value: float) -> str:
    return f"{value:+,.0f}"


def _fmt_seconds(value: float) -> str:
    return f"{value:+,.3f}s"


def _fmt_percent(value: float) -> str:
    return f"{value:+.1f}%"


def _fmt_ratio(value: float) -> str:
    return f"{value:.2f}x"


def _fmt_chars_plain(value: float) -> str:
    return f"{value:,.0f}"


def _fmt_seconds_plain(value: float) -> str:
    return f"{value:,.3f}s"


def _row(label: str, values: tuple[float, float, float], formatter) -> str:
    p10, p50, p90 = values
    return f"| {label} | {formatter(p10)} | {formatter(p50)} | {formatter(p90)} |"


def _top_table(
    title: str,
    frame: pd.DataFrame,
    left: pd.Series,
    right: pd.Series,
    delta: pd.Series,
    mask: pd.Series,
    ascending: bool,
    headers: tuple[str, str, str],
    value_fmt,
    delta_fmt,
) -> list[str]:
    ranked = delta[mask].sort_values(ascending=ascending).head(5)
    lines = [title, ""]
    if ranked.empty:
        return lines + ["None.", ""]
    h_left, h_right, h_delta = headers
    lines.append(f"| Query | {h_left} | {h_right} | {h_delta} |")
    lines.append("| --- | ---: | ---: | ---: |")
    for idx in ranked.index:
        lines.append(
            f"| {frame.at[idx, 'query_id']} | {value_fmt(left[idx])} | "
            f"{value_fmt(right[idx])} | {delta_fmt(delta[idx])} |"
        )
    lines.append("")
    return lines


def _section(title: str, frame: pd.DataFrame) -> list[str]:
    data = _numeric_frame(frame)
    if data.empty:
        return [f"## {title}", "", "No timed query results found.", ""]

    comp_size = data["comp_size"].replace({0: pd.NA})
    comp_time = data["comp_time"].replace({0: pd.NA})
    length_delta = data["preql_size"] - data["comp_size"]
    length_pct_delta = (length_delta / comp_size) * 100
    exec_delta = data["exec_time"] - data["comp_time"]
    exec_pct_delta = (exec_delta / comp_time) * 100
    exec_ratio = data["exec_time"] / comp_time

    length_wins = int((length_delta < 0).sum())
    exec_wins = int((exec_delta < 0).sum())
    total = len(data)

    preql_total = int(data["preql_size"].sum())
    reference_total = int(data["comp_size"].sum())
    trilogy_total = float(data["exec_time"].sum())
    reference_exec_total = float(data["comp_time"].sum())

    lines = [
        f"## {title}",
        "",
        f"Queries: {total}",
        "",
        f"PreQL is shorter than the reference SQL for {length_wins}/{total} queries. "
        f"Total PreQL length is {preql_total:,} chars vs {reference_total:,} reference SQL chars.",
        "",
        "| Length metric | P10 | P50 | P90 |",
        "| --- | ---: | ---: | ---: |",
        _row("PreQL - Reference SQL chars", _stats(length_delta), _fmt_chars),
        _row("PreQL vs Reference SQL", _stats(length_pct_delta), _fmt_percent),
        "",
    ]
    lines.extend(
        _top_table(
            "Top 5 queries where PreQL is longest vs reference SQL",
            data,
            data["preql_size"],
            data["comp_size"],
            length_delta,
            length_delta > 0,
            False,
            ("PreQL chars", "Reference SQL chars", "PreQL - Reference SQL"),
            _fmt_chars_plain,
            _fmt_chars,
        )
    )
    lines.extend(
        [
            f"Trilogy execution is faster than the reference SQL for {exec_wins}/{total} queries. "
            f"Total Trilogy execution time is {trilogy_total:.3f}s vs {reference_exec_total:.3f}s reference SQL time.",
            "",
            "| Performance metric | P10 | P50 | P90 |",
            "| --- | ---: | ---: | ---: |",
            _row("Trilogy - Reference SQL seconds", _stats(exec_delta), _fmt_seconds),
            _row("Trilogy vs Reference SQL", _stats(exec_pct_delta), _fmt_percent),
            _row("Trilogy / Reference SQL", _stats(exec_ratio), _fmt_ratio),
            "",
        ]
    )
    lines.extend(
        _top_table(
            "Top 5 queries where reference SQL is fastest vs Trilogy",
            data,
            data["exec_time"],
            data["comp_time"],
            exec_delta,
            exec_delta > 0,
            False,
            ("Trilogy s", "Reference SQL s", "Trilogy - Reference SQL"),
            _fmt_seconds_plain,
            _fmt_seconds,
        )
    )
    return lines


def build_summary(
    config: BenchmarkConfig,
    main_frame: pd.DataFrame,
    alt_frame: pd.DataFrame,
) -> str:
    lines = [
        f"# {config.name} Result Summary",
        "",
        f"Timing fingerprint: `{fingerprint()}`",
        "",
        "Signed deltas are `PreQL - Reference SQL` for size and "
        "`Trilogy execution - Reference SQL execution` for performance. "
        "Negative values mean PreQL is shorter or Trilogy is faster.",
        "",
    ]
    lines.extend(_section("Suggested Queries", main_frame))
    if not alt_frame.empty:
        lines.extend(_section("Alternative Queries", alt_frame))
    return "\n".join(lines).rstrip() + "\n"


def write_summary(
    config: BenchmarkConfig,
    main_frame: pd.DataFrame,
    alt_frame: pd.DataFrame,
) -> None:
    config.summary_path.write_text(
        build_summary(config, main_frame, alt_frame),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# entrypoint


def analyze(config: BenchmarkConfig, show: bool = False) -> None:
    _ensure_tcl_library()
    df, main_df, alt_df = load_frames(config)
    if df.empty:
        print(f"No timing data for {config.name}; skipping analysis.")
        return

    print(df)

    fp = fingerprint()
    plot_perf(
        main_df,
        f"Query Timing ({config.name})",
        config.root / f"{fp}-{config.file_prefix}-perf.png",
        show,
    )
    if not alt_df.empty:
        plot_perf(
            alt_df,
            f"Query Timing ({config.name} alternatives)",
            config.root / f"{fp}-{config.file_prefix}-perf-alt.png",
            show,
        )
    plot_sizes(
        main_df,
        f"Query Size by Source ({config.name})",
        config.root / f"{config.file_prefix}-size.png",
        show,
    )
    if not alt_df.empty:
        plot_sizes(
            alt_df,
            f"Query Size by Source ({config.name} alternatives)",
            config.root / f"{config.file_prefix}-size-alt.png",
            show,
        )
    write_summary(config, main_df, alt_df)

    plot_flow(
        df,
        f"Flow Time ({config.name})",
        config.root / f"{fp}-{config.file_prefix}-parse-perf.png",
        show,
    )
