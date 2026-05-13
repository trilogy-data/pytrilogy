import os
import platform
import sys
from os import environ
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tomllib

# Get aggregate info
machine = platform.machine()
cpu_name = platform.processor()
cpu_count = os.cpu_count()

fingerprint = (
    f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
)

# https://github.com/python/cpython/issues/125235#issuecomment-2412948604
if not environ.get("TCL_LIBRARY"):
    minor = sys.version_info.minor
    if minor == 13:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python313\tcl\tcl8.6"
    elif minor == 12:
        environ["TCL_LIBRARY"] = r"C:\Program Files\Python312\tcl\tcl8.6"
    else:
        pass


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
    parts = ax.violinplot(series, positions=positions, showmedians=True)
    for body in parts["bodies"]:
        body.set_alpha(0.2)
        body.set_facecolor("gray")
    rng = np.random.default_rng(0)
    for pos, values, wins in zip(positions, series, win_masks):
        jitter = rng.uniform(-0.08, 0.08, size=len(values))
        colors = np.where(wins, "#2ca02c", "#d62728")
        ax.scatter(pos + jitter, values, s=14, alpha=0.55, c=colors, linewidths=0)
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
    labels = []
    for i, name in enumerate(pretty):
        smallest = int((sizes[i] == mins).sum())
        largest = int((sizes[i] == maxs).sum())
        total = int(sizes[i].sum())
        labels.append(f"{name} ({smallest} smallest, {largest} largest, {total} chars)")

    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.set_xlabel("Source")
    ax.set_ylabel("Size (chars, log scale)")
    ax.set_yscale("log")
    positions = list(range(1, len(series) + 1))
    parts = ax.violinplot(series, positions=positions, showmedians=True)
    for body in parts["bodies"]:
        body.set_alpha(0.2)
        body.set_facecolor("gray")
    rng = np.random.default_rng(0)
    for i, (pos, values) in enumerate(zip(positions, series)):
        is_min = sizes[i] == mins
        is_max = sizes[i] == maxs
        colors = np.where(is_min, "#2ca02c", np.where(is_max, "#d62728", "#7f7f7f"))
        jitter = rng.uniform(-0.08, 0.08, size=len(values))
        ax.scatter(pos + jitter, values, s=14, alpha=0.55, c=colors, linewidths=0)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=15, ha="right")
    fig.tight_layout()
    if show:
        plt.show()
    else:
        plt.savefig(out_path)
    plt.close(fig)


def analyze(show: bool = False):
    results = []
    root = Path(__file__).parent
    for filename in os.listdir(root):
        if filename.endswith(".log"):
            with open(root / filename, "r") as f:
                try:
                    raw = f.read()
                    if not raw.strip():
                        continue
                    loaded = tomllib.loads(raw)
                except tomllib.TOMLDecodeError:
                    print(f"Error loading {filename}")
                    continue
                results.append(loaded)
    timing_path = Path(root / f"zquery_timing_{fingerprint}.log")
    with open(timing_path, "r") as f:
        raw = f.read()
        try:
            timing = tomllib.loads(raw) if raw.strip() else {}
        except tomllib.TOMLDecodeError:
            print(f"Error loading {timing_path.name}")
            timing = {}
    final_results = []
    for x in results:
        if "query_id" not in x:
            continue
        q_id = x["query_id"]
        if isinstance(q_id, int):
            timing_key = f"query_{q_id:02d}"
        else:
            timing_key = f"query_{q_id}"

        time_info = timing.get(timing_key)
        if not time_info:
            continue
        final_results.append({**x, **time_info})

    df = pd.DataFrame.from_records(final_results)

    print(df)

    df["query_id"] = df["query_id"].astype(str)
    is_alt = df["query_id"].str.contains(".", regex=False)
    main_df = df[~is_alt].sort_values("query_id")
    alt_df = df[is_alt].sort_values("query_id")

    plot_perf(
        main_df,
        "Query Timing (suggested)",
        root / f"{fingerprint}-tcp-ds-perf.png",
        show,
    )
    plot_perf(
        alt_df,
        "Query Timing (alternatives)",
        root / f"{fingerprint}-tcp-ds-perf-alt.png",
        show,
    )
    plot_sizes(
        main_df,
        "Query Size by Source",
        root / "tcp-ds-size.png",
        show,
    )
    plot_sizes(
        alt_df,
        "Query Size by Source (alternatives)",
        root / "tcp-ds-size-alt.png",
        show,
    )

    fig, ax = plt.subplots()
    ax.set_title("Flow Time")
    ax.set_xlabel("Stage")
    ax.set_ylabel("Time (s)")

    df["query_id"] = df["query_id"].astype("category")
    df["query_id"] = df["query_id"].cat.set_categories(df["query_id"].unique())

    df = df.sort_values("query_id")

    ax.boxplot([df["parse_time"], df["exec_time"]], tick_labels=["parse", "exec"])
    if show:
        plt.show()
    else:
        plt.savefig(root / f"{fingerprint}-tcp-ds-parse-perf.png")


if __name__ == "__main__":
    analyze()
