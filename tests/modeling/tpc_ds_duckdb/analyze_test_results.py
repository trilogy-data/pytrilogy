import json
import os
import platform
import sys
from os import environ
from pathlib import Path

import matplotlib.pyplot as plt
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


def analyze(show: bool = False):
    results = []
    root = Path(__file__).parent
    for filename in os.listdir(root):
        if filename.endswith(".log"):
            with open(root / filename, "r") as f:
                try:
                    loaded = tomllib.loads(f.read())
                except json.decoder.JSONDecodeError:
                    print(f"Error loading {filename}")
                    continue
                results.append(loaded)
    with open(root / "zquery_timing.log", "r") as f:
        timing = tomllib.loads(f.read())
    final_results = []
    for x in results:
        if "query_id" not in x:
            continue
        q_id = x["query_id"]
        time_info = timing.get(f"query_{q_id:02d}")
        if not time_info:
            continue
        final_results.append({**x, **time_info})

    df = pd.DataFrame.from_records(final_results)

    print(df)

    # Plot the results
    fig, ax = plt.subplots()
    ax.set_title("Query Timing")
    ax.set_xlabel("Generation")
    ax.set_ylabel("Execution time (s)")

    df["query_id"] = df["query_id"].astype("category")
    df["query_id"] = df["query_id"].cat.set_categories(df["query_id"].unique())

    df = df.sort_values("query_id")

    ax.boxplot(
        [df["exec_time"], df["comp_time"]], tick_labels=["Trilogy", "DuckDBDefault"]
    )
    if show:
        plt.show()
    else:
        plt.savefig(root / f"{fingerprint}-tcp-ds-perf.png")

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
    analyze(show=True)
