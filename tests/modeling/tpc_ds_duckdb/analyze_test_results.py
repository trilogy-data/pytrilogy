from pathlib import Path
import os
import pandas as pd
import json
import matplotlib.pyplot as plt
import tomllib


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

    df = pd.DataFrame.from_records(results)

    print(df)

    # Plot the results
    fig, ax = plt.subplots()
    ax.set_title("Query execution time")
    ax.set_xlabel("Query")
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
        plt.savefig(root / "tcp-ds-perf.png")


if __name__ == "__main__":
    analyze(show=True)
