from pathlib import Path


if __name__ == "__main__":
    import os
    import pandas as pd
    import json
    import matplotlib.pyplot as plt

    results = []
    root = Path(__file__).parent
    for filename in os.listdir(root):
        if filename.endswith(".log"):
            with open(root / filename, "r") as f:
                loaded = json.loads(f.read())
                results.append(loaded)
                print(f"----{filename}----")
                print(loaded["generated_sql"])
                print("-------")

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

    plt.show()
