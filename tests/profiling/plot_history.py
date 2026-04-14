"""Render ``history.jsonl`` as a PNG chart.

Reads every record from ``history.jsonl`` and writes ``history.png`` alongside
it: a total-parse-time trend on top and a per-file trend on the bottom. Commit
SHAs are used as x-axis labels so regressions can be traced to a commit.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).parent
DEFAULT_HISTORY = HERE / "history.jsonl"
DEFAULT_OUTPUT = HERE / "history.png"


def _load(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"history file not found: {path}")
    records = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    if not records:
        raise SystemExit(f"history file is empty: {path}")
    return records


def _plot(records: list[dict[str, Any]], output: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    xs = list(range(len(records)))
    labels = [
        f"{r.get('git_sha', '?')}\n{r.get('timestamp', '')[:10]}" for r in records
    ]
    totals = [r["total_best_ms"] for r in records]

    file_labels: list[str] = []
    for entry in records[-1]["files"]:
        file_labels.append(entry["label"])

    per_file: dict[str, list[float]] = {label: [] for label in file_labels}
    for r in records:
        by_label = {f["label"]: f["best_ms"] for f in r["files"]}
        for label in file_labels:
            per_file[label].append(by_label.get(label, float("nan")))

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(max(8, len(records) * 0.6 + 4), 9), sharex=True
    )

    ax_top.plot(xs, totals, marker="o", color="#1f77b4", linewidth=2)
    ax_top.set_title("v2 parser total corpus best-of-N parse time")
    ax_top.set_ylabel("total_best_ms")
    ax_top.grid(True, alpha=0.3)
    for x, total in zip(xs, totals):
        ax_top.annotate(
            f"{total:.0f}",
            (x, total),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            fontsize=8,
        )

    cmap = plt.get_cmap("tab20")
    for i, label in enumerate(file_labels):
        ax_bot.plot(
            xs,
            per_file[label],
            marker=".",
            label=label,
            color=cmap(i % 20),
            linewidth=1.2,
        )
    ax_bot.set_title("per-file best_ms")
    ax_bot.set_ylabel("best_ms")
    ax_bot.set_xlabel("run")
    ax_bot.grid(True, alpha=0.3)
    ax_bot.legend(loc="upper left", fontsize=7, ncol=2)

    ax_bot.set_xticks(xs)
    ax_bot.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)

    fig.tight_layout()
    fig.savefig(output, dpi=120)
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    records = _load(args.history)
    _plot(records, args.output)
    print(f"wrote {args.output} ({len(records)} runs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
