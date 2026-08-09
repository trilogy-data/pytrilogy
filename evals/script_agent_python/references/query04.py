#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy"]
#
# # Resolved from this checkout, not PyPI: these references are ground truth
# # for the current code, not whatever wheel was last published.
# [tool.uv.sources]
# pytrilogy = { path = "../../../", editable = true }
# ///

from trilogy.io import run


def distances() -> dict[str, int]:
    graph = {
        "A": [("B", 4), ("C", 2)],
        "B": [("D", 5)],
        "C": [("B", 1), ("D", 8), ("E", 10)],
        "D": [("E", 2)],
        "E": [("D", 2)],
    }
    result = {node: 10**9 for node in graph}
    result["A"] = 0
    pending = set(graph)
    while pending:
        node = min(pending, key=lambda candidate: result[candidate])
        pending.remove(node)
        for neighbor, weight in graph[node]:
            result[neighbor] = min(result[neighbor], result[node] + weight)
    return result


def rows() -> list[dict]:
    return [
        {"node": node, "distance": distance}
        for node, distance in sorted(distances().items())
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
