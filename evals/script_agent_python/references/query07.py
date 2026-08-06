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


def cluster(points: list[int]) -> tuple[list[int], list[float]]:
    centroids = [1.0, 10.0]
    assignments: list[int] = []
    while True:
        updated = [
            min(range(2), key=lambda i: (abs(point - centroids[i]), i))
            for point in points
        ]
        if updated == assignments:
            return assignments, centroids
        assignments = updated
        centroids = [
            sum(point for point, group in zip(points, assignments) if group == i)
            / assignments.count(i)
            for i in range(2)
        ]


points = [1, 2, 3, 10, 11, 12]
assignments, centroids = cluster(points)


def rows() -> list[dict]:
    return [
        {"point": point, "cluster": group, "centroid": centroids[group]}
        for point, group in zip(points, assignments)
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
