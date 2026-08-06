#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


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
rows = [
    {"point": point, "cluster": group, "centroid": centroids[group]}
    for point, group in zip(points, assignments)
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
