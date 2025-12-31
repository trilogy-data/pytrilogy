#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow"]
# ///

import sys

import pyarrow as pa


def fibonacci(n: int) -> list[int]:
    if n <= 0:
        return []
    if n == 1:
        return [0]
    fibs = [0, 1]
    for _ in range(2, n):
        fibs.append(fibs[-1] + fibs[-2])
    return fibs


def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    n = 25  # Keep small to avoid int overflow in pyarrow
    fibs = fibonacci(n)
    table = pa.table({"index": list(range(1, n + 1)), "fibonacci": fibs})
    emit(table)
