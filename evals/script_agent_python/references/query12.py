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


def ordered_tasks() -> list[str]:
    edges = [
        ("clean", "build"),
        ("lint", "test"),
        ("build", "test"),
        ("test", "package"),
    ]
    tasks = sorted({item for edge in edges for item in edge})
    incoming = {
        task: {left for left, right in edges if right == task} for task in tasks
    }
    result: list[str] = []
    while incoming:
        task = min(task for task, required in incoming.items() if not required)
        result.append(task)
        incoming.pop(task)
        for required in incoming.values():
            required.discard(task)
    return result


def rows() -> list[dict]:
    return [{"position": i + 1, "task": task} for i, task in enumerate(ordered_tasks())]


if __name__ == "__main__":
    raise SystemExit(run(rows))
