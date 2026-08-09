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

import zlib

from trilogy.io import run


def rows() -> list[dict]:
    return [
        {"text": value, "crc32": zlib.crc32(value.encode())}
        for value in sorted(["trilogy", "semantic", "datasource"])
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
