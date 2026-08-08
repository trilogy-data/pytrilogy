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

import re
from collections import Counter

from trilogy.io import run

counts = Counter(
    re.findall(r"[a-z]+", "To be, or not to be: that is the question.".lower())
)


def rows() -> list[dict]:
    return [
        {"word": word, "count": count}
        for word, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


if __name__ == "__main__":
    raise SystemExit(run(rows))
