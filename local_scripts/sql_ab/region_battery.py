"""Run a row battery over a two-region model; write {query: rows}.

    python local_scripts/sql_ab/region_battery.py <two|channels|channels_union|lines> <out.json>
    python local_scripts/sql_ab/region_battery.py diff <a.json> <b.json> [<c.json> ...]

Run it from the root of the tree under test (cwd goes first on sys.path) to
compare a branch against main or a pre-change commit, then triage each
difference by hand. The corpus and the suite are blind to most of what this
finds (docs/handoff_grain_pin_followups.md, "two-region probe").
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.getcwd())

from trilogy import Dialects

_CHANNEL_QUERIES = (
    [
        ["bucket", "channel"],
        ["customer_id", "channel"],
        ["bucket", "channel", "fee"],
        ["customer_id", "bucket", "channel"],
        ["channel", "target"],
    ],
    [
        "count(order_id) as n",
        "sum(amount) as s",
        "sum(fee) by channel as f",
        "sum(target) by bucket as t",
        "count(order_id) by channel as nc",
        "max(fee) as mf",
    ],
    [
        "channel is null",
        "bucket is null",
        "fee > 55",
        "bucket = 'z' or channel = 'shop'",
    ],
)

SPECS = {
    "two": (
        "tests.engine.test_padded_null_pairing:TWO_REGIONS",
        [
            ["customer_id"],
            ["bucket"],
            ["name", "bucket"],
            ["customer_id", "bucket"],
            ["customer_id", "target"],
        ],
        [
            "count(order_id) as n",
            "sum(amount) as s",
            "sum(target) by bucket as t",
            "count(customer_id) by bucket as cb",
            "sum(amount) by customer_id as a",
            "max(target) as mt",
        ],
        [
            "name is null",
            "bucket is null",
            "name = 'ann'",
            "target > 6",
            "amount is null",
            "bucket = 'z' or name = 'cat'",
        ],
    ),
    "channels": (
        "tests.engine.test_padded_null_pairing:SECOND_OPTIONAL_KEY",
        *_CHANNEL_QUERIES,
    ),
    "channels_union": (
        "tests.engine.test_padded_null_pairing:SECOND_OPTIONAL_KEY_UNION",
        *_CHANNEL_QUERIES,
    ),
    "lines": (
        "tests.helpers.models:LINE_ITEMS",
        [
            ["user_id"],
            ["product_id"],
            ["state", "product_id"],
            ["user_id", "product_id"],
            ["user_id", "cost"],
        ],
        [
            "revenue",
            "margin",
            "count(line_id) as n",
            "sum(sale_price) by user_id as su",
            "sum(cost) by product_id as sc",
            "count(user_id) by product_id as cu",
        ],
        [
            "state is null",
            "cost is null",
            "state = 'wa'",
            "cost > 1.5",
            "sale_price is null",
            "user_id = 3 or product_id = 3",
        ],
    ),
}


def _model(ref: str) -> str:
    module, name = ref.split(":")
    return getattr(__import__(module, fromlist=[name]), name)


def _rows(executor, query: str) -> str:
    try:
        rows = executor.execute_text(query + ";")[-1].fetchall()
    except Exception as e:
        return f"ERR {type(e).__name__}: {str(e)[:160]}"
    return repr(
        sorted(
            (tuple(r) for r in rows),
            key=lambda r: tuple((v is None, str(v)) for v in r),
        )
    )


def run(spec: str, out: str) -> None:
    ref, dims, aggs, wheres = SPECS[spec]
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_model(ref))
    result = {
        query: _rows(executor, query)
        for query in (
            f"select {', '.join(d)}, {a}{f' where {w}' if w else ''}"
            for d in dims
            for a in aggs
            for w in ["", *wheres]
        )
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)
    errors = sum(1 for v in result.values() if v.startswith("ERR"))
    print(f"{len(result)} queries, {errors} errors -> {out}")


def diff(paths: list[str]) -> None:
    runs = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            runs.append(json.load(f))
    moved = [q for q in runs[0] if len({r.get(q) for r in runs}) > 1]
    print(f"{len(moved)} of {len(runs[0])} differ")
    for query in moved:
        print(query)
        for path, run_ in zip(paths, runs):
            print(f"   {os.path.basename(path)}: {run_.get(query, '')[:200]}")


if __name__ == "__main__":
    if sys.argv[1] == "diff":
        diff(sys.argv[2:])
    else:
        run(sys.argv[1], sys.argv[2])
