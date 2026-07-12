#!/usr/bin/env python
"""Fetch the DABstep context files and task lists from Hugging Face.

Context files (payments.csv etc.) land in ``data/context/`` (gitignored —
re-run this script after a fresh clone). Task lists are small and checked in:
``tasks_dev.json`` (10 tasks WITH answers, used to validate the harness) and
``tasks_all.json`` (the full 450-task set; answers are held out upstream).

Dataset: https://huggingface.co/datasets/adyen/DABstep
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
CONTEXT_DIR = EVAL_DIR / "data" / "context"

RESOLVE_BASE = "https://huggingface.co/datasets/adyen/DABstep/resolve/main/data/context"
ROWS_API = (
    "https://datasets-server.huggingface.co/rows"
    "?dataset=adyen/DABstep&config=tasks&split={split}&offset={offset}&length={length}"
)

CONTEXT_FILES = [
    "acquirer_countries.csv",
    "fees.json",
    "manual.md",
    "merchant_category_codes.csv",
    "merchant_data.json",
    "payments-readme.md",
    "payments.csv",
]


def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "trilogy-dabstep-eval"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def download_context() -> None:
    CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
    for name in CONTEXT_FILES:
        dest = CONTEXT_DIR / name
        if dest.exists() and dest.stat().st_size > 0:
            print(f"  {name}: cached ({dest.stat().st_size:,} bytes)")
            continue
        print(f"  {name}: downloading ...")
        dest.write_bytes(_fetch(f"{RESOLVE_BASE}/{name}"))
        print(f"  {name}: {dest.stat().st_size:,} bytes")


def download_tasks(split: str, dest: Path) -> None:
    rows: list[dict] = []
    offset, page = 0, 100
    while True:
        payload = json.loads(
            _fetch(ROWS_API.format(split=split, offset=offset, length=page))
        )
        batch = [r["row"] for r in payload["rows"]]
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < page:
            break
    dest.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"  {dest.name}: {len(rows)} tasks")


def main() -> None:
    print("[1/2] context files ...")
    download_context()
    print("[2/2] task lists ...")
    download_tasks("dev", EVAL_DIR / "tasks_dev.json")
    download_tasks("default", EVAL_DIR / "tasks_all.json")


if __name__ == "__main__":
    main()
