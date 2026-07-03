#!/usr/bin/env python
"""Token-contribution report for the agent-info reference (trilogy/ai/constants.py).

The agent-info text is re-sent every turn, so its size is the single biggest lever
on per-query token churn. This breaks it into the blocks an editor can actually
move - the `##` (and optionally `###`) sections of RULE_PROMPT, plus the rendered
FUNCTIONS / AGGREGATE_FUNCTIONS / FUNCTION_EXAMPLES constants - and ranks them by
token cost so you can keep the high-value blocks as defaults and push the rest
behind progressive disclosure.

Usage:
    python evals/analyze_agent_info_tokens.py            # level-2 sections
    python evals/analyze_agent_info_tokens.py --depth 3  # also break out ### subsections
    python evals/analyze_agent_info_tokens.py --json     # machine-readable
Reusable on any markdown blob: --file <path> parses that file's headers instead.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


def token_counter():
    """Return (fn, label). Prefer a real tokenizer; fall back to the chars/4 proxy
    the rest of the eval tooling uses (analyze_run.py)."""
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")
        return (lambda s: len(enc.encode(s))), "tiktoken/cl100k_base"
    except Exception:
        return (
            lambda s: round(len(s) / 4)
        ), "chars/4 (approx; install tiktoken for exact)"


def split_markdown(md: str, depth: int) -> list[tuple[str, str]]:
    """Split into (heading, body) blocks at ATX headers of level 2..depth. Text
    before the first such header is the 'preamble' block. A header line is
    `^#{2,depth} ` - single `#` is skipped so `#`-comments inside code examples
    are not mistaken for headings."""
    pat = re.compile(r"^(#{2,%d})\s+(.*)$" % max(2, depth), re.M)
    blocks: list[tuple[str, str]] = []
    marks = list(pat.finditer(md))
    if not marks or marks[0].start() > 0:
        pre = md[: marks[0].start()] if marks else md
        if pre.strip():
            blocks.append(("(preamble: title + intro)", pre))
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(md)
        level = len(m.group(1))
        label = ("  " * (level - 2)) + m.group(2).strip()
        blocks.append((f"{'#' * level} {label}", md[m.start() : end]))
    return blocks


def collect_blocks(depth: int, source_file: Path | None) -> list[tuple[str, str]]:
    if source_file is not None:
        return split_markdown(source_file.read_text(encoding="utf-8"), depth)
    from trilogy.ai import constants as C

    blocks = split_markdown(C.RULE_PROMPT, depth)
    # Non-RULE_PROMPT constants that also ride along in the agent-info payload.
    for name in ("FUNCTIONS", "AGGREGATE_FUNCTIONS"):
        val = getattr(C, name, None)
        if isinstance(val, str) and val.strip():
            blocks.append((f"[const] {name}", val))
    fe = getattr(C, "FUNCTION_EXAMPLES", None)
    if isinstance(fe, dict) and fe:
        rendered = "\n".join(f"{k}: {v}" for k, v in fe.items())
        blocks.append(("[const] FUNCTION_EXAMPLES", rendered))
    return blocks


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--depth",
        type=int,
        default=2,
        help="deepest header level to break out (2=##, 3=###)",
    )
    ap.add_argument(
        "--file",
        type=Path,
        default=None,
        help="analyze an arbitrary markdown file instead of constants.py",
    )
    ap.add_argument("--json", action="store_true", help="emit JSON")
    args = ap.parse_args()

    count, label = token_counter()
    blocks = collect_blocks(args.depth, args.file)
    rows = [
        {
            "block": name,
            "tokens": count(body),
            "chars": len(body),
            "lines": body.count("\n") + 1,
        }
        for name, body in blocks
    ]
    total = sum(r["tokens"] for r in rows) or 1
    rows.sort(key=lambda r: -r["tokens"])
    cum = 0
    for r in rows:
        r["pct"] = round(100 * r["tokens"] / total, 1)
        cum += r["tokens"]
        r["cum_pct"] = round(100 * cum / total, 1)

    if args.json:
        print(
            json.dumps(
                {"tokenizer": label, "total_tokens": total, "blocks": rows}, indent=2
            )
        )
        return 0

    print(
        f"agent-info token report  |  tokenizer: {label}  |  total ~{total:,} tokens\n"
    )
    print(f"{'tokens':>7} {'pct':>5} {'cum%':>5} {'lines':>5}  block")
    print("-" * 78)
    for r in rows:
        print(
            f"{r['tokens']:>7,} {r['pct']:>4}% {r['cum_pct']:>4}% {r['lines']:>5}  {r['block']}"
        )
    print(
        "\nReading: blocks at the top dominate the per-turn cost. Keep the few "
        "high-value ones\nas defaults; the long tail (small cum-% gain each) are "
        "progressive-disclosure candidates."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
