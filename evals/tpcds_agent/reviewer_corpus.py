#!/usr/bin/env python
"""Reviewer-verdict corpus: extract, label, and measure.

The agent's submit reviewer (``_validate_completion`` in trilogy/scripts/agent.py)
decides DONE / NOT_DONE when the agent calls return_control_to_user. It should
fire NOT_DONE only when the agent ITSELF signalled it wasn't finished (mid-thought
farewell, self-noted uncertainty, an error it's still chasing) — NOT to second-guess
the numeric correctness of a cleanly-running query (it has no reference and
hallucinates flaws). This script builds a labeled test set from historical agent
logs so reviewer-prompt changes can be measured offline (true/false-positive rate)
instead of by expensive full agent reruns.

    # 1. mine every historical verdict + its faithful reviewer input
    python reviewer_corpus.py extract --out reviewer_corpus/all.jsonl

    # 2. (curate labels by hand into reviewer_corpus/labeled.jsonl)

    # 3. replay a reviewer prompt over the labeled set and score it
    python reviewer_corpus.py measure --labeled reviewer_corpus/labeled.jsonl
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

EVAL_DIR = Path(__file__).resolve().parent
REPO_ROOT = EVAL_DIR.parents[1]


def _records_from_log(path: Path) -> list[dict]:
    """One record per reviewer_verdict in a single agent log, each carrying the
    reviewer input as of that submit. Reconstructs the agent's messages and
    renders them with the LIVE `_render_reviewer_transcript` so the corpus and
    production reviewer can never drift (last-N agent messages, no task / tool
    output)."""
    from trilogy.ai.models import LLMMessage
    from trilogy.scripts.agent import _render_reviewer_transcript

    try:
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, json.JSONDecodeError):
        return []
    task = ""
    for r in rows:
        if r.get("type") == "session_start":
            task = r.get("command", "") or ""
            break

    messages: list[LLMMessage] = []
    last_farewell = ""
    submit_index = 0
    out: list[dict] = []
    for r in rows:
        t = r.get("type")
        if t == "llm_response":
            msg = LLMMessage(role="assistant", content=r.get("text") or "")
            msg.model_info = {"tool_calls": r.get("tool_calls") or []}
            messages.append(msg)
            for tc in r.get("tool_calls") or []:
                if tc.get("name") == "return_control_to_user":
                    last_farewell = (tc.get("arguments") or {}).get("message", "")
        elif t == "reviewer_verdict":
            out.append(
                {
                    "id": f"{path.stem}::{submit_index}",
                    "source": str(path.relative_to(REPO_ROOT)),
                    "task": task,
                    "transcript": _render_reviewer_transcript(messages),
                    "farewell": last_farewell,
                    "submit_index": submit_index,
                    "historical_verdict": "DONE" if r.get("is_done") else "NOT_DONE",
                    "historical_note": r.get("note", ""),
                }
            )
            submit_index += 1
    return out


def extract(args: argparse.Namespace) -> int:
    pattern = str(EVAL_DIR / "results" / "**" / "agent_log.q*.jsonl")
    logs = sorted(glob.glob(pattern, recursive=True))
    records: list[dict] = []
    for log in logs:
        records.extend(_records_from_log(Path(log)))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    done = sum(1 for r in records if r["historical_verdict"] == "DONE")
    print(
        f"extracted {len(records)} verdicts from {len(logs)} logs "
        f"(DONE={done}, NOT_DONE={len(records) - done}) -> {out}"
    )
    return 0


# The agent's OWN words signalling it is NOT finished — the only thing the
# narrowed reviewer should fire on. Correctness/completeness language ("missing
# clause", "wrong filter") is deliberately absent: the reviewer must not grade
# work, only detect that the agent itself flagged it wasn't done.
_NOTDONE_SIGNAL = re.compile(
    r"\b(let me\b|let'?s\b|^wait\b|\bwait[—,-]|i need to\b|i still need|still need to|"
    r"need to (revisit|fix|reconsider|add|handle|adjust|debug|investigate|check)|"
    r"not yet\b|\btodo\b|next,? i|i'?ll now\b|going to\b|i'?m (concerned|not sure|unsure)|"
    r"makes me concerned|uncertain|incomplete|simplif|stuck\b|blocked\b|"
    r"recursion error|\berror\b|unable to|couldn'?t|could not|does ?n'?t work|"
    r"not working|no results because|returns? null|all null)\b",
    re.I,
)
# Confident completion language with no continuation signal.
_DONE_SIGNAL = re.compile(
    r"\b(done\b|complete[d]?\b|finished\b|fixed\b|successfully\b|runs cleanly|"
    r"has been (written|created|validated|executed)|task (finished|complete))\b",
    re.I,
)


def _gold_bucket(farewell: str) -> tuple[str | None, str]:
    """Map an agent farewell to (gold_label, bucket) under the narrowed rule.
    Returns gold=None for ambiguous farewells (excluded from the test set)."""
    f = (farewell or "").strip()
    if not f:
        return "NOT_DONE", "clear_notdone"  # empty farewell = cut off mid-thought
    if _NOTDONE_SIGNAL.search(f):
        return "NOT_DONE", "clear_notdone"  # agent flagged remaining work / a problem
    if _DONE_SIGNAL.search(f):
        return "DONE", "clear_done"  # agent claims completion, no continuation signal
    return None, "ambiguous"


def label(args: argparse.Namespace) -> int:
    """Auto-label the extracted corpus by the narrowed rule and write the clear
    cases as the gold test set. Ambiguous farewells are dropped (recorded in the
    summary) — a clean set beats a noisy one for measuring the reviewer."""
    recs = [
        json.loads(line)
        for line in Path(args.corpus).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    from collections import Counter

    buckets: Counter[str] = Counter()
    by_gold: dict[str, list[dict]] = {"NOT_DONE": [], "DONE": []}
    for r in recs:
        gold, bucket = _gold_bucket(r["farewell"])
        buckets[bucket] += 1
        if gold is None:
            continue
        by_gold[gold].append({**r, "gold": gold, "bucket": bucket})
    # Cap each class so the committed test set stays small (transcripts are
    # large) and balanced; DONE vastly outnumbers NOT_DONE in the raw pool.
    # Pick one per distinct source run first so the cap spreads across many
    # query ids rather than 40 near-identical q13 runs, then top up.
    cap = args.per_class
    labeled: list[dict] = []
    for items in by_gold.values():
        seen: set[str] = set()
        first_per_run = [
            r for r in items if r["source"] not in seen and not seen.add(r["source"])
        ]
        rest = [r for r in items if r not in first_per_run]
        labeled.extend((first_per_run + rest)[:cap])
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for rec in labeled:
            f.write(json.dumps(rec) + "\n")
    nd = sum(1 for r in labeled if r["gold"] == "NOT_DONE")
    print(f"buckets (full pool): {dict(buckets)}")
    print(
        f"labeled test set: {len(labeled)} (NOT_DONE={nd}, DONE={len(labeled) - nd}, "
        f"cap {cap}/class) -> {out}"
    )
    return 0


def _predict_one(provider, transcript: str) -> str:
    from trilogy.ai.conversation import Conversation
    from trilogy.ai.models import LLMRequestOptions
    from trilogy.scripts.agent import REVIEWER_SYSTEM_PROMPT

    # Mirror _validate_completion exactly: system prompt + agent-only transcript,
    # NO task. Keep this string in sync with agent.py if it changes.
    reviewer = Conversation.create(provider, model_prompt=REVIEWER_SYSTEM_PROMPT)
    reviewer.add_message(f"AGENT'S RECENT MESSAGES:\n{transcript}", role="user")
    resp = reviewer.get_response(LLMRequestOptions(require_tool=False))
    first = (
        (resp.text or "").strip().splitlines()[0].strip().upper() if resp.text else ""
    )
    return "DONE" if first.startswith("DONE") else "NOT_DONE"


def measure(args: argparse.Namespace) -> int:
    """Replay the CURRENT reviewer prompt over the labeled set; report how often
    it agrees with gold. Positive class = NOT_DONE (a kickback)."""
    from common.report import load_env

    from trilogy.ai.providers.deepseek import DeepSeekProvider

    load_env(REPO_ROOT / ".env.secrets")
    import os

    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        print("ERROR: DEEPSEEK_API_KEY not set", file=sys.stderr)
        return 2
    rows = [
        json.loads(line)
        for line in Path(args.labeled).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    # Entries whose source log was deleted carry a pre-narrowing (full) transcript
    # that no longer matches what the live reviewer sees — exclude them so the
    # measurement reflects the current agent-only input. Re-extract from current
    # runs to replace them.
    stale = [r for r in rows if r.get("transcript_stale")]
    if stale:
        print(
            f"WARNING: skipping {len(stale)}/{len(rows)} entries with stale "
            "(pre-narrowing) transcripts — source logs deleted; rebuild the "
            "labeled set from current runs for full coverage.",
            file=sys.stderr,
        )
    rows = [r for r in rows if not r.get("transcript_stale")]
    # Balance the two classes so accuracy isn't dominated by the majority label.
    nd = [r for r in rows if r["gold"] == "NOT_DONE"]
    dn = [r for r in rows if r["gold"] == "DONE"]
    k = min(len(nd), len(dn), args.per_class)
    sample = nd[:k] + dn[:k]
    provider = DeepSeekProvider(name="reviewer-eval", model=args.model, api_key=key)

    def run(r: dict) -> tuple[str, str]:
        return r["gold"], _predict_one(provider, r["transcript"])

    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for fut in as_completed(ex.submit(run, r) for r in sample):
            results.append(fut.result())

    tp = sum(1 for g, p in results if g == "NOT_DONE" and p == "NOT_DONE")
    fn = sum(1 for g, p in results if g == "NOT_DONE" and p == "DONE")
    fp = sum(1 for g, p in results if g == "DONE" and p == "NOT_DONE")
    tn = sum(1 for g, p in results if g == "DONE" and p == "DONE")
    pos = tp + fn
    neg = fp + tn
    print(f"sample: {len(results)} ({pos} NOT_DONE, {neg} DONE) | model={args.model}")
    print(f"  TP={tp} FN={fn} FP={fp} TN={tn}")
    print(f"  recall/TPR (catch real not-done) = {tp / pos:.0%}" if pos else "  no pos")
    print(f"  FPR (kick back done work)        = {fp / neg:.0%}" if neg else "  no neg")
    print(
        f"  precision (kickbacks that were real) = {tp / (tp + fp):.0%}"
        if (tp + fp)
        else "  no predicted-positives"
    )
    print(f"  accuracy = {(tp + tn) / len(results):.0%}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Reviewer-verdict corpus tool")
    sub = parser.add_subparsers(dest="cmd", required=True)
    ex = sub.add_parser("extract", help="mine historical verdicts into a corpus jsonl")
    ex.add_argument("--out", default=str(EVAL_DIR / "reviewer_corpus" / "all.jsonl"))
    ex.set_defaults(func=extract)

    lb = sub.add_parser("label", help="auto-label clear cases into a gold test set")
    lb.add_argument("--corpus", default=str(EVAL_DIR / "reviewer_corpus" / "all.jsonl"))
    lb.add_argument(
        "--out", default=str(EVAL_DIR / "reviewer_corpus" / "labeled.jsonl")
    )
    lb.add_argument(
        "--per-class", type=int, default=40, help="cap cases per gold label"
    )
    lb.set_defaults(func=label)

    me = sub.add_parser("measure", help="replay current reviewer prompt over gold set")
    me.add_argument(
        "--labeled", default=str(EVAL_DIR / "reviewer_corpus" / "labeled.jsonl")
    )
    me.add_argument("--model", default="deepseek-chat")
    me.add_argument("--per-class", type=int, default=30)
    me.add_argument("--concurrency", type=int, default=6)
    me.set_defaults(func=measure)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
