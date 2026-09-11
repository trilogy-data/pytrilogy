"""Regenerate ``query_prompts.json`` and ``references/`` from the benchmark TTL.

The questions and gold SQL come from dbt's ``benchmark_questions.ttl``
(https://github.com/dbt-labs/dbt-llm-sl-bench, vendored under ``data/``), which
is itself the data.world ACME Insurance "chat with the data" benchmark. Ids 1-11
are the eleven questions the 2026 dbt blog post scored, in the order of that
harness's ``selected_challenges`` default; the rest of the catalog follows in
TTL order so ``--num-queries 43`` runs the whole ACME set.

Gold SQL is Snowflake-flavoured; the only rewrite DuckDB needs is
``DATEDIFF("day", ...)`` -> ``datediff('day', ...)``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
TTL = EVAL_DIR / "data" / "benchmark_questions.ttl"

BLOG_QUESTIONS = [
    "What is the total amount of premiums that a policy holder has paid by policy number?",
    "What is the average time to settle a claim by policy number?",
    "What is the total amount of premiums that a policy holder has paid?",
    "How many policies have agents sold by agent id?",
    "What is the total loss amounts, which is the sum of loss payment, loss reserve amount by claim number?",
    "How many policies does each policy holder have by policy holder id?",
    "What is the total amount of premiums paid by policy number?",
    "How many claims have been placed by policy number?",
    "What is the average policy size which is the the total amount of premium divided by the number of policies?",
    "How many policies do we have?",
    "How many claims do we have?",
]

_BLOCK = re.compile(r"\n\s*\n")
_EXPECTS = re.compile(r"QandA:expects\s+(.*?)\s;", re.DOTALL)
_PROMPT = re.compile(r'QandA:prompt\s+"(.*?)"\s*[,.]', re.DOTALL)
_QUERY = re.compile(r'QandA:queryText\s+"(.*?)"\s*;', re.DOTALL)
_TITLE = re.compile(r'dct:title\s+"(.*?)"')


def _unescape(text: str) -> str:
    return text.encode().decode("unicode_escape").strip()


def parse_ttl(text: str) -> list[dict]:
    inquiries: dict[str, dict] = {}
    queries: dict[str, dict] = {}
    for block in _BLOCK.split(text):
        if not block.strip().startswith("dwt:"):
            continue
        subject = block.split()[0]
        if "QandA:Inquiry" in block:
            expects = _EXPECTS.search(block)
            prompt = _PROMPT.search(block)
            assert expects and prompt, subject
            inquiries[subject] = {
                "expects": [e.strip() for e in expects.group(1).split(",")],
                "prompt": _unescape(prompt.group(1)),
            }
        elif "dwt:SqlQuery" in block:
            query = _QUERY.search(block)
            title = _TITLE.search(block)
            assert query, subject
            queries[subject] = {
                "sql": _unescape(query.group(1)),
                "title": title.group(1) if title else "",
            }
    catalog = []
    for inquiry in inquiries.values():
        gold = [queries[e] for e in inquiry["expects"] if e in queries]
        assert len(gold) == 1, inquiry["prompt"]
        catalog.append({"prompt": inquiry["prompt"], **gold[0]})
    return catalog


def to_duckdb(sql: str) -> str:
    return sql.replace('DATEDIFF("day",', "datediff('day',").strip() + "\n"


def ordered(catalog: list[dict]) -> list[dict]:
    by_prompt = {entry["prompt"]: entry for entry in catalog}
    head = [by_prompt.pop(prompt) for prompt in BLOG_QUESTIONS]
    return head + list(by_prompt.values())


def main() -> None:
    entries = ordered(parse_ttl(TTL.read_text(encoding="utf-8")))
    prompts = []
    refs = EVAL_DIR / "references"
    refs.mkdir(exist_ok=True)
    for idx, entry in enumerate(entries, start=1):
        # The title prefix encodes the benchmark's own difficulty tag:
        # {H,L}Q = high/low question complexity, {H,L}S = high/low schema hops.
        complexity = entry["title"].split(":")[0] if ":" in entry["title"] else ""
        prompts.append(
            {
                "id": idx,
                "grade": "medium",
                "kind": "blog" if idx <= len(BLOG_QUESTIONS) else "extended",
                "complexity": complexity,
                "prompt": entry["prompt"],
            }
        )
        (refs / f"query{idx:02d}.sql").write_text(
            to_duckdb(entry["sql"]), encoding="utf-8", newline="\n"
        )
    (EVAL_DIR / "query_prompts.json").write_text(
        json.dumps(prompts, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {len(prompts)} prompts and references")


if __name__ == "__main__":
    main()
