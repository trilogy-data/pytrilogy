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

# The blog's wording leaves the output shape to the reader (q03 never says "by
# policy holder"; q08 and q02 never say whether claim-less policies appear).
# Each rewrite states the grain, the columns in order, and which rows count,
# exactly as the gold SQL computes them; the verbatim text is kept beside it
# as ``prompt_original``.
SPECIFIED = {
    BLOG_QUESTIONS[0]: (
        "What is the total premium paid on each policy? Return one row per "
        "policy with two columns, in this order: the policy number, and the "
        "total premium amount paid on that policy (the sum of its premium "
        "amounts)."
    ),
    BLOG_QUESTIONS[1]: (
        "What is the average time to settle a claim, per policy? Return one row "
        "per policy that has at least one settled claim, with two columns, in "
        "this order: the policy number, and the average number of days from a "
        "claim's open date to its close date over that policy's settled "
        "(closed) claims. Policies with no settled claims are not included."
    ),
    BLOG_QUESTIONS[2]: (
        "How much premium has each policy holder paid in total? Return one row "
        "per policy holder with two columns, in this order: the policy holder's "
        "party id, and the total premium amount across all of their policies."
    ),
    BLOG_QUESTIONS[3]: (
        "How many policies has each agent sold? Return one row per agent with "
        "two columns, in this order: the agent's party id, and the number of "
        "policies that agent sold."
    ),
    BLOG_QUESTIONS[4]: (
        "What is the loss amount of each claim, where loss amount is the claim's "
        "loss payment amount plus its loss reserve amount? Return one row per "
        "claim with two columns, in this order: the company claim number, and "
        "that loss amount."
    ),
    BLOG_QUESTIONS[5]: (
        "How many policies does each policy holder have? Return one row per "
        "policy holder with two columns, in this order: the policy holder's "
        "party id, and their number of policies."
    ),
    BLOG_QUESTIONS[6]: (
        "What is the total premium paid, per policy? Return one row per policy "
        "with two columns, in this order: the policy number, and the total "
        "premium amount paid on that policy."
    ),
    BLOG_QUESTIONS[7]: (
        "How many claims have been placed against each policy? Return one row "
        "per policy that has at least one claim, with two columns, in this "
        "order: the policy number, and the number of claims placed against it. "
        "Policies with no claims are not included."
    ),
    BLOG_QUESTIONS[8]: (
        "What is the average policy size, defined as the total premium amount "
        "across all policies divided by the number of distinct policies that "
        "carry a premium? Return a single row with one column: that average."
    ),
    BLOG_QUESTIONS[9]: (
        "How many policies do we have in total? Return a single row with one "
        "column: the number of policies."
    ),
    BLOG_QUESTIONS[10]: (
        "How many claims do we have in total? Return a single row with one "
        "column: the number of claims."
    ),
}

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
        record = {
            "id": idx,
            "grade": "medium",
            "kind": "blog" if idx <= len(BLOG_QUESTIONS) else "extended",
            "complexity": complexity,
            "prompt": SPECIFIED.get(entry["prompt"], entry["prompt"]),
        }
        if entry["prompt"] in SPECIFIED:
            record["prompt_original"] = entry["prompt"]
        prompts.append(record)
        (refs / f"query{idx:02d}.sql").write_text(
            to_duckdb(entry["sql"]), encoding="utf-8", newline="\n"
        )
    (EVAL_DIR / "query_prompts.json").write_text(
        json.dumps(prompts, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {len(prompts)} prompts and references")


if __name__ == "__main__":
    main()
