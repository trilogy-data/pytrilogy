"""Guard rails for the TPC-DS agent eval prompts.

The eval intentionally hands the agent only the business question — the agent
must discover the schema by exploration (this is what gives the eval fidelity
to how a real user would use the tool). Each prompt is therefore checked for
TPC-DS column-name leaks before the eval runs.

Allowed: business words ("brand", "manufacturer ID 128", "11th month of 2000").
Forbidden: literal TPC-DS column identifiers (``cs_sales_price``,
``p_channel_email``, ``sr_cdemo_sk``) or backticked SQL snippets that name
them, since those hand the agent the exact column to use.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

PROMPTS_PATH = (
    Path(__file__).resolve().parents[2] / "evals" / "tpcds_agent" / "query_prompts.json"
)

# Two-/three-letter prefixes that uniquely identify a TPC-DS physical column.
# Source: TPC-DS spec table-column naming convention.
TPCDS_COLUMN_PREFIXES: tuple[str, ...] = (
    "ss",
    "sr",  # store_sales / store_returns
    "cs",
    "cr",  # catalog_sales / catalog_returns
    "ws",
    "wr",  # web_sales / web_returns
    "inv",  # inventory
    "c",  # customer
    "ca",  # customer_address
    "cd",  # customer_demographics
    "hd",  # household_demographics
    "d",  # date_dim
    "t",  # time_dim
    "i",  # item
    "s",  # store
    "w",  # warehouse
    "p",  # promotion
    "cc",  # call_center
    "cp",  # catalog_page
    "wp",  # web_page
    "web",  # web_site
    "ib",  # income_band
    "r",  # reason
    "sm",  # ship_mode
    "dv",  # dbgen_version
)

# A TPC-DS column ID looks like `<prefix>_<rest>` where `rest` is one or more
# lowercase-letter-or-underscore tokens, e.g. `cs_sales_price`, `p_channel_email`,
# `c_current_cdemo_sk`. Anchored at a word boundary so we don't match midwords
# like ``inverse``.
_COLUMN_PATTERN = re.compile(
    r"\b(?:" + "|".join(TPCDS_COLUMN_PREFIXES) + r")_[a-z][a-z_]*\b"
)

# A literal SQL-ish snippet in backticks is treated as a leak even if it
# happens not to match the column pattern — the prompt should never name
# columns directly, period.
_BACKTICK_SNIPPET = re.compile(r"`([^`]+)`")

# Phrasings that refer to TPC-DS internal counters by their integer value
# rather than the business date they encode. The canonical case is
# ``d_month_seq`` (Jan 2000 = 1200, Dec 2000 = 1211) — prompts should say
# "in the year 2000" instead. Catches "month sequence 1200", "month sequences
# 1200 through 1211", "month seq between 1200 and 1211", etc.
_MONTH_SEQ_PATTERN = re.compile(
    r"month[_ ]seq(?:uence)?s?\b[^.]*?\b\d{3,4}\b",
    re.IGNORECASE,
)

# Tokens that legitimately start with one of the short prefixes but are common
# English words / phrases / TPC-DS *business* terms — not column identifiers.
_ALLOWED_TOKENS: frozenset[str] = frozenset(
    {
        # Channel-rollup queries (TPC-DS Q5, Q80) hardcode these as SQL string
        # literals that prefix per-outlet IDs — the agent must emit them verbatim
        # to match the reference, so the prompt has to name them.
        "web_site",
    }
)


def _scan(prompt: str) -> list[str]:
    """Return every offending substring found in the prompt. Empty == clean."""
    leaks: list[str] = []
    for match in _COLUMN_PATTERN.finditer(prompt):
        token = match.group(0)
        if token in _ALLOWED_TOKENS:
            continue
        leaks.append(token)
    for match in _BACKTICK_SNIPPET.finditer(prompt):
        snippet = match.group(1)
        # A backticked snippet that is a single business word like `Sports`
        # is fine; a snippet with `=`, `_`, or a SQL operator is not.
        if re.search(r"[_=<>]|\s(and|or|in|not|is)\s", snippet, re.IGNORECASE):
            leaks.append(f"`{snippet}`")
    for match in _MONTH_SEQ_PATTERN.finditer(prompt):
        leaks.append(match.group(0))
    return leaks


def _load_prompts() -> list[dict]:
    return json.loads(PROMPTS_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "entry",
    _load_prompts(),
    ids=lambda e: f"q{e['id']:02d}",
)
def test_prompt_does_not_leak_schema(entry: dict) -> None:
    leaks = _scan(entry["prompt"])
    assert not leaks, (
        f"query {entry['id']} prompt leaks schema identifiers: {leaks}\n"
        "Prompts must describe the business question only — the agent is "
        "expected to discover column names by exploring the model."
    )


def test_prompt_pattern_catches_known_leaks() -> None:
    """Sanity-check the detector against a synthetic leaky prompt."""
    leaky = (
        "Filter by `p_channel_email = 'N'` and use the cs_sales_price field "
        "where sr_cdemo_sk equals c_current_cdemo_sk."
    )
    leaks = _scan(leaky)
    assert "cs_sales_price" in leaks
    assert "sr_cdemo_sk" in leaks
    assert "c_current_cdemo_sk" in leaks
    assert any("p_channel_email" in leak for leak in leaks)


def test_prompt_pattern_catches_month_seq_leaks() -> None:
    """Detector flags d_month_seq integer ranges (must be translated to dates)."""
    leaky = (
        "For web sales in the 12-month window of month sequences 1200 through "
        "1211, report shipping lag buckets."
    )
    leaks = _scan(leaky)
    assert leaks, "expected month-sequence range to be flagged as a leak"


def test_prompt_pattern_allows_business_text() -> None:
    """Sanity-check that ordinary business prose does not trip the detector."""
    clean = (
        "For store sales in the 11th month of year 2000, report total extended "
        "sales price by brand, restricted to items managed by manager ID 1. "
        "Order by year, then total descending, then brand id; limit 100 rows."
    )
    assert _scan(clean) == []
