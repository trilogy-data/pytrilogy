"""The curated model answers every blog question: each canonical
``enriched_model/query<NN>.preql`` must match its gold reference. Also pins
the vendored data to the benchmark's shape.

Run: ``python -m pytest evals/acme_insurance_agent/test_model.py``."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EVAL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(EVAL_DIR.parent))
sys.path.insert(0, str(EVAL_DIR))

import db_build
from common import scoring
from common.prompts import active_prompts
from spec import SPEC

MODEL_DIR = EVAL_DIR / "enriched_model"
BLOG_IDS = [p["id"] for p in active_prompts(SPEC) if p["kind"] == "blog"]


@pytest.fixture(scope="module")
def engine():
    db = db_build.build_database()
    engine = scoring.make_scoring_engine(db, MODEL_DIR, SPEC.duckdb_extension)
    yield engine
    engine.close()


def test_data_shape(engine) -> None:
    counts = {
        table: engine.execute_raw_sql(f'select count(*) from "{table}"').fetchone()[0]
        for table in db_build.TABLES
    }
    assert counts["Policy"] == 2
    assert counts["Claim"] == 2
    assert counts["Premium"] == 6
    assert counts["Claim_Amount"] == 8


@pytest.mark.parametrize("idx", BLOG_IDS)
def test_canonical_matches_gold(engine, idx: int) -> None:
    result = scoring.score_query(
        engine,
        MODEL_DIR,
        idx,
        SPEC.duckdb_extension,
        custom_refs_dir=SPEC.references_dir,
    )
    assert result.status == "pass", f"q{idx:02d}: {result.status} {result.detail}"
