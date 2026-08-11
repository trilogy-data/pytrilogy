import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common.scoring import _score_one, make_scoring_engine, score_query_timed


def test_score_one_can_use_a_separate_reference_database(tmp_path: Path):
    candidate_db = tmp_path / "candidate.duckdb"
    reference_db = tmp_path / "reference.duckdb"
    workspace = tmp_path / "workspace"
    references = tmp_path / "references"
    workspace.mkdir()
    references.mkdir()

    with duckdb.connect(str(candidate_db)) as connection:
        connection.execute("create table fact_source as select 1 as value")
    with duckdb.connect(str(reference_db)) as connection:
        connection.execute("create table source as select 1 as value")

    (workspace / "query01.sql").write_text(
        "select value from fact_source", encoding="utf-8"
    )
    (references / "query01.sql").write_text(
        "select value from source", encoding="utf-8"
    )

    candidate_engine = make_scoring_engine(candidate_db, workspace, "")
    reference_engine = make_scoring_engine(reference_db, references, "")
    result = _score_one(
        candidate_engine,
        workspace,
        1,
        "",
        custom_refs_dir=references,
        reference_engine=reference_engine,
    )

    assert result.status == "pass"


def test_timed_score_can_use_a_separate_reference_database(tmp_path: Path):
    candidate_db = tmp_path / "candidate.duckdb"
    reference_db = tmp_path / "reference.duckdb"
    workspace = tmp_path / "workspace"
    references = tmp_path / "references"
    workspace.mkdir()
    references.mkdir()

    with duckdb.connect(str(candidate_db)) as connection:
        connection.execute("create table fact_source as select 1 as value")
    with duckdb.connect(str(reference_db)) as connection:
        connection.execute("create table source as select 1 as value")

    (workspace / "query01.sql").write_text(
        "select value from fact_source", encoding="utf-8"
    )
    (references / "query01.sql").write_text(
        "select value from source", encoding="utf-8"
    )

    result = score_query_timed(
        candidate_db,
        workspace,
        1,
        "",
        30,
        custom_refs_dir=references,
        reference_db_path=reference_db,
    )

    assert result.status == "pass"
