import shutil
from pathlib import Path

import pytest

from local_scripts.oracle_sweep import runner

OK = {"sql": "a", "summary": 1, "scans": 1, "joins": 2, "ctes": 3, "status": "ok"}


def test_parse_seeds():
    assert runner.parse_seeds("5,1:3,2") == [1, 2, 5]


def test_parse_seeds_file(tmp_path: Path):
    path = tmp_path / "seeds.txt"
    path.write_text("9 3\n3\n")
    assert runner.parse_seeds(f"@{path}") == [3, 9]


def test_compare_names_what_moved():
    base = {
        (1, "base"): OK,
        (2, "base"): OK,
        (3, "base"): {"status": "raise", "error": "x"},
        (4, "base"): {"status": "raise", "error": "x"},
    }
    new = {
        (1, "base"): {**OK, "sql": "b", "summary": 0, "scans": 2, "joins": 1},
        (2, "base"): {**OK, "status": "wrong"},
        (3, "base"): OK,
        (4, "base"): {"status": "raise", "error": "x"},
    }
    moved = runner.compare(base, new)
    assert moved["broken"] == [(2, "base")]
    assert moved["fixed"] == [(3, "base")]
    assert moved["still_bad"] == [(4, "base")]
    assert moved["sql_changed"] == [(1, "base")]
    assert moved["summary_lost"] == moved["summary_lost_more_scans"] == [(1, "base")]
    assert moved["fewer_joins"] == [(1, "base")]
    assert not runner.report(base, new)


def test_records_round_trip(tmp_path: Path):
    records = {(2, "base"): {"seed": 2, "model": "base", **OK}}
    runner.write_records(tmp_path / "r.jsonl", records.values())
    assert runner.read_records(tmp_path / "r.jsonl") == records


def test_sweep_matches_the_oracle_whatever_the_jobs():
    seeds = [*range(24), 38477, 69075]
    one = runner.sweep("spellings", seeds, jobs=1, quiet=True)
    two = runner.sweep("spellings", seeds, jobs=2, quiet=True)
    assert one == two
    assert {r["status"] for r in one.values()} <= {"ok", "rejected"}
    assert one[(38477, "summary")]["summary"] and not one[(38477, "base")]["summary"]


@pytest.mark.skipif(
    not shutil.which("git") or not (runner.REPO / ".git").exists(),
    reason="needs the git checkout",
)
def test_a_ref_is_swept_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(runner, "CACHE", tmp_path)
    seeds = list(range(12))
    base = runner.sweep("first", seeds, ref="HEAD", jobs=1, quiet=True)
    new = runner.sweep("first", seeds, jobs=1, quiet=True, proven=base)
    assert runner.compare(base, new)["broken"] == []
    monkeypatch.setattr(runner, "_sweep", None)
    assert runner.sweep("first", seeds, ref="HEAD", jobs=1, quiet=True) == base
