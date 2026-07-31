from pathlib import Path

import pytest

from trilogy.execution.state.snapshot import (
    StateSnapshot,
    StateSnapshotSummary,
)
from trilogy.scripts.serve_helpers.state_cache import (
    StateSnapshotCache,
    fingerprint_directory,
)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "model.preql").write_text("key i int;", encoding="utf-8")
    return tmp_path


def _snapshot(target: str = ".", stale: int = 0) -> StateSnapshot:
    return StateSnapshot(
        snapshot_ts="2026-07-31T00:00:00+00:00",
        target=target,
        dialect="duck_db",
        summary=StateSnapshotSummary(total=1, managed=1, stale=stale),
    )


def test_round_trip(project):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    cache.put(".", _snapshot(stale=3), fp, "2026-07-31T00:00:00+00:00")

    hit = cache.get(".", fp)
    assert hit is not None
    assert hit.computed_at == "2026-07-31T00:00:00+00:00"
    assert hit.snapshot.summary.stale == 3


def test_miss_when_absent(project):
    assert StateSnapshotCache(project).get(".", fingerprint_directory(project)) is None


def test_targets_do_not_collide(project):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    cache.put(".", _snapshot(".", stale=1), fp, "t")
    cache.put("model.preql", _snapshot("model.preql", stale=2), fp, "t")

    assert cache.get(".", fp).snapshot.summary.stale == 1
    assert cache.get("model.preql", fp).snapshot.summary.stale == 2


def test_fingerprint_mismatch_is_a_miss(project):
    cache = StateSnapshotCache(project)
    cache.put(".", _snapshot(), fingerprint_directory(project), "t")
    assert cache.get(".", "some-other-fingerprint") is None


def test_fingerprint_tracks_edits_and_additions(project):
    before = fingerprint_directory(project)

    (project / "model.preql").write_text("key i int; key j int;", encoding="utf-8")
    after_edit = fingerprint_directory(project)
    assert after_edit != before

    (project / "second.preql").write_text("key k int;", encoding="utf-8")
    assert fingerprint_directory(project) != after_edit


def test_fingerprint_ignores_non_model_files(project):
    before = fingerprint_directory(project)
    (project / "notes.txt").write_text("not a model", encoding="utf-8")
    assert fingerprint_directory(project) == before


def test_clear_drops_every_entry(project):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    cache.put(".", _snapshot(), fp, "t")
    cache.put("model.preql", _snapshot("model.preql"), fp, "t")

    cache.clear()

    assert cache.get(".", fp) is None
    assert cache.get("model.preql", fp) is None


def test_clear_without_a_cache_directory_is_a_noop(project):
    StateSnapshotCache(project).clear()


def test_corrupt_entry_reads_as_a_miss(project):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    cache.put(".", _snapshot(), fp, "t")

    snapshot_path, _ = cache._paths(".")
    snapshot_path.write_text("{not json", encoding="utf-8")

    assert cache.get(".", fp) is None


def test_state_input_path_only_for_a_live_entry(project):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    assert cache.state_input_path(".", fp) is None

    cache.put(".", _snapshot(), fp, "t")
    seed = cache.state_input_path(".", fp)
    assert seed is not None and seed.exists()

    # A job must never seed from a snapshot the server would not itself serve.
    assert cache.state_input_path(".", "stale-fingerprint") is None


def test_adopt_takes_a_job_snapshot_and_renormalizes_target(project, tmp_path):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    written = tmp_path / "job.state.json"
    # A subprocess records the absolute path it was invoked with.
    written.write_text(
        _snapshot(target=str(project / "model.preql"), stale=0).model_dump_json(),
        encoding="utf-8",
    )

    assert cache.adopt("model.preql", written, fp) is True

    hit = cache.get("model.preql", fp)
    assert hit is not None
    assert hit.snapshot.target == "model.preql"
    assert hit.computed_at == "2026-07-31T00:00:00+00:00"


def test_adopt_of_an_unreadable_file_reports_failure(project, tmp_path):
    cache = StateSnapshotCache(project)
    fp = fingerprint_directory(project)
    assert cache.adopt(".", tmp_path / "missing.json", fp) is False
    assert cache.get(".", fp) is None
