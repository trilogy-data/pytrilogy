"""End-to-end audit of CLI internal consistency on a generated TPC-DS dataset.

The premise: `trilogy ingest` introspects warehouse tables/files and emits a
"perfect" .preql describing them. If that's truly perfect, then:

  1. `trilogy integration` on the freshly-ingested directory should validate
     cleanly (zero failures) — every column the .preql declares must really
     exist in the underlying parquet/table.
  2. `trilogy fmt` on those files should be a no-op — ingest already runs the
     same Renderer that fmt uses, so the on-disk bytes should already be the
     canonical formatting.
  3. Re-running `trilogy ingest` against the same parquets should produce
     byte-identical output — ingest itself should be deterministic.

Any of these failing means the CLI has an internal-consistency gap.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

REPO_ROOT = Path(__file__).resolve().parents[2]
TPCDS_CONFTEST_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"


def _ensure_tpcds_dataset() -> Path:
    """Materialise the sf=0.01 TPC-DS parquet set, reusing the conftest helper."""
    sys.path.insert(0, str(TPCDS_CONFTEST_DIR.parent.parent.parent))
    try:
        from tests.modeling.tpc_ds_duckdb.conftest import _ensure_dataset
    finally:
        sys.path.pop(0)

    import_path = TPCDS_CONFTEST_DIR / "memory_sf001"
    _ensure_dataset(import_path, sf=0.01)
    return import_path


def _parquet_sources(import_path: Path) -> list[Path]:
    return sorted(p for p in import_path.glob("*.parquet"))


def _run(runner: CliRunner, args: list[str]) -> None:
    result = runner.invoke(cli, args, catch_exceptions=False)
    if result.exit_code != 0:
        raise AssertionError(
            f"`trilogy {' '.join(args[:1])}` exited {result.exit_code}\n{result.output}"
        )


def _snapshot(dir_path: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(dir_path.glob("*.preql"))}


@pytest.fixture(scope="module")
def ingested_dir(tmp_path_factory) -> Path:
    """Ingest every TPC-DS parquet once per module — the dsdgen step is slow."""
    parquets = _parquet_sources(_ensure_tpcds_dataset())
    assert parquets, "expected sf=0.01 parquet set to be present after _ensure_dataset"

    out_dir = tmp_path_factory.mktemp("ingested")
    runner = CliRunner()
    _run(
        runner,
        [
            "ingest",
            ",".join(str(p) for p in parquets),
            "duckdb",
            "--output",
            str(out_dir),
            "--infer-level",
            "full",
        ],
    )

    produced = sorted(p.name for p in out_dir.glob("*.preql"))
    assert len(produced) == len(
        parquets
    ), f"ingest produced {len(produced)} files from {len(parquets)} parquets: {produced}"
    return out_dir


def test_ingest_output_passes_integration(ingested_dir: Path) -> None:
    """The .preql files ingest just wrote should validate against the same parquets."""
    runner = CliRunner()
    _run(runner, ["integration", str(ingested_dir), "duckdb"])


def test_ingest_output_is_format_stable(ingested_dir: Path) -> None:
    """`fmt` over freshly-ingested files must change nothing — same Renderer."""
    before = _snapshot(ingested_dir)
    runner = CliRunner()
    _run(runner, ["fmt", str(ingested_dir)])
    after = _snapshot(ingested_dir)

    changed = [name for name in before if before[name] != after[name]]
    assert not changed, (
        "fmt mutated files emitted by ingest — ingest's Renderer output is not canonical: "
        f"{changed}"
    )


def test_ingest_is_deterministic(ingested_dir: Path, tmp_path: Path) -> None:
    """A second ingest pass over the same parquets must produce identical bytes."""
    parquets = _parquet_sources(_ensure_tpcds_dataset())
    out_dir = tmp_path / "ingested_second"
    out_dir.mkdir()
    runner = CliRunner()
    _run(
        runner,
        [
            "ingest",
            ",".join(str(p) for p in parquets),
            "duckdb",
            "--output",
            str(out_dir),
            "--infer-level",
            "full",
        ],
    )

    first = _snapshot(ingested_dir)
    second = _snapshot(out_dir)
    assert set(first) == set(second), "different files produced across runs"
    diverged = [name for name in first if first[name] != second[name]]
    assert not diverged, f"ingest is non-deterministic on: {diverged}"
