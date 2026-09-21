"""Refreshing a file builds what that file declares.

A datasource reached only by import belongs to a run of the file that declares
it — which is also how a directory run assigns ownership, one script per
address. It is still probed: its watermark is the expected side of the assets
that *do* get built. ``--include-imports`` asks for the transitive behaviour.
"""

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

SOURCE = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/src.parquet`;

datasource upstream (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/upstream.parquet`
incremental by ev_ts;
"""

TOP = """import source;

datasource downstream (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/downstream.parquet`
incremental by ev_ts;
"""


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _rows(path) -> list[tuple]:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path.as_posix()}') ORDER BY 1"
        ).fetchall()
    finally:
        con.close()


@pytest.fixture
def project(tmp_path):
    """``upstream`` and ``downstream`` are both one row behind their root, and
    are declared in different files."""
    data = tmp_path / "data"
    data.mkdir()
    con = duckdb.connect()
    con.execute(f"""COPY (
            SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts
            UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'
        ) TO '{(data / "src.parquet").as_posix()}' (FORMAT PARQUET)""")
    for table in ("upstream.parquet", "downstream.parquet"):
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{(data / 'src.parquet').as_posix()}')"
            f" WHERE ev_id = 1) TO '{(data / table).as_posix()}' (FORMAT PARQUET)"
        )
    con.close()
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    (tmp_path / "source.preql").write_text(
        SOURCE.format(data=data.as_posix()), encoding="utf-8"
    )
    (tmp_path / "top.preql").write_text(
        TOP.format(data=data.as_posix()), encoding="utf-8"
    )
    return tmp_path, data


def _refresh(runner: CliRunner, *args: str):
    return runner.invoke(cli, ["refresh", *args, "duckdb"])


def test_a_file_refresh_builds_only_what_the_file_declares(runner, project):
    root, data = project

    result = _refresh(runner, str(root / "top.preql"))

    assert result.exit_code == 0, result.output
    assert [r[0] for r in _rows(data / "downstream.parquet")] == [1, 2]
    assert [r[0] for r in _rows(data / "upstream.parquet")] == [1]


def test_an_unbuilt_stale_import_is_reported(runner, project):
    root, _ = project

    result = _refresh(runner, str(root / "top.preql"))

    assert "stale imported asset(s) not built" in result.output
    assert "upstream" in result.output
    assert "--include-imports" in result.output


def test_include_imports_restores_the_transitive_build(runner, project):
    root, data = project

    result = _refresh(runner, str(root / "top.preql"), "--include-imports")

    assert result.exit_code == 0, result.output
    assert [r[0] for r in _rows(data / "upstream.parquet")] == [1, 2]
    assert [r[0] for r in _rows(data / "downstream.parquet")] == [1, 2]


def test_force_reaches_an_imported_asset_without_the_flag(runner, project):
    """Naming a datasource *is* scoping it; --force is an explicit request."""
    root, data = project

    result = _refresh(runner, str(root / "top.preql"), "--force", "upstream")

    assert result.exit_code == 0, result.output
    assert [r[0] for r in _rows(data / "upstream.parquet")] == [1, 2]


def test_a_directory_refresh_still_builds_everything(runner, project):
    """A directory run owns the whole graph and assigns one script per address,
    so the file scope must not narrow it."""
    root, data = project

    result = _refresh(runner, str(root))

    assert result.exit_code == 0, result.output
    assert [r[0] for r in _rows(data / "upstream.parquet")] == [1, 2]
    assert [r[0] for r in _rows(data / "downstream.parquet")] == [1, 2]


PARTITIONED_SOURCE = """key ev_id int;
property ev_id.ev_ts datetime;
property ev_id.ev_day date;

root datasource src (
    ev_id: ev_id,
    ev_ts: ev_ts,
    ev_day: ev_day
)
grain (ev_id)
query '''
select 1 as ev_id, timestamp '2024-01-10 12:00:00' as ev_ts, date '2024-01-10' as ev_day
''';

auto ev_count <- count(ev_id) by ev_day;
auto ev_high <- max(ev_ts) by ev_day;

datasource upstream_daily (
    ev_day: ev_day,
    ev_count: ev_count,
    ev_high: ev_high
)
grain (ev_day)
address upstream_daily
freshness by ev_high
partition by ev_day;

CREATE IF NOT EXISTS DATASOURCE upstream_daily;
"""

PARTITIONED_TOP = """import partitioned_source as ps;
"""


def test_partition_does_not_reach_past_the_scope(runner, tmp_path):
    """``--partition`` names a slice by concept, not a datasource, so it means
    "that slice, of what this run builds" — and says so rather than silently
    narrowing nothing. Only --force names past the scope."""
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    (tmp_path / "partitioned_source.preql").write_text(
        PARTITIONED_SOURCE, encoding="utf-8"
    )
    (tmp_path / "top.preql").write_text(PARTITIONED_TOP, encoding="utf-8")
    target = str(tmp_path / "top.preql")
    slice_ = "ps.ev_day=2024-01-10"

    scoped = _refresh(runner, target, "--partition", slice_)
    assert scoped.exit_code == 1, scoped.output
    assert "--include-imports" in scoped.output

    opened = _refresh(runner, target, "--include-imports", "--partition", slice_)
    assert opened.exit_code == 0, opened.output
    assert "Refreshing ps.upstream_daily: partition ev_day=2024-01-10 requested" in (
        opened.output.replace("\n", "")
    )


def test_an_imported_table_datasource_can_be_refreshed(runner, tmp_path):
    """The refresh's CREATE looked its target up by bare name, and
    ``environment.datasources`` is keyed by identifier — so a table-backed
    datasource reached through an import alias could never be built."""
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    (tmp_path / "partitioned_source.preql").write_text(
        PARTITIONED_SOURCE, encoding="utf-8"
    )
    (tmp_path / "top.preql").write_text(PARTITIONED_TOP, encoding="utf-8")

    result = _refresh(
        runner,
        str(tmp_path / "top.preql"),
        "--include-imports",
        "--force",
        "ps.upstream_daily",
    )

    assert result.exit_code == 0, result.output
    assert "Refreshed 1 asset(s)" in result.output
