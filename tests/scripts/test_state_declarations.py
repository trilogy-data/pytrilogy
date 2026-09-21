"""State is keyed by declaration, so every probe of a project agrees.

A datasource is declared in one file and reached through as many identifiers as
there are import paths to it. These tests hold the two snapshot producers — the
single-script probe and the directory probe — to one answer per declaration,
and hold ``refresh`` to the same plan and the same result whether it is pointed
at the directory or at a script that imports all of it (with
``--include-imports``, since a file otherwise builds only what it declares).
"""

import json
import shutil
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy import Dialects, Environment
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import StateSnapshot
from trilogy.execution.state.declaration import (
    canonical,
    declaration_key,
    group_declarations,
    rekey_watermark,
)
from trilogy.execution.state.snapshot import (
    DatasourceState,
    merge_into_snapshot,
)
from trilogy.execution.state.state_store import BaseStateStore, create_refresh_plan
from trilogy.execution.state.watermarks import DatasourceWatermark
from trilogy.scripts.trilogy import cli

SOURCE = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/src_events.parquet`;
"""

# Behind its root: the one stale declaration in the project.
EVENTS = """import source;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/target_events.parquet`
incremental by ev_ts;
"""

# Level with its root: fresh, and must stay fresh under every spelling.
MIRROR = """import source;

datasource mirror_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{data}/mirror_events.parquet`
incremental by ev_ts;
"""

# One hop: spells the declarations ``ev.*`` and ``mir.*``.
MID = """import events as ev;
import mirror as mir;
"""

# Two paths to ``events`` (``mid.ev.*`` and ``direct.*``), a bare path to
# ``mirror``, and a merge across the two — the shape that split one table into
# a stale entry and several fresh ones.
TOP = """import mid as mid;
import events as direct;
import mirror;

merge direct.ev_id into mid.ev.ev_id;
"""

FILES = {
    "source.preql": SOURCE,
    "events.preql": EVENTS,
    "mirror.preql": MIRROR,
    "mid.preql": MID,
    "top.preql": TOP,
}


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _project(root: Path) -> Path:
    data = root / "data"
    data.mkdir(parents=True)
    src = (data / "src_events.parquet").as_posix()
    con = duckdb.connect()
    con.execute(f"""COPY (
            SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts
            UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'
        ) TO '{src}' (FORMAT PARQUET)""")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src}') WHERE ev_id = 1) "
        f"TO '{(data / 'target_events.parquet').as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src}')) "
        f"TO '{(data / 'mirror_events.parquet').as_posix()}' (FORMAT PARQUET)"
    )
    con.close()
    (root / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    for name, text in FILES.items():
        (root / name).write_text(text.format(data=data.as_posix()), encoding="utf-8")
    return root


def _state(runner: CliRunner, target: Path, out: Path) -> StateSnapshot:
    result = runner.invoke(cli, ["state", str(target), "--output", str(out)])
    assert result.exit_code == 0, result.output
    return StateSnapshot.model_validate(json.loads(out.read_text(encoding="utf-8")))


def _values(watermarks) -> list[tuple[str | None, str | None]]:
    # By physical column: the concept address carries the probing namespace.
    return sorted((w.column, w.value) for w in watermarks)


def _entries(snapshot: StateSnapshot) -> dict[tuple[str, str | None, str], dict]:
    return {
        (asset.address, ds.script, ds.datasource_id): {
            "managed": asset.managed,
            "is_root": ds.is_root,
            "refresh_kind": ds.refresh_kind,
            "status": ds.status,
            "observed": _values(ds.observed_watermarks),
            "expected": _values(ds.expected_watermarks),
            "columns": sorted(c.column for c in ds.columns),
        }
        for asset in snapshot.assets
        for ds in asset.datasources
    }


def test_directory_snapshot_holds_one_entry_per_declaration(runner, tmp_path):
    project = _project(tmp_path / "p")
    entries = _entries(_state(runner, project, tmp_path / "dir.json"))

    assert set(entries) == {
        ("data/src_events.parquet", "source.preql", "src_events"),
        ("data/target_events.parquet", "events.preql", "target_events"),
        ("data/mirror_events.parquet", "mirror.preql", "mirror_events"),
    }
    assert (
        entries[("data/target_events.parquet", "events.preql", "target_events")][
            "status"
        ]
        == "stale"
    )
    assert (
        entries[("data/mirror_events.parquet", "mirror.preql", "mirror_events")][
            "status"
        ]
        == "fresh"
    )


def test_import_paths_ride_as_aliases(runner, tmp_path):
    project = _project(tmp_path / "p")
    snapshot = _state(runner, project / "top.preql", tmp_path / "top.json")
    aliases = {
        ds.datasource_id: ds.aliases
        for asset in snapshot.assets
        for ds in asset.datasources
    }
    assert aliases["target_events"] == ["direct.target_events", "mid.ev.target_events"]
    assert aliases["mirror_events"] == ["mid.mir.mirror_events", "mirror_events"]


@pytest.mark.parametrize(
    "script", ["events.preql", "mirror.preql", "mid.preql", "top.preql"]
)
def test_single_script_state_matches_the_directory(runner, tmp_path, script):
    """Every declaration a script can judge reads exactly as the directory
    probe reads it. A root is only watermarked in service of a dependent, so a
    script holding no dependent of a root reports it unobserved — every
    managed entry, and every observed root, must match."""
    project = _project(tmp_path / "p")
    directory = _entries(_state(runner, project, tmp_path / "dir.json"))
    single = _entries(_state(runner, project / script, tmp_path / "one.json"))

    judged = {
        key: entry
        for key, entry in single.items()
        if not entry["is_root"] or entry["observed"]
    }
    assert judged, "the script judged nothing"
    assert judged == {key: directory[key] for key in judged}


def _refresh(runner: CliRunner, target: Path, state_file: Path) -> StateSnapshot:
    # ``--include-imports`` because the comparison is against a directory run,
    # which owns every declaration in the project. A file otherwise builds only
    # what it declares, and ``top.preql`` declares nothing.
    result = runner.invoke(
        cli,
        [
            "refresh",
            str(target),
            "duckdb",
            "--include-imports",
            "--state-file",
            str(state_file),
        ],
    )
    assert result.exit_code == 0, result.output
    return StateSnapshot.model_validate(
        json.loads(state_file.read_text(encoding="utf-8"))
    )


def _rows(parquet: Path) -> list[tuple]:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{parquet.as_posix()}') ORDER BY 1"
        ).fetchall()
    finally:
        con.close()


def test_refresh_of_a_script_matches_refresh_of_its_directory(runner, tmp_path):
    by_directory = _project(tmp_path / "dir")
    by_script = tmp_path / "script"
    shutil.copytree(by_directory, by_script)
    for name, text in FILES.items():
        (by_script / name).write_text(
            text.format(data=(by_script / "data").as_posix()), encoding="utf-8"
        )

    from_directory = _refresh(runner, by_directory, tmp_path / "dir.json")
    from_script = _refresh(runner, by_script / "top.preql", tmp_path / "script.json")

    assert _entries(from_script) == _entries(from_directory)
    assert {e["status"] for e in _entries(from_script).values()} == {"fresh"}
    for table in ("target_events.parquet", "mirror_events.parquet"):
        assert _rows(by_script / "data" / table) == _rows(by_directory / "data" / table)


def _top_executor(project: Path):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=project)
    )
    executor.parse_text((project / "top.preql").read_text(), root=project / "top.preql")
    return executor


def test_a_declaration_is_planned_once(tmp_path):
    executor = _top_executor(_project(tmp_path / "p"))
    plan = create_refresh_plan(executor)
    assert [a.datasource_id for a in plan.stale_assets] == ["direct.target_events"]


def test_a_declaration_is_probed_once(tmp_path, monkeypatch):
    from trilogy.execution.state import state_store as module

    probed: list[str] = []
    real = module.get_incremental_key_watermarks

    def counting(datasource, executor):
        probed.append(datasource.identifier)
        return real(datasource, executor)

    monkeypatch.setattr(module, "get_incremental_key_watermarks", counting)
    executor = _top_executor(_project(tmp_path / "p"))
    store = BaseStateStore()
    watermarks = store.watermark_all_assets(executor.environment, executor)

    assert len(probed) == 2, probed
    spellings = {"direct.target_events", "mid.ev.target_events"}
    assert spellings <= set(watermarks)
    values = {next(iter(watermarks[s].keys.values())).value for s in spellings}
    assert len(values) == 1


def test_invalidating_one_spelling_invalidates_the_declaration(tmp_path):
    executor = _top_executor(_project(tmp_path / "p"))
    store = BaseStateStore()
    store.watermark_all_assets(executor.environment, executor)

    store.invalidate("direct.target_events")

    assert "mid.ev.target_events" not in store.watermarks
    assert "mirror_events" in store.watermarks


def test_declared_in_survives_every_import_path(tmp_path):
    project = _project(tmp_path / "p")
    env = _top_executor(project).environment
    declared = {
        ds.identifier: Path(ds.declared_in).name if ds.declared_in else None
        for ds in env.datasources.values()
    }
    assert declared == {
        "direct.src_events": "source.preql",
        "direct.target_events": "events.preql",
        "mid.ev.src_events": "source.preql",
        "mid.ev.target_events": "events.preql",
        "mid.mir.src_events": "source.preql",
        "mid.mir.mirror_events": "mirror.preql",
        "src_events": "source.preql",
        "mirror_events": "mirror.preql",
    }
    groups = group_declarations(env.datasources.values())
    assert len(groups) == 3
    target = env.datasources["mid.ev.target_events"]
    assert canonical(groups[declaration_key(target)]).identifier == (
        "direct.target_events"
    )


def test_rekey_translates_through_the_physical_column(tmp_path):
    env = _top_executor(_project(tmp_path / "p")).environment
    source = env.datasources["direct.target_events"]
    target = env.datasources["mid.ev.target_events"]
    key = next(c.concept.address for c in source.columns if c.alias == "ev_ts")
    watermark = DatasourceWatermark(
        keys={key: UpdateKey(key, UpdateKeyType.INCREMENTAL_KEY, "2024-01-10")}
    )

    shared = rekey_watermark(watermark, source, target)

    expected = next(c.concept.address for c in target.columns if c.alias == "ev_ts")
    assert shared is not None
    assert list(shared.keys) == [expected]
    assert shared.keys[expected].concept_name == expected


def test_rekey_declines_a_key_with_no_column(tmp_path):
    env = _top_executor(_project(tmp_path / "p")).environment
    watermark = DatasourceWatermark(
        keys={
            "direct.ev_ts.date": UpdateKey(
                "direct.ev_ts.date", UpdateKeyType.INCREMENTAL_KEY, "2024-01-10"
            )
        }
    )
    assert (
        rekey_watermark(
            watermark,
            env.datasources["direct.target_events"],
            env.datasources["mid.ev.target_events"],
        )
        is None
    )


def _entry(name: str, script: str, aliases: list[str], status: str = "fresh"):
    return DatasourceState(
        datasource_id=name, script=script, aliases=aliases, status=status
    )


def test_two_declarations_of_one_address_stay_separate():
    """A writer and a reader can model one file under the same name from two
    files. They are two declarations, and folding them drops one's verdict."""
    snapshot = merge_into_snapshot(
        [
            ("data/x.parquet", _entry("x", "writer.preql", ["x"], "stale")),
            ("data/x.parquet", _entry("x", "reader.preql", ["read.x"])),
        ]
    )

    assert [(d.script, d.status) for d in snapshot.assets[0].datasources] == [
        ("reader.preql", "fresh"),
        ("writer.preql", "stale"),
    ]


def test_one_declaration_probed_from_two_scripts_is_one_entry():
    snapshot = merge_into_snapshot(
        [
            ("data/x.parquet", _entry("x", "model.preql", ["x"])),
            ("data/x.parquet", _entry("x", "model.preql", ["mid.x"])),
        ]
    )

    assert [d.aliases for d in snapshot.assets[0].datasources] == [["mid.x", "x"]]


def test_a_pre_declaration_snapshot_is_rejected(tmp_path):
    """Schema 1 keyed entries by import path and filed the *probing* script
    under ``script``, so its records pair with nothing here — and the declaring
    file they would need was never written down. Re-probing recovers it."""
    from trilogy.execution.state import StateSchemaError, read_state_snapshot

    path = tmp_path / "old.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_ts": "2024-01-01T00:00:00Z",
                "assets": [
                    {
                        "address": "data/x.parquet",
                        "datasources": [
                            {"datasource_id": "mid.x", "script": "mid.preql"}
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(StateSchemaError, match="trilogy state <target>"):
        read_state_snapshot(path)


def test_a_parse_rooted_at_a_directory_has_no_declaring_file(tmp_path):
    """The declaring file is the file being parsed. A parse rooted at a
    directory has none — including a directory whose name carries a suffix,
    which would otherwise become the origin of every declaration under it and
    fold two files' same-named declarations into one."""
    from trilogy.parser import parse_text

    root = tmp_path / "models.v2"
    data = tmp_path / "data"
    data.mkdir(parents=True)
    root.mkdir()

    env, _ = parse_text(
        SOURCE.format(data=data.as_posix()),
        Environment(working_path=root),
        root=root,
    )

    assert [ds.declared_in for ds in env.datasources.values()] == [None]
