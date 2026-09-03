"""`[dependencies]` in trilogy.toml: declared edges with a `when` condition.

Derived edges (imports, declare/persist) skip a dependent when its upstream
fails. A declared `when = "failed"` edge is the opposite: the dependent runs
*because* the upstream failed — a repair script — and stands down as a
successful skip when it did not, so the run stays green.
"""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.execution.test_run_outputs import (
    assert_output_listed,
    rich_mode,
    rich_modes,
)
from trilogy.core import graph as nx
from trilogy.scripts.dependency import (
    DependencyResolver,
    ScriptNode,
    edge_condition,
)
from trilogy.scripts.parallel_execution import (
    ExecutionResult,
    RunState,
    _mark_node_complete,
)
from trilogy.scripts.project_config import (
    DeclaredDependency,
    load_declared_dependencies,
)
from trilogy.scripts.trilogy import cli


@pytest.fixture(autouse=True)
def _reset_output_format():
    from trilogy.scripts import display_core

    yield
    display_core.set_output_format("rich")


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def _toml(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "trilogy.toml"
    path.write_text(body, encoding="utf-8")
    return path


def test_load_declared_dependencies(tmp_path: Path):
    path = _toml(
        tmp_path,
        "[dependencies]\n"
        '"repair.preql" = { after = ["refresh.preql", "sub/other.preql"], when = "failed" }\n'
        '"audit.preql" = { after = "refresh.preql" }\n',
    )
    assert load_declared_dependencies(path) == [
        DeclaredDependency(
            script=(tmp_path / "repair.preql").resolve(),
            after=(
                (tmp_path / "refresh.preql").resolve(),
                (tmp_path / "sub" / "other.preql").resolve(),
            ),
            when="failed",
        ),
        DeclaredDependency(
            script=(tmp_path / "audit.preql").resolve(),
            after=((tmp_path / "refresh.preql").resolve(),),
            when="completed",
        ),
    ]


def test_no_section_is_no_dependencies(tmp_path: Path):
    assert (
        load_declared_dependencies(_toml(tmp_path, '[engine]\ndialect = "duck_db"\n'))
        == []
    )


@pytest.mark.parametrize(
    "body, error, match",
    [
        ('[dependencies]\n"a.preql" = ["b.preql"]\n', TypeError, "must be a table"),
        (
            '[dependencies]\n"a.preql" = { when = "failed" }\n',
            ValueError,
            "needs `after`",
        ),
        ('[dependencies]\n"a.preql" = { after = [] }\n', ValueError, "needs `after`"),
        (
            '[dependencies]\n"a.preql" = { after = ["b.preql"], when = "sometimes" }\n',
            ValueError,
            "must be one of",
        ),
        (
            '[dependencies]\n"a.preql" = { after = ["b.preql"], on = "failed" }\n',
            ValueError,
            "unknown keys",
        ),
    ],
)
def test_load_rejects_malformed_entries(tmp_path: Path, body: str, error, match: str):
    with pytest.raises(error, match=match):
        load_declared_dependencies(_toml(tmp_path, body))


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------


def _scripts(tmp_path: Path, **files: str) -> dict[str, str]:
    keys = {}
    for name, body in files.items():
        path = tmp_path / f"{name}.preql"
        path.write_text(body, encoding="utf-8")
        keys[name] = str(ScriptNode(path=path).path)
    return keys


def test_folder_graph_carries_declared_edges(tmp_path: Path):
    keys = _scripts(tmp_path, a="select 1 -> x;", repair="select 2 -> y;")
    _toml(
        tmp_path,
        '[dependencies]\n"repair.preql" = { after = ["a.preql"], when = "failed" }\n',
    )
    graph = DependencyResolver().build_folder_graph(tmp_path)
    assert graph.has_edge(keys["a"], keys["repair"])
    assert edge_condition(graph, keys["a"], keys["repair"]) == "failed"


def test_derived_edges_read_as_completed(tmp_path: Path):
    keys = _scripts(tmp_path, a="key x int;", b="import a;\nselect x;")
    graph = DependencyResolver().build_folder_graph(tmp_path)
    assert edge_condition(graph, keys["a"], keys["b"]) == "completed"


def test_declared_edge_naming_an_unknown_script_is_refused(tmp_path: Path):
    _scripts(tmp_path, a="select 1 -> x;")
    _toml(
        tmp_path,
        '[dependencies]\n"repair.preql" = { after = ["a.preql"], when = "failed" }\n',
    )
    with pytest.raises(ValueError, match="repair.preql, which is not a script"):
        DependencyResolver().build_folder_graph(tmp_path)


def test_declared_cycle_is_refused(tmp_path: Path):
    _scripts(tmp_path, a="key x int;", b="import a;\nselect x;")
    _toml(
        tmp_path,
        '[dependencies]\n"a.preql" = { after = ["b.preql"], when = "always" }\n',
    )
    with pytest.raises(ValueError, match="Circular"):
        DependencyResolver().build_folder_graph(tmp_path)


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

A, B, REPAIR, NOTIFY = (
    str(Path(f"/scripts/{n}.preql")) for n in ("a", "b", "repair", "notify")
)


def _graph(edges: list[tuple[str, str, str | None]]) -> nx.DiGraph:
    """Only the nodes the edges name, so `ready` reads as what this graph released."""
    graph = nx.DiGraph()
    for upstream, dependent, when in edges:
        graph.add_edge(upstream, dependent)
        if when:
            graph.edges[(upstream, dependent)]["when"] = when
    return graph


def _finish(state: RunState, node: str, success: bool) -> None:
    """Claim `node` off the ready queue and complete it, as a worker would."""
    if node in state.ready:
        state.ready.remove(node)
    state.in_progress.add(node)
    _mark_node_complete(state, node, success, None)


def _result_for(state: RunState, node: str) -> ExecutionResult:
    return next(r for r in state.results if str(r.node.path) == node)


def test_repair_runs_when_its_upstream_fails():
    state = RunState.for_graph(_graph([(A, REPAIR, "failed")]))
    _finish(state, A, success=False)
    assert state.ready == [REPAIR]
    assert state.results == []


def test_repair_stands_down_when_its_upstream_succeeds():
    state = RunState.for_graph(_graph([(A, REPAIR, "failed")]))
    _finish(state, A, success=True)
    assert state.ready == []
    result = _result_for(state, REPAIR)
    assert result.success and result.skipped
    assert "a.preql did not fail" in str(result.error)
    assert REPAIR in state.completed and REPAIR not in state.failed


def test_always_runs_either_way():
    for success in (True, False):
        state = RunState.for_graph(_graph([(A, REPAIR, "always")]))
        _finish(state, A, success=success)
        assert state.ready == [REPAIR]


def test_derived_edge_still_skips_on_failure():
    state = RunState.for_graph(_graph([(A, B, None)]))
    _finish(state, A, success=False)
    result = _result_for(state, B)
    assert not result.success and result.skipped
    assert B in state.failed


def test_a_dependency_skip_is_not_a_failure():
    # a -> b (derived), b -> repair (when=failed). a fails, so b is skipped —
    # but b did not *fail*, so the repair for b has nothing to repair.
    state = RunState.for_graph(_graph([(A, B, None), (B, REPAIR, "failed")]))
    _finish(state, A, success=False)
    assert state.ready == []
    assert not _result_for(state, B).success
    repair = _result_for(state, REPAIR)
    assert repair.success and repair.skipped


def test_a_stood_down_repair_does_not_release_its_downstream():
    # notify exists to announce the repair. The repair never ran, so there is
    # nothing to announce — and nothing failed either, so the skip is green.
    state = RunState.for_graph(_graph([(A, REPAIR, "failed"), (REPAIR, NOTIFY, None)]))
    _finish(state, A, success=True)
    assert state.ready == []
    notify = _result_for(state, NOTIFY)
    assert notify.success and notify.skipped
    assert "repair.preql stood down" in str(notify.error)
    assert NOTIFY not in state.failed


def test_a_repair_that_fired_releases_its_downstream():
    state = RunState.for_graph(_graph([(A, REPAIR, "failed"), (REPAIR, NOTIFY, None)]))
    _finish(state, A, success=False)
    assert state.ready == [REPAIR]
    _finish(state, REPAIR, success=True)
    assert state.ready == [NOTIFY]


def test_a_repair_that_fired_and_broke_skips_its_downstream_red():
    state = RunState.for_graph(_graph([(A, REPAIR, "failed"), (REPAIR, NOTIFY, None)]))
    _finish(state, A, success=False)
    _finish(state, REPAIR, success=False)
    assert state.ready == []
    notify = _result_for(state, NOTIFY)
    assert not notify.success and notify.skipped
    assert NOTIFY in state.failed


def test_repair_waits_for_every_upstream():
    state = RunState.for_graph(_graph([(A, REPAIR, "failed"), (B, REPAIR, "failed")]))
    _finish(state, A, success=False)
    assert REPAIR not in state.ready  # b still running
    _finish(state, B, success=True)
    assert state.ready == [REPAIR]


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------

PR_URL = "https://github.com/o/r/pull/45"

BROKEN = """key fid int;

datasource broken (
    fid
)
grain (fid)
query '''
select not_a_real_column as fid
''';

select fid;
"""

OK = "select 1 -> num;\n"


def _repair_workspace(tmp_path: Path, upstream_body: str) -> None:
    (tmp_path / "upstream.preql").write_text(upstream_body, encoding="utf-8")
    (tmp_path / "repair.preql").write_text("call `./repair.py`;\n", encoding="utf-8")
    (tmp_path / "repair.py").write_text(
        f"print('::trilogy-output name=fix_pr value={PR_URL}')\n", newline="\n"
    )
    _toml(
        tmp_path,
        '[engine]\ndialect = "duck_db"\n\n'
        '[dependencies]\n"repair.preql" = { after = ["upstream.preql"], when = "failed" }\n',
    )


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _file_end(records: list[dict], name: str) -> dict:
    return next(
        r for r in records if r["type"] == "file_end" and Path(r["file"]).name == name
    )


@rich_modes
def test_run_directory_fires_the_repair_on_failure(tmp_path: Path, rich: bool):
    _repair_workspace(tmp_path, BROKEN)
    report = tmp_path / "report.jsonl"
    with rich_mode(rich):
        result = CliRunner().invoke(
            cli, ["run", str(tmp_path), "duck_db", "--report-file", str(report)]
        )
    # The upstream failed, so the run is red — but the repair ran and reported.
    assert result.exit_code == 1, result.output
    records = _records(report)
    assert _file_end(records, "upstream.preql")["success"] is False
    repair = _file_end(records, "repair.preql")
    assert repair["success"] is True and "skipped" not in repair
    outputs = [r for r in records if r["type"] == "output"]
    assert [(o["name"], o["value"]) for o in outputs] == [("fix_pr", PR_URL)]
    summary = records[-1]
    assert summary["type"] == "summary"
    assert (summary["succeeded"], summary["failed"], summary["skipped"]) == (1, 1, 0)
    assert_output_listed(result.output, "fix_pr", "link", PR_URL, rich)


def test_run_directory_stands_the_repair_down_on_success(tmp_path: Path):
    _repair_workspace(tmp_path, OK)
    report = tmp_path / "report.jsonl"
    result = CliRunner().invoke(
        cli, ["run", str(tmp_path), "duck_db", "--report-file", str(report)]
    )
    assert result.exit_code == 0, result.output
    records = _records(report)
    repair = _file_end(records, "repair.preql")
    assert repair["success"] is True and repair["skipped"] is True
    assert "did not fail" in repair["error"]
    assert not [r for r in records if r["type"] == "output"]
    summary = records[-1]
    assert (summary["succeeded"], summary["failed"], summary["skipped"]) == (1, 0, 1)
    assert summary["success"] is True
