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

from trilogy.core import graph as nx
from trilogy.scripts.dependency import (
    DependencyResolver,
    ScriptNode,
    edge_condition,
)
from trilogy.scripts.parallel_execution import (
    ExecutionResult,
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

A, B, REPAIR = (str(Path(f"/scripts/{n}.preql")) for n in ("a", "b", "repair"))


def _graph(edges: list[tuple[str, str, str | None]]) -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_nodes_from([A, B, REPAIR])
    for upstream, dependent, when in edges:
        graph.add_edge(upstream, dependent)
        if when:
            graph.edges[(upstream, dependent)]["when"] = when
    return graph


class _Run:
    """The scheduler's state for one run, so a test reads as a sequence of
    outcomes rather than a dozen positional arguments."""

    def __init__(self, graph: nx.DiGraph) -> None:
        self.graph = graph
        self.node_map = {k: ScriptNode(path=Path(k)) for k in graph.nodes()}
        self.completed: set[str] = set()
        self.failed: set[str] = set()
        self.in_progress: set[str] = set()
        self.remaining = {k: graph.in_degree(k) for k in graph.nodes()}
        self.ready: list[str] = []
        self.results: list[ExecutionResult] = []
        self.triggered: set[str] = set()

    def finish(self, node: str, success: bool) -> None:
        self.in_progress.add(node)
        _mark_node_complete(
            node,
            success,
            self.graph,
            self.node_map,
            self.completed,
            self.failed,
            self.in_progress,
            self.remaining,
            self.ready,
            self.results,
            None,
            self.triggered,
        )

    def result_for(self, node: str) -> ExecutionResult:
        return next(r for r in self.results if str(r.node.path) == node)


def test_repair_runs_when_its_upstream_fails():
    run = _Run(_graph([(A, REPAIR, "failed")]))
    run.finish(A, success=False)
    assert run.ready == [REPAIR]
    assert run.results == []


def test_repair_stands_down_when_its_upstream_succeeds():
    run = _Run(_graph([(A, REPAIR, "failed")]))
    run.finish(A, success=True)
    assert run.ready == []
    result = run.result_for(REPAIR)
    assert result.success and result.skipped
    assert "a.preql did not fail" in str(result.error)
    assert REPAIR in run.completed and REPAIR not in run.failed


def test_always_runs_either_way():
    for success in (True, False):
        run = _Run(_graph([(A, REPAIR, "always")]))
        run.finish(A, success=success)
        assert run.ready == [REPAIR]


def test_derived_edge_still_skips_on_failure():
    run = _Run(_graph([(A, B, None)]))
    run.finish(A, success=False)
    result = run.result_for(B)
    assert not result.success and result.skipped
    assert B in run.failed


def test_a_dependency_skip_is_not_a_failure():
    # a -> b (derived), b -> repair (when=failed). a fails, so b is skipped —
    # but b did not *fail*, so the repair for b has nothing to repair.
    run = _Run(_graph([(A, B, None), (B, REPAIR, "failed")]))
    run.finish(A, success=False)
    assert run.ready == []
    assert not run.result_for(B).success
    repair = run.result_for(REPAIR)
    assert repair.success and repair.skipped


def test_repair_waits_for_every_upstream():
    run = _Run(_graph([(A, REPAIR, "failed"), (B, REPAIR, "failed")]))
    run.finish(A, success=False)
    assert run.ready == []  # b still running
    run.finish(B, success=True)
    assert run.ready == [REPAIR]


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


def test_run_directory_fires_the_repair_on_failure(tmp_path: Path):
    _repair_workspace(tmp_path, BROKEN)
    report = tmp_path / "report.jsonl"
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
    assert f"Output fix_pr (link): {PR_URL}" in result.output


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
