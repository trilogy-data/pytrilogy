from pathlib import Path

from trilogy.scripts.dependency import NoDependencyStrategy, ScriptNode

TEST_NODES = [
    ScriptNode(path=Path("/some/path"), content="select 1 as test;"),
    ScriptNode(path=Path("/some_other/path"), content="select 1 as test;"),
]


def test_no_dependency():
    x = NoDependencyStrategy()

    built = x.build_graph(TEST_NODES)

    assert len(built.edges) == 0
