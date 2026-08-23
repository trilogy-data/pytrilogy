"""An import must not rewrite the source environment's custom functions.

A parsed import environment is shared - process-wide by the import store, and
object-for-object with its importer on a bare ``import x;`` - so the factory an
aliased import namespaces is the same object the next edge namespaces. Mutating
it re-prefixed the previous edge's work until the accumulated address resolved
to nothing (``Undefined concept: launch.launch....manufacturer.col``).
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from trilogy import Environment, parse
from trilogy.parsing.v2 import import_service as isvc

ROOT_TEXT = "import mid as one;\nimport mid as two;\nimport leaf as direct;\n"


@pytest.fixture(autouse=True)
def clean_store():
    isvc.clear_import_env_store()
    yield
    isvc.clear_import_env_store()


@pytest.fixture
def diamond(tmp_path: Path) -> Path:
    """helper is reached under four aliases through two levels of nesting."""
    (tmp_path / "helper.preql").write_text("def double(col) -> col * 2;\n")
    (tmp_path / "leaf.preql").write_text(
        "import helper;\nkey id int;\nproperty id.doubled <- @double(id);\n"
    )
    (tmp_path / "mid.preql").write_text(
        "import helper;\nimport leaf as left;\nimport leaf as right;\n"
    )
    return tmp_path


def _arg_names(env: Environment) -> dict[str, list[str]]:
    return {
        key: [arg.name for arg in factory.function_arguments]
        for key, factory in env.functions.items()
    }


def _parse_root(model_dir: Path) -> dict[str, list[str]]:
    env = Environment(working_path=str(model_dir))
    parse(ROOT_TEXT, env)
    return _arg_names(env)


def test_with_namespace_returns_a_copy():
    env = Environment()
    parse("key x int;\ndef double(col) -> col * 2;", env)
    source = env.functions["double"]

    first = source.with_namespace("a")
    assert first is not source
    assert [arg.name for arg in source.function_arguments] == ["col"]
    assert [arg.name for arg in first.function_arguments] == ["a.col"]

    second = source.with_namespace("b")
    assert [arg.name for arg in second.function_arguments] == ["b.col"]
    assert [arg.name for arg in first.function_arguments] == ["a.col"]
    assert [arg.name for arg in source.function_arguments] == ["col"]


def test_argument_addresses_match_the_import_depth(diamond: Path):
    for key, args in _parse_root(diamond).items():
        for arg in args:
            assert arg.count(".") <= key.count("."), (key, arg)


def test_concurrent_parses_match_a_sequential_parse(diamond: Path):
    """The directory runner parses models in a thread pool against the shared
    import store; a mutating with_namespace tore between body and arguments."""
    expected = _parse_root(diamond)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(_parse_root, [diamond] * 8))
    assert all(result == expected for result in results)


def test_custom_function_resolves_through_nested_aliases(diamond: Path):
    env = Environment(working_path=str(diamond))
    parse(ROOT_TEXT, env)
    assert env.concepts["one.left.doubled"].lineage is not None
