"""`import_paths` fallback roots: an import that misses under working_path
resolves against each configured root in order (Environment field, wired from
trilogy.toml `import_paths`)."""

from __future__ import annotations

import pytest

from trilogy.core.models.environment import Environment
from trilogy.execution.config import audit_config_file, load_config_file

MODEL = (
    "key order_id int;\nproperty order_id.amount float;\n"
    "datasource orders (order_id: order_id, amount: amount) "
    "grain (order_id) address orders_tbl;\n"
)


@pytest.fixture
def roots(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "orders.preql").write_text(MODEL, encoding="utf-8")
    work = tmp_path / "work"
    work.mkdir()
    return work, model


@pytest.fixture
def model_subdir(roots):
    _, model = roots
    nested = model / "nested"
    nested.mkdir()
    (nested / "deep.preql").write_text("key deep_key int;\n", encoding="utf-8")
    return nested


def test_fallback_resolution(roots):
    work, model = roots
    env = Environment(working_path=work, import_paths=[model])
    env.parse("import orders as o;\nselect sum(o.amount) -> t;")
    assert "o.amount" in env.concepts


def test_working_path_wins(roots):
    work, model = roots
    (work / "orders.preql").write_text(
        "key local_only int;\n",
        encoding="utf-8",
    )
    env = Environment(working_path=work, import_paths=[model])
    env.parse("import orders as o;")
    assert "o.local_only" in env.concepts
    assert "o.amount" not in env.concepts


def test_first_root_wins(roots, tmp_path):
    work, model = roots
    other = tmp_path / "other"
    other.mkdir()
    (other / "orders.preql").write_text("key other_only int;\n", encoding="utf-8")
    env = Environment(working_path=work, import_paths=[other, model])
    env.parse("import orders as o;")
    assert "o.other_only" in env.concepts


def test_miss_still_errors_against_working_path(roots):
    work, model = roots
    env = Environment(working_path=work, import_paths=[model])
    with pytest.raises(Exception, match="nothere"):
        env.parse("import nothere;")


def test_nested_imports_inherit_roots(roots, tmp_path):
    work, model = roots
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "common.preql").write_text("key shared_key int;\n", encoding="utf-8")
    # model file imports `common`, which lives only in the second root
    (model / "entry.preql").write_text("import common as c;\n", encoding="utf-8")
    env = Environment(working_path=work, import_paths=[model, shared])
    env.parse("import entry as e;")
    assert "e.c.shared_key" in env.concepts


@pytest.mark.parametrize("target", ["orders", "orders.preql"])
def test_add_file_import_uses_fallback_roots(roots, target):
    work, model = roots
    env = Environment(working_path=work, import_paths=[model])
    env.add_file_import(target, "o")
    assert "o.amount" in env.concepts


def test_add_file_import_dotted_path_uses_fallback_roots(roots, model_subdir):
    work, model = roots
    env = Environment(working_path=work, import_paths=[model])
    env.add_file_import("nested.deep", "d")
    assert "d.deep_key" in env.concepts


def test_add_file_import_prefers_working_path(roots):
    work, model = roots
    (work / "orders.preql").write_text("key local_only int;\n", encoding="utf-8")
    env = Environment(working_path=work, import_paths=[model])
    env.add_file_import("orders", "o")
    assert "o.local_only" in env.concepts


def test_add_file_import_propagates_roots_to_nested_imports(roots, tmp_path):
    work, model = roots
    shared = tmp_path / "shared2"
    shared.mkdir()
    (shared / "common.preql").write_text("key shared_key int;\n", encoding="utf-8")
    (model / "entry.preql").write_text("import common as c;\n", encoding="utf-8")
    env = Environment(working_path=work, import_paths=[model, shared])
    env.add_file_import("entry", "e")
    assert "e.c.shared_key" in env.concepts


def test_duplicate_carries_import_paths(roots):
    work, model = roots
    env = Environment(working_path=work, import_paths=[model])
    assert env.duplicate().import_paths == [model]


@pytest.mark.parametrize("entry", ['["../model"]', '"../model"'])
def test_toml_import_paths_load_and_audit(roots, entry):
    work, model = roots
    toml = work / "trilogy.toml"
    toml.write_text(
        f'import_paths = {entry}\n\n[engine]\ndialect = "duck_db"\n',
        encoding="utf-8",
    )
    config = load_config_file(toml)
    assert config.import_paths == [model.resolve()]
    assert not audit_config_file(toml)
