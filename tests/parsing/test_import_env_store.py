"""Cross-parse import environment store: reuse, invalidation, and safety.

The store shares parsed import Environments across fresh top-level parses.
Entries are validated by re-hashing the transitive text closure and by an
env-integrity stamp; anything not context-free (cycles, dict-resolver text)
never enters the store.
"""

from pathlib import Path

import pytest

from trilogy import Environment, parse
from trilogy.core.models.environment import DictImportResolver, EnvironmentConfig
from trilogy.parsing.v2 import import_service as isvc


@pytest.fixture(autouse=True)
def clean_store():
    isvc.clear_import_env_store()
    yield
    isvc.clear_import_env_store()


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "base.preql").write_text(
        "key id int;\nproperty id.name string;\n"
        "datasource base_ds (id: id, name: name) grain (id) address base_tbl;\n"
    )
    (tmp_path / "mid.preql").write_text(
        "import base as base;\nauto id_doubled <- base.id * 2;\n"
    )
    return tmp_path


ROOT_TEXT = "import mid as mid;\nselect mid.base.id, mid.id_doubled;"


def _sig(env: Environment) -> tuple:
    return (
        sorted(env.concepts.data.keys()),
        sorted(env.datasources.keys()),
        sorted(env.alias_origin_lookup.keys()),
    )


def _fresh(model_dir: Path) -> Environment:
    env = Environment(working_path=str(model_dir))
    parse(ROOT_TEXT, env)
    return env


def test_store_reuse_is_equivalent(model_dir: Path):
    reference = _fresh(model_dir)
    assert len(isvc._IMPORT_ENV_STORE) == 2
    warm = _fresh(model_dir)
    assert _sig(reference) == _sig(warm)
    # the second parse reused cached envs rather than re-parsing
    assert len(isvc._IMPORT_ENV_STORE) == 2


def test_transitive_content_invalidation(model_dir: Path):
    first = _fresh(model_dir)
    assert "mid.base.extra" not in first.concepts.data
    base = model_dir / "base.preql"
    base.write_text(base.read_text() + "property id.extra string;\n")
    # editing base must invalidate mid's cached env (mid imports base)
    second = _fresh(model_dir)
    assert "mid.base.extra" in second.concepts.data


def test_integrity_invalidation_on_shared_mutation(model_dir: Path):
    _fresh(model_dir)
    entry = next(
        e
        for e in isvc._IMPORT_ENV_STORE.values()
        if any(k.endswith("base_ds") for k in e.env.datasources)
    )
    cached_ds = next(iter(entry.env.datasources.values()))
    cached_ds.columns = cached_ds.columns[:-1]
    # the mutated entry is evicted and re-parsed; the fresh parse sees full columns
    recovered = _fresh(model_dir)
    ds = next(v for k, v in recovered.datasources.items() if k.endswith("base_ds"))
    assert len(ds.columns) == 2


def test_cycle_participants_never_cached(tmp_path: Path):
    (tmp_path / "a.preql").write_text("import b as b;\nkey a_id int;\n")
    (tmp_path / "b.preql").write_text("import a as a;\nkey b_id int;\n")
    env = Environment(working_path=str(tmp_path))
    parse("import a as a;", env)
    assert len(isvc._IMPORT_ENV_STORE) == 0
    # a second parse still resolves the cycle the same way
    env2 = Environment(working_path=str(tmp_path))
    parse("import a as a;", env2)
    assert sorted(env2.concepts.data.keys()) == sorted(env.concepts.data.keys())


def test_dict_resolver_bypasses_store():
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={"leaf": "key id int;\n"})
    )
    env = Environment(config=config)
    parse("import leaf as leaf;\nselect leaf.id;", env)
    assert "leaf.id" in env.concepts.data
    assert len(isvc._IMPORT_ENV_STORE) == 0


def test_store_disabled_flag(model_dir: Path, monkeypatch):
    monkeypatch.setattr(isvc, "IMPORT_ENV_STORE_ENABLED", False)
    reference = _fresh(model_dir)
    assert len(isvc._IMPORT_ENV_STORE) == 0
    warm = _fresh(model_dir)
    assert _sig(reference) == _sig(warm)


def test_datasource_key_propagation_does_not_poison_store(tmp_path: Path):
    """A datasource declaring an imported KEY concept at a wider grain re-keys
    that concept (FK propagation) — the rewrite must stay scoped to the
    declaring environment, not leak into the shared cached child env."""
    (tmp_path / "child.preql").write_text("key code string;\n")
    (tmp_path / "root.preql").write_text(
        "import child;\n"
        "key tag string;\n"
        "datasource wide (tag: tag, code: ?code) grain (tag) address wide_tbl;\n"
    )
    env_a = Environment(working_path=str(tmp_path))
    parse("import root as root;", env_a)
    env_b = Environment(working_path=str(tmp_path))
    parse("import child as child;", env_b)
    assert not env_b.concepts.data["child.code"].keys


def test_distinct_parameters_get_distinct_entries(model_dir: Path):
    _fresh(model_dir)
    env = Environment(working_path=str(model_dir))
    env.parameters["run_date"] = "2026-08-04"
    parse(ROOT_TEXT, env)
    # same files, different parameter fingerprint -> separate entries
    assert len(isvc._IMPORT_ENV_STORE) == 4
