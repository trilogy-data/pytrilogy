"""Cross-parse import environment store: reuse, invalidation, and safety.

The store shares parsed import Environments across fresh top-level parses.
Entries are validated by re-hashing the transitive text closure and by an
env-integrity stamp; anything not context-free (a cycle stub) never enters
the store. Dict-resolver entries are keyed on content as well as address, and
their closures are validated by reading back through the resolver.
"""

import copy
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from trilogy import Environment, parse
from trilogy.core.models.environment import DictImportResolver, EnvironmentConfig
from trilogy.parsing.v2 import hydration as hyd
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


def test_store_disabled_flag(model_dir: Path, monkeypatch):
    monkeypatch.setattr(isvc, "IMPORT_ENV_STORE_ENABLED", False)
    reference = _fresh(model_dir)
    assert len(isvc._IMPORT_ENV_STORE) == 0
    warm = _fresh(model_dir)
    assert _sig(reference) == _sig(warm)


def test_datasource_fk_binding_never_rewrites_a_concept(tmp_path: Path):
    """A datasource binding an imported KEY at a wider grain declares an FK, and
    that declaration is derived on demand — it never writes back onto the
    concept, so it cannot leak into the shared cached child env."""
    (tmp_path / "child.preql").write_text("key code string;\n")
    (tmp_path / "root.preql").write_text(
        "import child;\n"
        "key tag string;\n"
        "datasource wide (tag: tag, code: ?code) grain (tag) address wide_tbl;\n"
    )
    env_a = Environment(working_path=str(tmp_path))
    parse("import root as root;", env_a)
    assert not env_a.concepts.data["root.code"].keys
    assert env_a.fk_derived_keys()["root.code"] == frozenset({"root.tag"})

    env_b = Environment(working_path=str(tmp_path))
    parse("import child as child;", env_b)
    assert not env_b.concepts.data["child.code"].keys
    assert "child.code" not in env_b.fk_derived_keys()


def _entry_for(name: str):
    return next(e for e in isvc._IMPORT_ENV_STORE.values() if name in str(e.closure))


def test_projection_shared_across_parses(model_dir: Path):
    first = _fresh(model_dir)
    second = _fresh(model_dir)
    assert _sig(first) == _sig(second)
    # the namespaced copies themselves are reused, not rebuilt per importer
    assert first.concepts.data["mid.base.id"] is second.concepts.data["mid.base.id"]
    entry = _entry_for("mid.preql")
    assert list(entry.projections) == ["mid"]


def test_distinct_aliases_get_distinct_projections(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse("import base as a;\nimport base as b;\nselect a.id, b.id;", env)
    assert env.concepts.data["a.id"] is not env.concepts.data["b.id"]
    assert env.concepts.data["a.id"].address == "a.id"
    assert env.concepts.data["b.id"].address == "b.id"
    assert sorted(_entry_for("base.preql").projections) == ["a", "b"]


def test_projection_rebuilt_after_importer_mutates_shared_datasource(model_dir: Path):
    first = _fresh(model_dir)
    ds = next(v for k, v in first.datasources.items() if k.endswith("base_ds"))
    ds.columns = ds.columns[:-1]
    # the next importer must not inherit the strip through the cached projection
    second = _fresh(model_dir)
    ds2 = next(v for k, v in second.datasources.items() if k.endswith("base_ds"))
    assert len(ds2.columns) == 2


def test_concept_filter_is_per_edge_over_a_shared_projection(model_dir: Path):
    unfiltered = _fresh(model_dir)
    assert "mid.base.name" in unfiltered.concepts

    env = Environment(working_path=str(model_dir))
    parse("from base as base import id;\nselect base.id;", env)
    assert "base.name" in env.concepts.data
    assert "base.name" in env.concepts.hidden
    # the shared projection is not itself narrowed — a later unfiltered import
    # of the same file still sees the full public surface
    after = _fresh(model_dir)
    assert "mid.base.name" in after.concepts


def test_bulk_merge_matches_the_per_concept_loop(model_dir: Path, monkeypatch):
    """The bulk projection merge is only a fast path; forcing it to decline must
    leave the same environment, and it must actually fire on an ordinary
    import (otherwise this test would pass vacuously)."""
    from trilogy.core.models.environment import Environment as Env

    fired: list[bool] = []
    real = Env._bulk_merge_projected_concepts

    def spy(self, projection, concepts):
        took = real(self, projection, concepts)
        fired.append(took)
        return took

    monkeypatch.setattr(Env, "_bulk_merge_projected_concepts", spy)
    bulk = _fresh(model_dir)
    assert any(fired)

    monkeypatch.setattr(
        Env, "_bulk_merge_projected_concepts", lambda self, projection, concepts: False
    )
    isvc.clear_import_env_store()
    looped = _fresh(model_dir)

    assert _sig(bulk) == _sig(looped)
    assert bulk.concepts.hidden == looped.concepts.hidden
    assert {k: str(v.lineage) for k, v in bulk.concepts.data.items()} == {
        k: str(v.lineage) for k, v in looped.concepts.data.items()
    }


def test_bulk_merge_declines_for_a_filtered_import(model_dir: Path, monkeypatch):
    from trilogy.core.models.environment import Environment as Env

    fired: list[bool] = []
    real = Env._bulk_merge_projected_concepts

    def spy(self, projection, concepts):
        took = real(self, projection, concepts)
        fired.append(took)
        return took

    monkeypatch.setattr(Env, "_bulk_merge_projected_concepts", spy)
    env = Environment(working_path=str(model_dir))
    parse("from base as base import id;\nselect base.id;", env)
    assert fired == [False]
    assert "base.name" in env.concepts.hidden


def test_active_env_entries_never_reach_an_unprefixed_parse(model_dir: Path, tmp_path):
    """`env` activation rewrites datasource Address objects in place, and those
    objects are shared into every namespaced copy — so the activation is part of
    the store key rather than something the integrity stamp has to catch."""
    from trilogy import Dialects
    from trilogy.execution.envs import (
        EnvActivation,
        EnvironmentManager,
        datasource_transform_from_active,
        env_activation_scope,
    )

    def parsed(model_dir: Path) -> Environment:
        # mirrors scripts/common.py::create_executor, which is where the CLI
        # installs the deployment-env address rewrite
        env = Environment(working_path=str(model_dir))
        executor = Dialects.DUCK_DB.default_executor(environment=env)
        executor.datasource_transform = datasource_transform_from_active(model_dir)
        executor.parse_text(ROOT_TEXT)
        return env

    manager = EnvironmentManager("proj", home=tmp_path / "home")
    manager.create("dev")
    with env_activation_scope(EnvActivation(name="dev", manager=manager)):
        scoped = parsed(model_dir)
    scoped_ds = next(v for k, v in scoped.datasources.items() if k.endswith("base_ds"))
    assert scoped_ds.safe_address == "dev_base_tbl"

    plain = parsed(model_dir)
    plain_ds = next(v for k, v in plain.datasources.items() if k.endswith("base_ds"))
    assert plain_ds.safe_address == "base_tbl"


def test_distinct_parameters_get_distinct_entries(model_dir: Path):
    _fresh(model_dir)
    env = Environment(working_path=str(model_dir))
    env.parameters["run_date"] = "2026-08-04"
    parse(ROOT_TEXT, env)
    # same files, different parameter fingerprint -> separate entries
    assert len(isvc._IMPORT_ENV_STORE) == 4


def _race_projection_for(
    work_items: list[tuple[isvc._ImportEnvEntry, str]], threads: int
) -> list[BaseException]:
    """Race `threads` readers over each (entry, alias) publish in turn.

    One long-lived thread set covers every alias, and the switch interval drops
    only once each worker has run. Starting threads *under* a 1e-6 interval is
    the hazard: about one newborn in 400 never wins its first GIL acquisition,
    so it reaches no Python frame, arrives at no barrier, and strands the other
    parties on a wait that had no timeout - six hours of ubuntu-3.11 CI per run.
    """
    errors: list[BaseException] = []
    barrier = threading.Barrier(threads)
    ready = threading.Semaphore(0)
    go = threading.Event()

    def work() -> None:
        ready.release()
        try:
            if not go.wait(timeout=60):
                raise TimeoutError("race never started")
            for entry, alias in work_items:
                barrier.wait(timeout=60)
                entry.projection_for(alias)
        except BaseException as e:
            errors.append(e)

    workers = [threading.Thread(target=work) for _ in range(threads)]
    for t in workers:
        t.start()
    for _ in workers:
        if not ready.acquire(timeout=60):
            errors.append(RuntimeError("worker never started"))

    original_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        go.set()
        for t in workers:
            t.join(timeout=120)
            if t.is_alive():
                errors.append(RuntimeError("worker never finished"))
    finally:
        sys.setswitchinterval(original_interval)
    return errors


def test_projection_publish_is_atomic_under_threads(model_dir: Path):
    """A directory run parses on a thread pool against this one global store, so
    several scripts importing the same file under the same alias hit one entry's
    projection cache at once. Publishing the projection and its integrity stamp
    as two writes tears: a reader sees the projection before the stamp exists
    and raises KeyError(alias) — observed in CI as a script failing on the name
    of a module it imports.

    A short switch interval is what makes this deterministic rather than a
    once-per-few-thousand-runs flake; one alias per round, because the tear is
    in publishing a single alias's projection.
    """
    parse(ROOT_TEXT, Environment(working_path=str(model_dir)))
    source = next(iter(isvc._IMPORT_ENV_STORE.values())).env

    errors = _race_projection_for(
        [
            (isvc._ImportEnvEntry(env=source, closure={}, integrity=()), f"ns_{i}")
            for i in range(50)
        ],
        threads=8,
    )

    assert not errors, f"racing projection_for raised: {errors[:3]}"


# --- dict resolver -----------------------------------------------------------

DICT_MODEL = {
    "base": "key id int;\nproperty id.name string;\n"
    "datasource base_ds (id: id, name: name) grain (id) address base_tbl;\n",
    "mid": "import base as base;\nauto id_doubled <- base.id * 2;\n",
    "other": "key other_id int;\n",
}
DICT_ROOT = (
    "import mid as mid;\nimport other as other;\nselect mid.base.id, mid.id_doubled;"
)


def _dict_env(content: dict[str, str], config_cls=EnvironmentConfig) -> Environment:
    return Environment(
        config=config_cls(import_resolver=DictImportResolver(content=dict(content)))
    )


@pytest.fixture
def hydrations(monkeypatch) -> list[str]:
    """Targets hydrated from scratch (the root parse excluded)."""
    seen: list[str] = []
    original = hyd.NativeHydrator.parse

    def spy(self, document, ephemeral=False):
        if self.token_address != "root":
            seen.append(str(self.token_address))
        return original(self, document, ephemeral)

    monkeypatch.setattr(hyd.NativeHydrator, "parse", spy)
    return seen


def test_dict_resolver_second_parse_hydrates_nothing(hydrations: list[str]):
    reference = _dict_env(DICT_MODEL)
    parse(DICT_ROOT, reference)
    assert sorted(hydrations) == ["base", "mid", "other"]
    hydrations.clear()
    warm = _dict_env(DICT_MODEL)
    parse(DICT_ROOT, warm)
    assert hydrations == []
    assert _sig(reference) == _sig(warm)


def test_dict_resolver_edit_rehydrates_only_the_affected_closure(
    hydrations: list[str],
):
    parse(DICT_ROOT, _dict_env(DICT_MODEL))
    hydrations.clear()
    edited = {**DICT_MODEL, "base": DICT_MODEL["base"] + "property id.extra string;\n"}
    env = _dict_env(edited)
    parse(DICT_ROOT, env)
    # base changed, mid imports base; other is untouched
    assert sorted(hydrations) == ["base", "mid"]
    assert "mid.base.extra" in env.concepts.data


def test_dict_resolvers_sharing_an_address_coexist(hydrations: list[str]):
    a = {"leaf": "key a_id int;\n"}
    b = {"leaf": "key b_id int;\n"}
    for content, expected in ((a, "leaf.a_id"), (b, "leaf.b_id"), (a, "leaf.a_id")):
        env = _dict_env(content)
        parse("import leaf as leaf;", env)
        assert expected in env.concepts.data
    # a and b each hydrated exactly once: the second parse of a was a hit
    assert hydrations == ["leaf", "leaf"]
    assert len(isvc._IMPORT_ENV_STORE) == 2


def test_dict_resolver_store_disabled_flag(monkeypatch):
    monkeypatch.setattr(isvc, "IMPORT_ENV_STORE_ENABLED", False)
    reference = _dict_env(DICT_MODEL)
    parse(DICT_ROOT, reference)
    assert len(isvc._IMPORT_ENV_STORE) == 0
    warm = _dict_env(DICT_MODEL)
    parse(DICT_ROOT, warm)
    assert _sig(reference) == _sig(warm)


NESTED_MODEL = {
    "x": "key top_x int;\n",
    "nest.x": "key nested_x int;\n",
    "nest.child": "import x as x;\nauto c <- x.nested_x * 2;\n",
}


@pytest.mark.parametrize(
    "text",
    [
        "import x as x;\nimport nest.child as child;\nselect child.c, x.top_x;",
        "import nest.child as child;\nimport x as x;\nselect child.c, x.top_x;",
    ],
)
def test_nested_dict_import_does_not_collide_with_a_top_level_name(text: str):
    """`nest.child`'s `import x` resolves under its own directory first, so it
    is `nest.x`; the parse-local caches must not confuse it with the top `x`."""
    env = _dict_env(NESTED_MODEL)
    parse(text, env)
    assert {"x.top_x", "child.x.nested_x", "child.c"} <= set(env.concepts.data)


def test_narrowed_resolver_closure_validates_through_copy_for_root(
    hydrations: list[str],
):
    text = "import nest.child as child;\nselect child.c;"
    parse(text, _dict_env(NESTED_MODEL))
    assert sorted(hydrations) == ["nest.child", "x"]
    hydrations.clear()
    parse(text, _dict_env(NESTED_MODEL))
    assert hydrations == []
    # editing the NESTED x (not the top-level one) invalidates nest.child
    edited = {**NESTED_MODEL, "x": "key top_x int;\nkey unrelated int;\n"}
    parse(text, _dict_env(edited))
    assert hydrations == []
    edited = {**NESTED_MODEL, "nest.x": "key nested_x int;\nkey nested_y int;\n"}
    env = _dict_env(edited)
    parse(text, env)
    assert sorted(hydrations) == ["nest.child", "x"]
    assert "child.x.nested_y" in env.concepts.data


class _FlatConfig(EnvironmentConfig):
    """The studio's shape: every dict key is absolute, nothing is narrowed."""

    def copy_for_root(self, root: str | None) -> "_FlatConfig":
        return copy.deepcopy(self)


FLAT_MODEL = {
    "shared.base": "key id int;\n",
    "nest.child": "import shared.base as base;\nauto c <- base.id * 2;\n",
}


def test_flat_config_closure_validates_with_absolute_keys(hydrations: list[str]):
    text = "import nest.child as child;\nimport shared.base as base;\nselect child.c, base.id;"
    reference = _dict_env(FLAT_MODEL, _FlatConfig)
    parse(text, reference)
    # shared.base is a diamond (direct + via nest.child) and hydrates once
    assert sorted(hydrations) == ["nest.child", "shared.base"]
    hydrations.clear()
    warm = _dict_env(FLAT_MODEL, _FlatConfig)
    parse(text, warm)
    assert hydrations == []
    assert _sig(reference) == _sig(warm)
    edited = {**FLAT_MODEL, "shared.base": "key id int;\nkey extra int;\n"}
    env = _dict_env(edited, _FlatConfig)
    parse(text, env)
    assert sorted(hydrations) == ["nest.child", "shared.base"]
    assert "child.base.extra" in env.concepts.data


PARAM_MODEL = {"leaf": "param scale int;\nkey id int;\nauto scaled <- id * scale;\n"}


def test_stored_child_env_does_not_observe_a_later_set_parameters(
    hydrations: list[str],
):
    env = _dict_env(PARAM_MODEL)
    env.set_parameters(scale=2)
    parse("import leaf as leaf;", env)
    entry = next(iter(isvc._IMPORT_ENV_STORE.values()))
    env.set_parameters(scale=3)
    assert entry.env.parameters == {"scale": 2}
    hydrations.clear()
    # a different parameter value is a different entry, not a stale hit
    env2 = _dict_env(PARAM_MODEL)
    env2.set_parameters(scale=3)
    parse("import leaf as leaf;", env2)
    assert hydrations == ["leaf"]
    assert len(isvc._IMPORT_ENV_STORE) == 2


def test_same_dict_model_parsed_concurrently_is_identical():
    def work(_: int) -> tuple:
        env = _dict_env(DICT_MODEL)
        parse(DICT_ROOT, env)
        return _sig(env)

    with ThreadPoolExecutor(max_workers=4) as pool:
        sigs = list(pool.map(work, range(16)))
    assert all(s == sigs[0] for s in sigs)
    assert len(isvc._IMPORT_ENV_STORE) == 3


def test_store_max_is_settable(monkeypatch):
    monkeypatch.setattr(isvc, "_IMPORT_ENV_STORE_MAX", 128)
    parse(DICT_ROOT, _dict_env(DICT_MODEL))
    assert len(isvc._IMPORT_ENV_STORE) == 3
    isvc.set_import_env_store_max(1)
    assert len(isvc._IMPORT_ENV_STORE) == 1
    parse(DICT_ROOT, _dict_env(DICT_MODEL))
    assert len(isvc._IMPORT_ENV_STORE) == 1


FALLBACK_MODEL = {
    "core": "key id int;\n",
    "y": "key top_y int;\n",
    "nest.y": "key nested_y int;\n",
    "nest.child": "import core as core;\nimport y as y;\nauto c <- core.id * 2;\n",
}


def test_nested_dict_import_falls_back_to_the_absolute_address(
    hydrations: list[str],
):
    """A nested file may import a top-level module by its full address (the
    studio ships every address absolute); a relative match still wins."""
    text = "import nest.child as child;\nimport core as core;\nselect child.c;"
    env = _dict_env(FALLBACK_MODEL)
    parse(text, env)
    assert {"child.core.id", "child.y.nested_y", "core.id"} <= set(env.concepts.data)
    # core is one file however it is reached: hydrated once, not once per depth
    assert sorted(hydrations) == ["core", "nest.child", "y"]
    hydrations.clear()
    edited = {**FALLBACK_MODEL, "core": "key id int;\nkey extra int;\n"}
    env = _dict_env(edited)
    parse(text, env)
    assert sorted(hydrations) == ["core", "nest.child"]
    assert "child.core.extra" in env.concepts.data


def test_fallback_import_resolves_its_own_imports_from_its_own_directory():
    """`x` reached from inside `nest` by absolute fallback is the top-level
    file, so ITS relative imports resolve at the top level, not under nest."""
    model = {
        "x": "import y as y;\nauto v <- y.top_y;\n",
        "y": "key top_y int;\n",
        "nest.y": "key nested_y int;\n",
        "nest.child": "import x as x;\nauto c <- x.v * 2;\n",
    }
    env = _dict_env(model)
    parse("import nest.child as child;\nselect child.c;", env)
    assert "child.x.y.top_y" in env.concepts.data
