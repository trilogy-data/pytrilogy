"""Re-parsing identical statements must not invalidate content_version-stamped
caches (the serve/dashboard pattern: one persistent Environment, each request
re-parsed). Effective writes still bump; overlay push/pop bumps only the raw
`mutations` counter that parse-scoped caches key on."""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment, parse
from trilogy.core.models.datasource import DatasourceState
from trilogy.core.query_processor import _session_build_caches


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "base.preql").write_text(
        "key id int;\nproperty id.name string;\n"
        "datasource base_ds (id: id, name: name) grain (id) address base_tbl;\n"
    )
    return tmp_path


QUERY = "import base as base;\nselect base.id, base.name || '!' as loud_name;"


def test_identical_reparse_keeps_session_bundle(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    bundle = _session_build_caches(env, None)
    concepts_cv = env.concepts.content_version
    datasources_cv = env.datasources.content_version
    parse(QUERY, env)
    assert env.concepts.content_version == concepts_cv
    assert env.datasources.content_version == datasources_cv
    assert _session_build_caches(env, None) is bundle


def test_new_declaration_keeps_bundle_and_extends_baseline(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.generate_sql(QUERY)
    bundle = _session_build_caches(env, None)
    (baseline,) = bundle.env_baselines.values()
    units = len(baseline.units)
    sv = env.concepts.structure_version
    parse("auto shouty <- upper(base.name);", env)
    assert env.concepts.structure_version == sv
    assert _session_build_caches(env, None) is bundle
    executor.generate_sql("select base.id, shouty;")
    assert bundle.env_baselines[next(iter(bundle.env_baselines))] is baseline
    assert len(baseline.units) > units
    assert "local.shouty" in baseline.environment.concepts


def test_rebound_declaration_evicts_bundle(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    bundle = _session_build_caches(env, None)
    sv = env.concepts.structure_version
    parse("import base as base;\nselect base.id, base.name || '?' as loud_name;", env)
    assert env.concepts.structure_version > sv
    assert _session_build_caches(env, None) is not bundle


def test_alias_sequence_matches_fresh_environment(model_dir: Path):
    """A persistent environment answering a run of statements, each registering
    a new output alias, must render byte-identically to a fresh environment
    per statement: the extended baseline is the full build."""
    statements = [
        "import base as base;\nselect base.id, base.name || '!' as loud_name;",
        "import base as base;\nselect base.name as label, count(base.id) as n;",
        "import base as base;\nselect label, n where n > 1;",
        "import base as base;\nselect upper(loud_name) as shouty, base.id;",
    ]
    persistent = Environment(working_path=str(model_dir))
    executor = Dialects.DUCK_DB.default_executor(environment=persistent)
    fresh_env = Environment(working_path=str(model_dir))
    fresh = Dialects.DUCK_DB.default_executor(environment=fresh_env)
    for statement in statements:
        assert executor.generate_sql(statement) == fresh.generate_sql(statement)
    assert len(_session_build_caches(persistent, None).env_baselines) == 1


def test_changed_lineage_replaces_concept(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    cv = env.concepts.content_version
    parse("import base as base;\nselect base.id, base.name || '?' as loud_name;", env)
    assert env.concepts.content_version > cv
    assert ",?)" in str(env.concepts["local.loud_name"].lineage)


def test_status_survives_identical_reparse(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    ds = next(iter(env.datasources.values()))
    ds.status = DatasourceState.UNPUBLISHED
    parse(QUERY, env)
    assert next(iter(env.datasources.values())).status == DatasourceState.UNPUBLISHED


def test_overlay_ops_bump_mutations_not_content_version(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    mutations = env.concepts.mutations
    cv = env.concepts.content_version
    with env.concepts.push_overlay({}):
        pass
    assert env.concepts.mutations == mutations + 2
    assert env.concepts.content_version == cv
