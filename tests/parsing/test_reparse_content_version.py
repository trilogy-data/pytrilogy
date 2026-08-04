"""Re-parsing identical statements must not invalidate content_version-stamped
caches (the serve/dashboard pattern: one persistent Environment, each request
re-parsed). Effective writes still bump; overlay push/pop bumps only the raw
`mutations` counter that parse-scoped caches key on."""

from pathlib import Path

import pytest

from trilogy import Environment, parse
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


def test_new_declaration_evicts_bundle(model_dir: Path):
    env = Environment(working_path=str(model_dir))
    parse(QUERY, env)
    bundle = _session_build_caches(env, None)
    parse("auto shouty <- upper(base.name);", env)
    assert _session_build_caches(env, None) is not bundle


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
