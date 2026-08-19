"""Refresh planning must not evict the session build-environment caches.

Every statement in a refresh plan used to rebuild the build environment
(O(N^2) in model size): probe hide/restore bumped the datasource dict's
counters unconditionally, and each probe's parse committed a fresh alias
concept — both invalidating the content_version stamp that guards the
cross-statement BuildCaches store. These tests pin the mechanism, not the
wall clock: probes now run stamp-neutrally (ephemeral parse + counter
snapshot/restore) so a whole plan materializes the environment at most twice
— once for the full environment, once for the probes' hidden view.
"""

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.execution.state import (
    create_refresh_plan,
    execute_refresh_plan,
)
from trilogy.execution.state.isolation import hidden_datasources

MODEL = """
key tree_id int;
property tree_id.species string;
property <*>.muni_time datetime;
property <*>.community_time datetime;
auto published_through <- greatest(muni_time, community_time);

root datasource muni_clock (
    muni_time: muni_time
)
query '''SELECT TIMESTAMP '2026-08-01' AS muni_time''';

root datasource community_clock (
    community_time: community_time
)
query '''SELECT TIMESTAMP '2026-07-01' AS community_time''';

root datasource raw_trees (
    tree_id: tree_id,
    species: species,
)
grain (tree_id)
query '''SELECT 1 AS tree_id, 'oak' AS species''';

datasource published_trees (
    tree_id: tree_id,
    species: species,
    published_through: published_through,
)
grain (tree_id)
address published_trees
freshness by published_through;
"""


def _executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    return executor


def test_refresh_plan_materializes_the_environment_at_most_twice(monkeypatch):
    """One baseline for the full environment, one for the probes' hidden view
    — independent of how many statements or probes the plan holds."""
    executor = _executor()
    calls: list[int] = []
    original = Environment.materialize_baseline

    def counting(self, *args, **kwargs):
        calls.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Environment, "materialize_baseline", counting)
    plan = create_refresh_plan(executor)
    execute_refresh_plan(executor, plan, dry_run=True)
    assert len(calls) <= 2, f"{len(calls)} baseline builds for one refresh plan"


def test_execute_ephemeral_leaves_no_durable_writes():
    executor = _executor()
    env = executor.environment
    pre = (
        env.concepts.content_version,
        env.datasources.content_version,
        len(env.concepts.data),
    )
    result = executor.execute_ephemeral("SELECT MAX(muni_time) -> _probe_0;")
    assert result is not None
    assert result.fetchone()[0] is not None
    assert "local._probe_0" not in env.concepts.data
    assert pre == (
        env.concepts.content_version,
        env.datasources.content_version,
        len(env.concepts.data),
    )


def test_execute_ephemeral_rejects_definition_statements():
    """Ephemeral safety rests on rollback discarding every durable write,
    which only selects guarantee."""
    executor = _executor()
    with pytest.raises(TypeError, match="ephemeral"):
        executor.execute_ephemeral("key oops int;")


def test_hidden_datasources_restores_the_stamp():
    executor = _executor()
    datasources = executor.environment.datasources
    pre = (datasources.mutations, datasources.content_version)
    with hidden_datasources(executor.environment, ["published_trees"]):
        assert "published_trees" not in datasources
        assert (datasources.mutations, datasources.content_version) != pre
    assert "published_trees" in datasources
    assert (datasources.mutations, datasources.content_version) == pre


def test_hidden_datasources_keeps_the_bump_when_the_scope_writes():
    """Counter restore is only honest for an object-identical restore; any
    other write inside the window must keep its eviction."""
    executor = _executor()
    datasources = executor.environment.datasources
    pre = (datasources.mutations, datasources.content_version)
    with hidden_datasources(executor.environment, ["published_trees"]):
        datasources["extra"] = datasources["raw_trees"]
    assert (datasources.mutations, datasources.content_version) != pre
    del datasources["extra"]
