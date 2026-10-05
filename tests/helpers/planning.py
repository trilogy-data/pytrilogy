from collections.abc import Callable
from typing import Any

from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import Factory, get_canonical_pseudonyms
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import plan_trace
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.concept_strategies_v4 import (
    search_concepts as search_concepts_v4,
)
from trilogy.core.processing.nodes import History
from trilogy.core.processing.v4_helper.models import BuildInfo
from trilogy.executor import Executor
from trilogy.parser import parse_text


def plan(model: str, query: str) -> tuple[BuildInfo, BuildEnvironment]:
    """The v4 planner's build info for `query`'s SELECT, without its WHERE."""
    env = Environment()
    env, _ = env.parse(model)
    _, parsed = parse_text(query, env)
    statement = parsed[-1]
    history = History(base_environment=env)
    history.build_caches.pseudonym_map = get_canonical_pseudonyms(env)
    factory = Factory(
        environment=env,
        build_cache=history.build_caches.build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        grain_build_cache=history.build_caches.grain_build_cache,
        pseudonym_map=history.build_caches.pseudonym_map,
    )
    built = factory.build(statement.as_lineage(env))
    build_env = env.materialize_for_select(
        built.local_concepts,
        build_cache=history.build_caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=factory.grain_build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        datasource_build_cache=history.build_caches.datasource_build_cache,
    )
    info = search_concepts_v4(
        list(built.output_components),
        V4History(base_environment=env, build_caches=history.build_caches),
        build_env,
        0,
        generate_graph(build_env),
        conditions=[],
    )
    return info, build_env


def recorded(executor: Executor, query: str) -> plan_trace.PlanTrace:
    with plan_trace.recording(query) as trace:
        executor.generate_sql(query)
    return trace


def built_groups(
    trace: plan_trace.PlanTrace, derivation: str | None = None
) -> list[str]:
    return [
        s.data.group
        for s in trace.steps
        if s.phase == "node" and derivation in (None, s.data.derivation)
    ]


class Spy:
    """Calls `wrapped`, recording `record(result, *args, **kwargs)` per call
    (the result itself by default)."""

    def __init__(
        self, wrapped: Callable[..., Any], record: Callable[..., Any] | None = None
    ) -> None:
        self.wrapped = wrapped
        self.record = record
        self.seen: list[Any] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        result = self.wrapped(*args, **kwargs)
        if self.record is None:
            self.seen.append(result)
        else:
            self.seen.append(self.record(result, *args, **kwargs))
        return result
