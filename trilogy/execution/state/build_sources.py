"""Keep a refresh from reading through outputs that have not been built yet.

A model's sibling outputs often answer each other's questions, so the planner
may route a target through one. On a fresh model nothing but the roots exists,
and every first build would fail on a missing file. Each build therefore checks
the non-root sources its plan reads, hides the unbuilt ones and re-plans, which
falls back to whatever can still answer — the roots, in the end.
"""

from collections.abc import Iterable

from trilogy import Executor
from trilogy.constants import logger
from trilogy.core.enums import PersistMode
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.execute import collect_source_addresses
from trilogy.core.statements.author import PersistStatement
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.exceptions import UNRESOLVABLE_ERRORS
from trilogy.execution.state.isolation import hidden_datasources
from trilogy.execution.state.watermarks import is_unbuilt

LOGGER_PREFIX = "[BUILD_SOURCES]"


class UnbuiltSourceError(Exception):
    def __init__(self, target: str, unbuilt: Iterable[str]):
        names = ", ".join(sorted(unbuilt))
        super().__init__(
            f"'{target}' can only be built from source(s) that are not built yet: "
            f"{names}. Build them first, or give the model a root that can "
            "answer these columns."
        )


def read_datasources(
    executor: Executor, target: Datasource, query: ProcessedQuery
) -> set[str]:
    """Identifiers of the non-root datasources other than ``target`` that a
    planned query reads. Matched on address: an inlined leaf may be renamed."""
    addresses = collect_source_addresses(query.ctes)
    return {
        ds_id
        for ds_id, ds in executor.environment.datasources.items()
        if ds_id != target.identifier
        and not ds.is_root
        and ds.address != target.address
        and ds.address in addresses
    }


def _planned_reads(executor: Executor, target: Datasource) -> set[str]:
    """Plan the build exactly as ``update_datasource`` will: a persist, which
    scopes a partial target to its complete-where and never reads itself."""
    env = executor.environment
    persist = PersistStatement(
        datasource=target,
        select=target.create_update_statement(env, None, line_no=None),
        persist_mode=PersistMode.OVERWRITE,
    )
    queries = executor.generator.generate_queries(env, [persist], hooks=executor.hooks)
    return {
        ds_id
        for query in queries
        if isinstance(query, ProcessedQuery)
        for ds_id in read_datasources(executor, target, query)
    }


def hide_unbuilt_sources(
    executor: Executor,
    target: Datasource,
    hidden: Iterable[str],
    cache: ColumnStatsCache | None = None,
) -> set[str]:
    """``hidden`` plus every unbuilt datasource the target's plan would read.

    Re-plans until the plan reads only built sources; raises
    :class:`UnbuiltSourceError` when nothing else can answer."""
    env = executor.environment
    hide = set(hidden)
    checked: set[str] = set()
    unbuilt: set[str] = set()
    while True:
        with hidden_datasources(env, hide):
            try:
                reads = _planned_reads(executor, target)
            except UNRESOLVABLE_ERRORS as e:
                if not unbuilt:
                    raise
                raise UnbuiltSourceError(target.identifier, unbuilt) from e
        found = {
            ds_id
            for ds_id in reads - checked
            if is_unbuilt(env.datasources[ds_id], executor, cache)
        }
        if not found:
            return hide
        logger.info(
            "%s '%s' re-planning without unbuilt source(s): %s",
            LOGGER_PREFIX,
            target.identifier,
            ", ".join(sorted(found)),
        )
        checked |= reads
        unbuilt |= found
        hide |= found
