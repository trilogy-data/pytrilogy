"""Sort a file build by its datasource's ``order by`` as it is written.

The rows to write arrive as SQL whose columns are the target's own (the build,
or for an append the build plus the rows already in the file). The datasource
is pinned to that SQL as a query address and a select of its columns and order
terms is planned against it, so an order term derived from the columns (a
spatial key, say) is computed for the sort without being stored. The sort is
the outermost clause of what ``COPY`` reads, so the engine keeps it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING

from trilogy.core.enums import AddressType, Derivation
from trilogy.core.models.author import Grain
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import Environment
from trilogy.execution.assertions import pinned_datasource
from trilogy.execution.state.isolation import scope_where
from trilogy.parsing.render import Renderer

if TYPE_CHECKING:
    from trilogy.executor import Executor

SORT_KEY_PREFIX = "_trilogy_sort_"


@contextmanager
def stored_columns_as_roots(
    environment: Environment, datasource: Datasource
) -> Iterator[None]:
    """Plan the sort with every column the datasource stores read as stored.

    The rows being sorted are the datasource's own, so a column it holds is a
    fact of the row, not something to evaluate again. Left derived, a column
    such as `coalesce(common_names[1], species)` is re-planned from its
    inputs, which the pinned scope does not bind, or grain-pinned to the sort
    select's keys (`grain_pin`), and the write fails to plan. For the scope,
    each such concept is its root twin: same address, type and keys, no
    lineage. The originals are put back object-identical, counters included,
    so the environment's planning caches survive (as `hidden_datasources`).
    """
    concepts = environment.concepts
    derived = {
        c.concept.address: concepts[c.concept.address]
        for c in datasource.columns
        if c.is_concrete
        and c.concept.address in concepts
        and concepts[c.concept.address].lineage is not None
    }
    pre = (concepts.mutations, concepts.content_version)
    for address, concept in derived.items():
        # A root property's grain is its keys; the lineage's inputs no longer
        # widen it.
        grain = Grain(components=set(concept.keys)) if concept.keys else concept.grain
        concepts[address] = replace(
            concept, lineage=None, derivation=Derivation.ROOT, grain=grain
        )
    try:
        yield
    finally:
        for address, concept in derived.items():
            concepts[address] = concept
        if derived:
            concepts.mutations, concepts.content_version = pre


def sorted_rows_sql(executor: Executor, datasource: Datasource, sql: str) -> str:
    assert datasource.order_by is not None
    renderer = Renderer()
    quote = executor.generator.QUOTE_CHARACTER
    keys = [
        (f"{SORT_KEY_PREFIX}{i}", item.order.value)
        for i, item in enumerate(datasource.order_by.items)
    ]
    columns = {
        c.concept.address: c.alias
        for c in datasource.columns
        if c.is_concrete and isinstance(c.alias, str)
    }
    items = [
        f"{renderer.to_string(item.expr)} -> {name}"
        for item, (name, _) in zip(datasource.order_by.items, keys)
    ] + [renderer.to_string(c.concept) for c in datasource.columns if c.is_concrete]
    text = "select " + ", ".join(items) + scope_where(datasource) + ";"
    address = Address(location=sql, type=AddressType.QUERY)
    with (
        stored_columns_as_roots(executor.environment, datasource),
        pinned_datasource(executor.environment, datasource, address),
    ):
        planned = executor.plan_ephemeral(text)
        inner = executor.compile_for_execution(planned)
    named = {col.name: col.safe_address for col in planned.output_columns}
    projected = ", ".join(
        f"{quote}{col.safe_address}{quote} AS {quote}{columns[col.address]}{quote}"
        for col in planned.output_columns
        if col.address in columns
    )
    order = ", ".join(
        f"{quote}{named[name]}{quote} {direction}" for name, direction in keys
    )
    return f"SELECT {projected} FROM ({inner}) AS _trilogy_sorted ORDER BY {order}"
