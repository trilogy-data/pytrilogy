"""Sort a file build by its datasource's ``order by`` as it is written.

The rows to write arrive as SQL whose columns are the target's own (the build,
or for an append the build plus the rows already in the file). The datasource
is pinned to that SQL as a query address and a select of its columns and order
terms is planned against it, so an order term derived from the columns (a
spatial key, say) is computed for the sort without being stored. The sort is
the outermost clause of what ``COPY`` reads, so the engine keeps it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.execution.assertions import pinned_datasource
from trilogy.execution.state.isolation import scope_where
from trilogy.parsing.render import Renderer

if TYPE_CHECKING:
    from trilogy.executor import Executor

SORT_KEY_PREFIX = "_trilogy_sort_"


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
    with pinned_datasource(executor.environment, datasource, address):
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
