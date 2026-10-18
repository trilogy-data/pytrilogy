"""Resolving a rollup's keyspace walks each concept once.

A rollup aggregate is keyed on its every `by` dimension, and each dimension
re-reached the others, so a per-path walk grew with the orderings of the
dimensions: an 8-dimension rollup (q67) planned for minutes.
"""

from unittest import mock

from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import keyspace


def _walks(width: int) -> int:
    dims = [f"d{i}" for i in range(width)]
    model = "\n".join(
        [
            "key id int;",
            "property id.amount int;",
            *(f"property id.{d} string;" for d in dims),
            "datasource facts (id: id, a: amount"
            + "".join(f", {d}: {d}" for d in dims)
            + ") grain (id)",
            "query '''select 1 id, 5 a" + "".join(f", 'x' {d}" for d in dims) + "''';",
        ]
    )
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    listed = ", ".join(dims)
    query = f"select {listed}, sum(amount) as total by rollup ({listed});"
    with mock.patch.object(
        keyspace, "_entity_keys", wraps=keyspace._entity_keys
    ) as walk:
        rows = executor.execute_query(query).fetchall()
    assert len(rows) == width + 1
    return walk.call_count


def test_rollup_keyspace_walk_is_polynomial_in_dimensions():
    narrow = _walks(2)
    wide = _walks(7)
    assert wide <= narrow * 10
