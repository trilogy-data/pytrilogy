from trilogy import parse
from trilogy.core.enums import Modifier
from trilogy.parsing.render import Renderer

PREQL = """
key order_id int;
key customer_id int;
property order_id.price float;

partial datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    price: price,
)
grain (order_id)
address my_table;
"""

PREQL_TILDE = """
key order_id int;
key customer_id int;
property order_id.price float;

~ datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    price: price,
)
grain (order_id)
address my_table;
"""

PREQL_ROOT_PARTIAL = """
key order_id int;
key customer_id int;
property order_id.price float;

root partial datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    price: price,
)
grain (order_id)
address my_table;
"""

PREQL_EXPLICIT_MIXED = """
key order_id int;
key customer_id int;
property order_id.price float;

partial datasource orders (
    order_id: order_id,
    customer_id: ~customer_id,
    price: price,
)
grain (order_id)
address my_table;
"""


def _all_partial(ds) -> bool:
    return all(Modifier.PARTIAL in c.modifiers for c in ds.columns)


def test_partial_keyword():
    env, _ = parse(PREQL)
    ds = env.datasources["orders"]
    assert ds.is_partial
    assert _all_partial(ds)


def test_partial_tilde_syntax():
    env, _ = parse(PREQL_TILDE)
    ds = env.datasources["orders"]
    assert ds.is_partial
    assert _all_partial(ds)


def test_root_partial():
    env, _ = parse(PREQL_ROOT_PARTIAL)
    ds = env.datasources["orders"]
    assert ds.is_root
    assert ds.is_partial
    assert _all_partial(ds)


def test_partial_no_duplicate_modifier():
    """Explicit ~ on a column in a partial datasource should not double-add PARTIAL."""
    env, _ = parse(PREQL_EXPLICIT_MIXED)
    ds = env.datasources["orders"]
    assert ds.is_partial
    for col in ds.columns:
        assert col.modifiers.count(Modifier.PARTIAL) == 1


def test_partial_render_roundtrip():
    env, _ = parse(PREQL)
    ds = env.datasources["orders"]
    r = Renderer()
    rendered = r.to_string(ds)
    assert rendered.startswith("partial datasource")
    env2, _ = parse(rendered)
    ds2 = env2.datasources["orders"]
    assert ds2.is_partial
    assert _all_partial(ds2)


def test_root_partial_render_roundtrip():
    env, _ = parse(PREQL_ROOT_PARTIAL)
    ds = env.datasources["orders"]
    r = Renderer()
    rendered = r.to_string(ds)
    assert rendered.startswith("root partial datasource")
    env2, _ = parse(rendered)
    ds2 = env2.datasources["orders"]
    assert ds2.is_root
    assert ds2.is_partial
