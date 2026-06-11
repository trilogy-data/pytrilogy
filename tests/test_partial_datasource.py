import pytest

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
    env2, _ = parse(r.to_string(env))
    ds2 = env2.datasources["orders"]
    assert ds2.is_partial
    assert _all_partial(ds2)


def test_root_partial_render_roundtrip():
    env, _ = parse(PREQL_ROOT_PARTIAL)
    ds = env.datasources["orders"]
    r = Renderer()
    rendered = r.to_string(ds)
    assert rendered.startswith("root partial datasource")
    env2, _ = parse(r.to_string(env))
    ds2 = env2.datasources["orders"]
    assert ds2.is_root
    assert ds2.is_partial


def test_intrinsic_partial_captured_outside_partial_keyword():
    """``~col`` on a non-``partial`` datasource is still an intrinsic
    column-level partial — the user's claim that the column has missing
    values stands regardless of whether the table itself is declared partial.
    """
    src = """
key order_id int;
property order_id.order_date date;

datasource web_orders (
    order_id: ~order_id,
    order_date: order_date,
) grain(order_id)
complete where order_date <= cast('2024-01-01' as date)
address web_table;
"""
    env, _ = parse(src)
    ds = env.datasources["web_orders"]
    assert not ds.is_partial  # no partial keyword
    assert "local.order_id" in ds.column_level_partial_addresses


def test_intrinsic_partial_captured_inside_partial_keyword():
    """Inside ``partial datasource``, only columns explicitly tagged ``~``
    qualify as intrinsic — others get their PARTIAL stamp from the keyword."""
    env, _ = parse(PREQL_EXPLICIT_MIXED)
    ds = env.datasources["orders"]
    assert ds.is_partial
    # Only ~customer_id was column-tagged.
    assert ds.column_level_partial_addresses == {"local.customer_id"}


def test_intrinsic_partial_dropped_when_discriminator_matches():
    """Covering UNION over an enum where the intrinsic column IS the
    discriminator: the union covers every value of the column directly, so
    the intrinsic partial drops out at the union level."""
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    src = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a (~category, sales)
grain(category)
complete where category = 'A'
query '''select 'A' as category, 10 as sales''';

datasource ds_b (~category, sales)
grain(category)
complete where category = 'B'
query '''select 'B' as category, 20 as sales''';
"""
    engine = Dialects.DUCK_DB.default_executor(conf=DuckDBConfig())
    engine.execute_text(src)
    rows = engine.execute_text("select category, sales order by category asc;")[
        -1
    ].fetchall()
    assert len(rows) == 2
    assert rows[0].category == "A" and rows[0].sales == 10
    assert rows[1].category == "B" and rows[1].sales == 20


def test_intrinsic_partial_dropped_when_discriminator_is_property():
    """Covering UNION partitioned on a property of the intrinsic column.
    Every value of the intrinsic concept has a determined property value, so
    covering every property value covers every intrinsic-concept value.
    Concretely: orders partitioned by date — every order has a date, dates
    are exhaustively partitioned, so every order is covered."""
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    src = """
key order_id int;
property order_id.order_date date;

datasource web_orders (
    order_id: ~order_id,
    order_date: order_date,
) grain(order_id)
complete where order_date <= cast('2024-01-01' as date)
query '''select 1 as order_id, cast('2024-01-01' as date) as order_date''';

datasource store_orders (
    order_id: ~order_id,
    order_date: order_date,
) grain(order_id)
complete where order_date > cast('2024-01-01' as date)
query '''select 2 as order_id, cast('2024-10-01' as date) as order_date''';
"""
    engine = Dialects.DUCK_DB.default_executor(conf=DuckDBConfig())
    engine.execute_text(src)
    rows = engine.execute_text(
        "select order_id where order_date <= '2024-01-01'::date order by order_id;"
    )[-1].fetchall()
    assert len(rows) == 1
    assert rows[0].order_id == 1


def test_intrinsic_partial_survives_unrelated_discriminator():
    """When the discriminator is unrelated to the intrinsic concept (neither
    equal nor a property of it), the intrinsic partial survives — the union
    covers the discriminator's universe, but that doesn't repair the
    intrinsic concept's missing values."""
    src = """
key sales_channel enum<string>['WEB', 'STORE'];
key order_id int;
key item_id int;

partial datasource web_returns (
    raw(''' 'WEB' '''): sales_channel,
    order_id: ~order_id,
    item_id: ~item_id,
) grain(sales_channel, order_id, item_id)
complete where sales_channel = 'WEB'
address web_returns_table;

partial datasource store_returns (
    raw(''' 'STORE' '''): sales_channel,
    order_id: ~order_id,
    item_id: ~item_id,
) grain(sales_channel, order_id, item_id)
complete where sales_channel = 'STORE'
address store_returns_table;
"""
    env, _ = parse(src)
    assert env.datasources["web_returns"].column_level_partial_addresses == {
        "local.order_id",
        "local.item_id",
    }
    assert env.datasources["store_returns"].column_level_partial_addresses == {
        "local.order_id",
        "local.item_id",
    }
    # The union's discriminator is sales_channel — neither equal to order_id /
    # item_id, nor a property of them. The intrinsic partials survive at the
    # union level (verified end-to-end by tpc_ds q78 / q80 which depend on
    # the resulting LEFT_OUTER sales→returns join).


def test_complete_where_without_partial_marker_rejected():
    """A `complete where` source with no `~` column and no `partial` keyword is
    contradictory — it would look fully complete, so an unfiltered query would
    silently read only its slice. The parser rejects it at declaration."""
    src = """
key customer_id int;
property customer_id.region string;
property customer_id.revenue float;

datasource east (customer_id: customer_id, region: region, revenue: revenue)
grain (customer_id)
complete where region = 'east'
query '''select 101 as customer_id, 'east' as region, 10.0 as revenue''';
"""
    with pytest.raises(Exception, match="complete where"):
        parse(src)


def test_complete_where_accepted_with_partial_keyword():
    """The `partial datasource` keyword satisfies the partiality requirement."""
    src = """
key customer_id int;
property customer_id.region string;
property customer_id.revenue float;

partial datasource east (customer_id: customer_id, region: region, revenue: revenue)
grain (customer_id)
complete where region = 'east'
query '''select 101 as customer_id, 'east' as region, 10.0 as revenue''';
"""
    env, _ = parse(src)
    assert env.datasources["east"].is_partial


def test_complete_where_accepted_with_tilde_column():
    """A single `~col` partial column satisfies the partiality requirement."""
    src = """
key customer_id int;
property customer_id.region string;
property customer_id.revenue float;

datasource east (customer_id: ~customer_id, region: region, revenue: revenue)
grain (customer_id)
complete where region = 'east'
query '''select 101 as customer_id, 'east' as region, 10.0 as revenue''';
"""
    env, _ = parse(src)
    ds = env.datasources["east"]
    assert not ds.is_partial
    assert ds.column_level_partial_addresses == {"local.customer_id"}


def test_column_level_partial_concepts_property():
    """The ``column_level_partial_concepts`` property resolves intrinsic-partial
    addresses to the concept objects on both Datasource and BuildDatasource.
    Empty when nothing is column-tagged; non-empty when ``~col`` is used."""
    src = """
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
    env, _ = parse(src)
    ds = env.datasources["orders"]
    intrinsic = ds.column_level_partial_concepts
    assert {c.address for c in intrinsic} == {"local.customer_id"}

    # BuildDatasource should expose the same property after materialization,
    # carrying the address set through the Factory build path.
    build_env = env.materialize_for_select()
    build_ds = build_env.datasources["orders"]
    intrinsic_build = build_ds.column_level_partial_concepts
    assert {c.address for c in intrinsic_build} == {"local.customer_id"}

    # No tildes anywhere → both lists empty (early-return path).
    src_no_intrinsic = """
key order_id int;
property order_id.price float;
datasource orders (
    order_id: order_id,
    price: price,
) grain (order_id) address my_table;
"""
    env2, _ = parse(src_no_intrinsic)
    ds2 = env2.datasources["orders"]
    assert ds2.column_level_partial_concepts == []
    build_ds2 = env2.materialize_for_select().datasources["orders"]
    assert build_ds2.column_level_partial_concepts == []
