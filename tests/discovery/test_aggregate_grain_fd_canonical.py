"""Lock: an abstract aggregate's identity does not depend on how its grain is spelled.

An abstract aggregate is pinned at the select grain and that grain is hashed
into its canonical name, so two spellings of one grouping were two concepts:
`select order_id, region, total` missed the `order_id`-grain summary that
`select order_id, customer_id, region, total` read. The canonical name now
hashes the FD-minimal `by` (the whole key chain `order_id -> customer_id ->
region`, and a global non-partial merge's identity). Only the name: the
lineage keeps its full `by`, so a query no summary answers plans as before.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import get_query_node

_MODEL = """
key order_id int;
key customer_id int;
property customer_id.region string;
property order_id.amount float;
auto total <- sum(amount);

datasource orders (order_id, {customer_binding}, amount)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 6.0 as amount
union all select 2, 101, 4.0
union all select 3, 102, 4.0
''';

datasource customers (customer_id, region)
grain (customer_id)
query '''
select 101 as customer_id, 'east' as region
union all select 102, 'west'
union all select 103, 'north'
''';
"""

# Sentinel totals: only a read of this table returns them.
_SUMMARY = """
datasource order_summary (order_id, total)
grain (order_id)
query '''
select 1 as order_id, 96.0 as total
union all select 2, 94.0
union all select 3, 94.5
''';
"""


def _engine(customer_binding: str = "customer_id", summary: bool = True):
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    text = _MODEL.format(customer_binding=customer_binding)
    engine.parse_text(text + (_SUMMARY if summary else ""))
    return engine


def _rows(engine, query: str) -> list[tuple]:
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


def _identity(engine, dims: str, aggregate: str) -> str:
    env = engine.environment
    select = env.parse(f"select {dims}, {aggregate} as agg;")[1][-1]
    built: list = []
    get_query_node(env, select.as_lineage(env), build_lineage_sink=built)
    return next(
        c.canonical_address for c in built[-1].output_components if c.name == "agg"
    )


@pytest.mark.parametrize(
    "dims",
    [
        "order_id, customer_id",
        "order_id, region",
        "order_id, customer_id, region",
        "region, order_id",
        "order_id, upper(region) as shout",
    ],
)
def test_key_chain_folds(dims: str):
    engine = _engine()
    assert _identity(engine, dims, "sum(amount)") == _identity(
        engine, "order_id", "sum(amount)"
    )


def test_undetermined_property_stays():
    engine = _engine()
    assert _identity(engine, "customer_id, amount", "sum(amount)") != _identity(
        engine, "customer_id", "sum(amount)"
    )


@pytest.mark.parametrize(
    "select",
    [
        "order_id, total",
        "order_id, customer_id, total",
        "order_id, region, total",
        "order_id, customer_id, region, total",
    ],
)
def test_summary_read_at_every_spelling(select: str):
    rows = _rows(_engine(), f"select {select} order by order_id asc;")
    assert [r[-1] for r in rows] == [96.0, 94.0, 94.5]


# A `~` binding proves the FD only where the key is present. Customer 103 has
# no order: its null-extended row has one region per (NULL) order, so dropping
# `region` from the aggregate's identity leaves the rows alone.
@pytest.mark.parametrize(
    "customer_binding,extension",
    [("customer_id", []), ("~customer_id", [(None, "north", None)])],
)
def test_partial_determined_key_rows(customer_binding: str, extension: list[tuple]):
    engine = _engine(customer_binding=customer_binding, summary=False)
    assert _rows(engine, "select order_id, region, total order by order_id asc;") == [
        (1, "east", 6.0),
        (2, "east", 4.0),
        (3, "west", 4.0),
        *extension,
    ]


_MERGE = """
key org_code string;
property org_code.state_code string;
key state string;
property state.state_name string;
key launch_id string;

merge state_code into {target};

datasource orgs (org_code, state_code)
grain (org_code)
query '''
select 'NASA' as org_code, 'US' as state_code
union all select 'CASC', 'CN'
''';

datasource states (state, state_name)
grain (state)
query '''
select 'US' as state, 'United States' as state_name
union all select 'CN', 'China'
union all select 'FR', 'France'
''';

datasource launches (launch_id, org_code)
grain (launch_id)
query '''
select 'L1' as launch_id, 'NASA' as org_code
union all select 'L2', 'CASC'
union all select 'L3', 'NASA'
''';
"""


def _merge_engine(target: str):
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MERGE.format(target=target))
    return engine


@pytest.mark.parametrize("dim", ["state", "state_name"])
def test_equal_merge_target_folds(dim: str):
    engine = _merge_engine("state")
    assert _identity(engine, f"org_code, {dim}", "count(launch_id)") == _identity(
        engine, "org_code", "count(launch_id)"
    )


# A partial merge holds only on matched rows: it is not an identity.
@pytest.mark.parametrize("dim", ["state", "state_name"])
def test_partial_merge_target_stays(dim: str):
    engine = _merge_engine("~state")
    assert _identity(engine, f"org_code, {dim}", "count(launch_id)") != _identity(
        engine, "org_code", "count(launch_id)"
    )


def test_two_spellings_agree():
    engine = _merge_engine("state")
    query = (
        "select org_code, state, count(launch_id) as launches,"
        " count(launch_id) by org_code as explicit order by org_code asc;"
    )
    assert _rows(engine, query) == [("CASC", "CN", 1, 1), ("NASA", "US", 2, 2)]


# Two names for one materialized expression share a canonical, and the graph
# keys a node by canonical: the summary scan has to emit both names.
@pytest.mark.parametrize(
    "dims", ["order_id", "order_id, customer_id", "order_id, region"]
)
def test_two_names_for_one_materialized_aggregate(dims: str):
    rows = _rows(
        _engine(),
        f"select {dims}, total, sum(amount) by order_id as explicit"
        " order by order_id asc;",
    )
    assert [r[-2:] for r in rows] == [(96.0, 96.0), (94.0, 94.0), (94.5, 94.5)]
