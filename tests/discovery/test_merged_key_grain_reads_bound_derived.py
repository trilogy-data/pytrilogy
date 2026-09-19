"""Lock: a bound derived column stays readable wherever its lineage is not.

`organization.preql` binds `short_e_name` -- a coalesce over `_short_e_name`,
which no table binds -- directly as a column of its raw table. The column is
the concept's only source: decomposing it walks into `_short_e_name` and
dead-ends ("No datasource exists for root concept"). Pre-v4 planners read a
bound column first and these shapes all resolved; v4 derives first and only
short-circuits a demanded output at exactly the datasource grain, so every
other position (a finer or coarser grain, an aggregate or scalar input, a
condition) regressed.

`launch.preql` is how space_reporting hit it: it imports that file twice and
merges `org.state_code`, a property of `org.code`, into `state.code`, a key.
The merged property is now spelled by a KEY, so a request naming it beside
`org.code` sits at `Grain<org.code, state.code>` while the organization table
declares `Grain<org.code>` -- one more way to miss the exact grain. That is
the shape the `launch.org.organizations` refresh failed in from 2026-09-14.

Two rules cover it. A bound derived column whose lineage cannot be sourced is
a root at every grain -- scalars unconditionally, stored aggregates and windows
under conditions their table can express. And the exact-grain gate that remains for re-derivable
columns and summary aggregates compares grains modulo functional dependency:
`org.code` determines `state.code`, so a `Grain<org.code>` summary still has
one row per (org.code, state.code).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import NoDatasourceException
from trilogy.core.models.environment import Environment

_ORGANIZATION = """
key code string;
property code.state_code string;
property code.short_name string;
property code._short_e_name string;
auto short_e_name <- coalesce(
    CASE WHEN _short_e_name = '-' THEN NULL ELSE _short_e_name END,
    short_name
);

datasource organizations_raw (
    Code: code,
    StateCode: state_code,
    ShortName: short_name,
    ShortEName: short_e_name,
)
grain (code)
query '''
select 'NASA' as Code, 'US' as StateCode, 'NASA Hq' as ShortName, 'NASA' as ShortEName
union all
select 'CASC' as Code, 'CN' as StateCode, 'CASC-CN' as ShortName, 'CASC' as ShortEName
''';
"""

_LAUNCH = """
import organization as org;
import organization as state;

merge org.state_code into state.code;

key id string;
property id.agency string;

datasource launches (
    Launch_Tag: id,
    Agency: org.code,
)
grain (id)
query '''
select 'L1' as Launch_Tag, 'NASA' as Agency
union all select 'L2' as Launch_Tag, 'CASC' as Agency
union all select 'L3' as Launch_Tag, 'NASA' as Agency
''';

auto launch_count <- count(id);

# Sentinel counts: only a read of this table returns them.
datasource org_summary (
    Code: org.code,
    N: launch_count,
)
grain (org.code)
query '''
select 'NASA' as Code, 41 as N
union all select 'CASC' as Code, 42 as N
''';
"""


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "organization.preql").write_text(_ORGANIZATION)
    (tmp_path / "launch.preql").write_text(_LAUNCH)
    return tmp_path


def _engine(model_dir: Path, text: str):
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    engine.parse_text(text)
    return engine


def _rows(model_dir: Path, query: str) -> list[tuple]:
    engine = _engine(model_dir, "import launch;")
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


# `org` is the demoted side of the merge (its property now reads as the key
# `state.code`); `state` is the surviving side and the control.
@pytest.mark.parametrize("side", ["org", "state"])
def test_bound_derived_reads_beside_merged_key(model_dir: Path, side: str):
    query = (
        f"select {side}.code, {side}.state_code, {side}.short_e_name"
        f" order by {side}.code asc;"
    )
    sql = _engine(model_dir, "import launch;").generate_sql(query)[-1]
    assert '"ShortEName"' in sql, sql
    assert _rows(model_dir, query) == [("CASC", "CN", "CASC"), ("NASA", "US", "NASA")]


@pytest.mark.parametrize(
    "query,expected",
    [
        pytest.param(
            "select org.short_e_name order by org.short_e_name asc;",
            [("CASC",), ("NASA",)],
            id="alone",
        ),
        pytest.param(
            "select id, org.short_e_name order by id asc;",
            [("L1", "NASA"), ("L2", "CASC"), ("L3", "NASA")],
            id="finer_grain",
        ),
        pytest.param(
            "select id, org.code, org.state_code, org.short_e_name order by id asc;",
            [
                ("L1", "NASA", "US", "NASA"),
                ("L2", "CASC", "CN", "CASC"),
                ("L3", "NASA", "US", "NASA"),
            ],
            id="finer_grain_beside_merged_key",
        ),
        pytest.param(
            "select org.state_code, org.short_e_name order by org.state_code asc;",
            [("CN", "CASC"), ("US", "NASA")],
            id="merged_key_only",
        ),
        pytest.param(
            "select org.short_e_name, count(id) as launches"
            " order by org.short_e_name asc;",
            [("CASC", 1), ("NASA", 2)],
            id="group_key",
        ),
        pytest.param(
            "select org.state_code, count(org.short_e_name) as names"
            " order by org.state_code asc;",
            [("CN", 1), ("US", 1)],
            id="aggregate_input",
        ),
        pytest.param(
            "select org.code, org.state_code, upper(org.short_e_name) as shout"
            " order by org.code asc;",
            [("CASC", "CN", "CASC"), ("NASA", "US", "NASA")],
            id="scalar_input",
        ),
        pytest.param(
            "where org.short_e_name = 'NASA' select id order by id asc;",
            [("L1",), ("L3",)],
            id="condition_at_finer_grain",
        ),
        pytest.param(
            "where org.short_e_name = 'NASA' select org.code, org.state_code;",
            [("NASA", "US")],
            id="condition_beside_merged_key",
        ),
    ],
)
def test_unsourced_lineage_reads_the_binding(
    model_dir: Path, query: str, expected: list[tuple]
):
    assert _rows(model_dir, query) == expected


# The summary neither binds nor declares `state.code`; `org.code` determines it.
@pytest.mark.parametrize(
    "query,expected",
    [
        pytest.param(
            "select org.code, launch_count order by org.code asc;",
            [("CASC", 42), ("NASA", 41)],
            id="exact_grain",
        ),
        pytest.param(
            "select org.code, org.state_code, launch_count order by org.code asc;",
            [("CASC", "CN", 42), ("NASA", "US", 41)],
            id="beside_merged_key",
        ),
        pytest.param(
            "select org.code, state.code, launch_count order by org.code asc;",
            [("CASC", "CN", 42), ("NASA", "US", 41)],
            id="beside_surviving_key_spelling",
        ),
    ],
)
def test_summary_reads_at_fd_equivalent_grain(
    model_dir: Path, query: str, expected: list[tuple]
):
    assert _rows(model_dir, query) == expected


# The failure surfaced through `refresh`, whose rebuild query names every
# column of the managed table -- key, merged property and bound derived alike.
# `launch` sorts before `local`, the namespace space_reporting reaches
# launch.preql under; `zorg` after, the control.
@pytest.mark.parametrize("alias", ["launch", "zorg"])
def test_managed_table_rebuild_reads_bound_derived(model_dir: Path, alias: str):
    (model_dir / "organization.preql").write_text(_ORGANIZATION + """
datasource organizations (
    code,
    state_code,
    short_name,
    short_e_name,
)
grain (code)
address organizations;
""")
    engine = _engine(model_dir, f"import launch as {alias};")
    for side in ("org", "state"):
        target = engine.environment.datasources[f"{alias}.{side}.organizations"]
        sql = engine.update_datasource(target, dry_run=True)
        assert sql is not None and '"ShortEName"' in sql, sql


# Stored cross-row values over `_hidden`, which no table binds: an aggregate
# with its own grain, a window over it, and a scalar of it.
_CUSTOMER_TOTALS = """
key order_id int;
key customer_id int;
property customer_id.region string;
property order_id._hidden float;
auto cust_total <- sum(_hidden) by customer_id;
auto cust_rank <- rank customer_id by cust_total desc;
auto is_big <- cust_total > 5;

datasource orders (order_id, customer_id)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id
union all select 2, 101
union all select 3, 102
''';

datasource customers (
    customer_id,
    region,
    cust_total,
    cust_rank,
    is_big,
)
grain (customer_id)
query '''
select 101 as customer_id, 'east' as region, 10.0 as cust_total, 1 as cust_rank, true as is_big
union all select 102, 'west', 4.0, 2, false
''';
"""


def _total_rows(query: str) -> list[tuple]:
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_CUSTOMER_TOTALS)
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize(
    "query,expected",
    [
        pytest.param(
            "select order_id, cust_total order by order_id asc;",
            [(1, 10.0), (2, 10.0), (3, 4.0)],
            id="aggregate_finer_grain",
        ),
        pytest.param(
            "select region, sum(cust_total) as total order by region asc;",
            [("east", 10.0), ("west", 4.0)],
            id="aggregate_input",
        ),
        pytest.param(
            "select customer_id, cust_total * 2 as doubled order by customer_id asc;",
            [(101, 20.0), (102, 8.0)],
            id="scalar_input",
        ),
        pytest.param(
            "select customer_id, cust_rank order by customer_id asc;",
            [(101, 1), (102, 2)],
            id="window_exact_grain",
        ),
        pytest.param(
            "select order_id, cust_rank order by order_id asc;",
            [(1, 1), (2, 1), (3, 2)],
            id="window_finer_grain",
        ),
        pytest.param(
            "select order_id, is_big order by order_id asc;",
            [(1, True), (2, True), (3, False)],
            id="scalar_of_aggregate",
        ),
        pytest.param(
            "where is_big select order_id order by order_id asc;",
            [(1,), (2,)],
            id="condition",
        ),
        pytest.param(
            "where region = 'east' select order_id, cust_total order by order_id asc;",
            [(1, 10.0), (2, 10.0)],
            id="group_level_condition",
        ),
    ],
)
def test_stored_cross_row_value_reads_the_binding(query: str, expected: list[tuple]):
    assert _total_rows(query) == expected


# A row filter below the stored grain changes what the aggregate would be; the
# column cannot answer, and saying so beats returning the unfiltered value.
@pytest.mark.parametrize("value", ["cust_total", "cust_rank", "is_big"])
def test_stored_cross_row_value_refuses_finer_condition(value: str):
    with pytest.raises(NoDatasourceException):
        _total_rows(f"where order_id = 1 select customer_id, {value};")


# A customer total denormalized onto an order-grain table sits at the target
# grain, and the table can express `order_date`, but the filter splits the
# groups the column summed. The stored 10.0 is not the filtered 6.0.
_DENORMALIZED_TOTAL = """
key order_id int;
key customer_id int;
key order_date int;
property order_id.amount float;
auto cust_total <- sum(amount) by customer_id;

datasource orders (order_id, customer_id, order_date, amount)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 1 as order_date, 6.0 as amount
union all select 2, 101, 2, 4.0
union all select 3, 102, 1, 4.0
''';

datasource order_totals (order_id, order_date, cust_total)
grain (order_id)
query '''
select 1 as order_id, 1 as order_date, 10.0 as cust_total
union all select 2, 2, 10.0
union all select 3, 1, 4.0
''';
"""


@pytest.mark.parametrize(
    "query,expected",
    [
        pytest.param(
            "select order_id, cust_total order by order_id asc;",
            [(1, 10.0), (2, 10.0), (3, 4.0)],
            id="unfiltered",
        ),
        pytest.param(
            "where order_date = 1 select order_id, cust_total order by order_id asc;",
            [(1, 6.0), (3, 4.0)],
            id="finer_condition_recomputes",
        ),
    ],
)
def test_stored_aggregate_below_its_grain(query: str, expected: list[tuple]):
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_DENORMALIZED_TOTAL)
    rows = [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]
    assert rows == expected
