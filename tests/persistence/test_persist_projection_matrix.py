"""A persist writes its target positionally: the select's Nth column lands in the
datasource's Nth declared column. A projection short one column is therefore not
a missing value, it is a SHIFT of every column after it — and once the shifted
types happen to line up, nothing downstream can tell.

This matrix pins the invariant across the persist shapes that differ in how the
planner assembles the final projection, by writing each target on DuckDB and
reading it back against the same concepts selected directly. A dropped or
reordered column shows up as a row mismatch, and a short projection shows up as
the planner's arity guard.

`sibling_aggregates` is the 2026-08 regression the matrix was written for:
`total_revenue` (a sum over a cross-datasource product) vanished from the
projection while `total_quantity` (a sum over a `group ... by` measure) survived,
so the eight-column target got a seven-column select.
"""

from dataclasses import dataclass
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import _validate_persist_projection, process_query
from trilogy.core.statements.author import PersistStatement
from trilogy.executor import Executor
from trilogy.parser import parse

MODEL = """
key order_id int;
property order_id.item_count int;
property order_id.order_status string;

datasource orders (
    order_id: order_id,
    num_of_item: item_count,
    status: order_status,
)
grain (order_id)
query '''
select 1 as order_id, 2 as num_of_item, 'Complete' as status union all
select 2, 3, 'Shipped' union all
select 3, 1, 'Complete'
''';

key product_id int;
property product_id.product_cost float;

datasource products (
    id: product_id,
    cost: product_cost,
)
grain (product_id)
query '''
select 10 as id, 1.5 as cost union all
select 11, 2.5
''';

key item_id int;
property item_id.created_at timestamp;
property item_id.sale_price float;
property item_id.list_price float?;
property item_id.item_status string;

auto revenue <- item_count * sale_price;
auto total_revenue <- sum(revenue);
auto item_quantity <- group(item_count) by item_id;
auto total_quantity <- sum(item_quantity);
auto total_cost <- sum(product_cost * item_count);
auto max_price <- max(sale_price);

# A window over a DERIVED input, plus a value reading both the window's output
# and that same input. `charged_price` exists only above the scan, so nothing
# downstream of the window can re-derive it.
auto charged_price <- coalesce(list_price, sale_price);
auto prior_charged_price <- lag(charged_price, 1) over (partition by order_id order by item_id asc);
auto price_gap <- charged_price - prior_charged_price;
auto order_avg_gap <- avg(price_gap) by order_id;

datasource order_items (
    id: item_id,
    order_id: order_id,
    product_id: ~product_id,
    created_at: created_at,
    sale_price: sale_price,
    list_price: ?list_price,
    status: item_status,
)
grain (item_id)
query '''
select 1 as id, 1 as order_id, 10 as product_id, timestamp '2024-01-01' as created_at, 10.0 as sale_price, 12.0 as list_price, 'Complete' as status union all
select 2, 1, 11, timestamp '2024-01-01', 5.0, null, 'Complete' union all
select 3, 2, 10, timestamp '2024-01-02', 7.0, 9.0, 'Shipped' union all
select 4, 3, 11, timestamp '2024-01-03', 3.0, null, 'Complete'
''';
"""


@dataclass(frozen=True)
class Case:
    name: str
    columns: tuple[tuple[str, str], ...]
    grain: tuple[str, ...]
    mode: str = "OVERWRITE"
    clauses: str = ""

    @property
    def concepts(self) -> list[str]:
        return [concept for _, concept in self.columns]

    def datasource(self) -> str:
        body = ",\n    ".join(f"{alias}: {concept}" for alias, concept in self.columns)
        return (
            f"datasource {self.name} (\n    {body},\n)\n"
            f"grain ({', '.join(self.grain)})\n"
            f"address tbl_{self.name}\n{self.clauses};"
        )


CASES = [
    # The reported shape: one aggregate over a cross-datasource product, one over
    # a value grouped to the fact key, at a grain neither of them is keyed by.
    Case(
        name="sibling_aggregates",
        columns=(
            ("order_id", "order_id"),
            ("created_date", "created_at.date"),
            ("total_revenue", "total_revenue"),
            ("total_quantity", "total_quantity"),
        ),
        grain=("order_id", "created_at.date"),
    ),
    # Same, widened with the dimensions and the partial key binding the reported
    # model carries — the missing column there was the seventh of eight.
    Case(
        name="sibling_aggregates_wide",
        columns=(
            ("order_id", "order_id"),
            ("created_date", "created_at.date"),
            ("product_id", "product_id"),
            ("item_status", "item_status"),
            ("order_status", "order_status"),
            ("total_revenue", "total_revenue"),
            ("total_quantity", "total_quantity"),
        ),
        grain=("order_id", "created_at.date", "product_id", "item_status"),
    ),
    # Two aggregates that each read a product across two datasources.
    Case(
        name="two_derived_aggregates",
        columns=(
            ("order_id", "order_id"),
            ("total_revenue", "total_revenue"),
            ("total_cost", "total_cost"),
        ),
        grain=("order_id",),
    ),
    # A derived-input aggregate beside one that reads a raw column.
    Case(
        name="derived_and_raw_aggregates",
        columns=(
            ("order_id", "order_id"),
            ("total_revenue", "total_revenue"),
            ("max_price", "max_price"),
        ),
        grain=("order_id",),
    ),
    # Controls: each measure alone must render the same values it renders beside
    # its sibling, or the sibling case is testing the wrong thing.
    Case(
        name="derived_only",
        columns=(
            ("order_id", "order_id"),
            ("created_date", "created_at.date"),
            ("total_revenue", "total_revenue"),
        ),
        grain=("order_id", "created_at.date"),
    ),
    Case(
        name="grouped_only",
        columns=(
            ("order_id", "order_id"),
            ("created_date", "created_at.date"),
            ("total_quantity", "total_quantity"),
        ),
        grain=("order_id", "created_at.date"),
    ),
    # A window's own input read beside its output, and an aggregate over the
    # result. The window sibling consumes `charged_price` without emitting it,
    # so the projection contract used to strip it off the dimension parent as
    # already-supplied and the whole `price_gap` branch left the plan.
    Case(
        name="window_over_derived_input",
        columns=(
            ("item_id", "item_id"),
            ("order_id", "order_id"),
            ("item_status", "item_status"),
            ("price_gap", "price_gap"),
            ("order_avg_gap", "order_avg_gap"),
        ),
        grain=("item_id",),
    ),
    # The reported model is a partitioned incremental append, not a plain
    # overwrite; the projection is assembled after the partition condition is
    # folded into the select.
    Case(
        name="partitioned_append",
        columns=(
            ("order_id", "order_id"),
            ("created_date", "created_at.date"),
            ("total_revenue", "total_revenue"),
            ("total_quantity", "total_quantity"),
        ),
        grain=("order_id", "created_at.date"),
        mode="APPEND",
        clauses="incremental by created_at.date\npartition by created_at.date",
    ),
]


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "model.preql").write_text(MODEL, encoding="utf-8")
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


def _sorted_rows(rows) -> list[tuple]:
    return sorted((tuple(row) for row in rows), key=repr)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_persist_writes_every_declared_column(models: Path, case: Case):
    engine = _executor(models)
    engine.execute_text(
        f"import model;\n{case.datasource()}\n"
        f"create if not exists datasources {case.name};\n"
        f"{case.mode} {case.name};"
    )
    written = _sorted_rows(
        engine.execute_raw_sql(f"select * from tbl_{case.name}").fetchall()
    )
    # A second executor, so the target datasource is not in the environment the
    # expectation is planned against — otherwise the comparison could read the
    # table it is supposed to be checking.
    reference = _executor(models)
    expected = _sorted_rows(
        reference.execute_text(f"import model;\nselect {', '.join(case.concepts)};")[
            -1
        ].fetchall()
    )
    assert written, "matrix case wrote no rows, so it proves nothing"
    assert written == expected


def test_short_projection_is_rejected_naming_the_column(models: Path):
    """The guard itself: a plan that cannot render a declared column must fail
    before any SQL is sent, naming the column rather than leaving the warehouse
    to report an arity mismatch against a staging table the user never saw."""
    case = CASES[0]
    environment = Environment(working_path=models)
    _, statements = parse(
        f"import model;\n{case.datasource()}\nOVERWRITE {case.name};", environment
    )
    statement = statements[-1]
    assert isinstance(statement, PersistStatement)
    select = process_query(environment, statement.select)
    select.base.output_columns = [
        c for c in select.base.output_columns if not c.address.endswith("total_revenue")
    ]
    with pytest.raises(UnresolvableQueryException) as excinfo:
        _validate_persist_projection(statement, select)
    assert "total_revenue" in str(excinfo.value)
    assert "3 of 4" in str(excinfo.value)
