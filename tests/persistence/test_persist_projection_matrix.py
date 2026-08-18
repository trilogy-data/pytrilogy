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
from trilogy.core.enums import DatasourceState
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import (
    _validate_persist_projection,
    get_query_node,
    process_query,
)
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


# A property populated ONLY via `merge` (no datasource binds it), persisted next
# to an abstract-grain watermark column from a separate root source. The
# watermark makes the FINAL assembly a multi-contributor merge, whose projection
# used to match parent outputs by address alone — the merge target is exposed
# only as its origin's pseudonym, so it silently dropped (2026-08, dbh
# imputation: 0.3.316 wrote a shifted 2-of-3 projection; 0.3.331's arity guard
# turned that into a hard error).
MERGED_PROPERTY_MODEL = """
key entity_id string;

property <*>.updated_through datetime;

property entity_id.city string;
property entity_id.raw_measure float?;
property entity_id.measure float;
property entity_id.imputed_measure float;

auto derived_measure <- raw_measure + 1.0;
merge derived_measure into measure;

auto derived_imputed <- coalesce(raw_measure, avg(raw_measure) by city);
merge derived_imputed into imputed_measure;

datasource update_time (
    updated_at: updated_through
)
query '''SELECT TIMESTAMP '2026-08-01 00:00:00' AS updated_at''';

datasource raw_rows (
    entity_id: entity_id,
    city: city,
    raw_measure: raw_measure,
)
grain (entity_id)
query '''SELECT * FROM (VALUES
    ('p1', 'boston', 10.0),
    ('p2', 'boston', null),
    ('p3', 'cambridge', 3.5)
) t(entity_id, city, raw_measure)''';
"""


@dataclass(frozen=True)
class MergedCase:
    name: str
    concepts: tuple[str, ...]

    def datasource(self) -> str:
        body = ",\n    ".join(self.concepts)
        return (
            f"datasource {self.name} (\n    {body},\n)\n"
            f"grain (entity_id)\naddress tbl_{self.name};"
        )


MERGED_CASES = [
    # The reported repro: trivial scalar derivation merged into the property.
    MergedCase(
        name="merge_scalar_beside_watermark",
        concepts=("entity_id", "measure", "updated_through"),
    ),
    # The real downstream shape (Boston dbh): a partitioned imputation
    # aggregate `coalesce(x, avg(x) by k)` merged into the property.
    MergedCase(
        name="merge_imputed_beside_watermark",
        concepts=("entity_id", "imputed_measure", "updated_through"),
    ),
    # Both merge targets beside the watermark: two pseudonym-covered outputs
    # must survive the same multi-contributor projection.
    MergedCase(
        name="merge_both_beside_watermark",
        concepts=("entity_id", "measure", "imputed_measure", "updated_through"),
    ),
    # Control: no watermark, so the plan is single-contributor and rides the
    # _bridge_pseudonyms path — pins the OTHER assembly path against drift.
    MergedCase(
        name="merge_scalar_no_watermark",
        concepts=("entity_id", "measure"),
    ),
]


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "model.preql").write_text(MODEL, encoding="utf-8")
    (tmp_path / "merged_property.preql").write_text(
        MERGED_PROPERTY_MODEL, encoding="utf-8"
    )
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


@pytest.mark.parametrize("case", MERGED_CASES, ids=lambda c: c.name)
def test_merge_populated_property_writes_every_column(models: Path, case: MergedCase):
    engine = _executor(models)
    engine.execute_text(
        f"import merged_property;\n{case.datasource()}\n"
        f"create if not exists datasources {case.name};\n"
        f"OVERWRITE {case.name};"
    )
    written = _sorted_rows(
        engine.execute_raw_sql(f"select * from tbl_{case.name}").fetchall()
    )
    reference = _executor(models)
    expected = _sorted_rows(
        reference.execute_text(
            f"import merged_property;\nselect {', '.join(case.concepts)};"
        )[-1].fetchall()
    )
    assert written, "persist wrote no rows, so it proves nothing"
    assert written == expected


def test_merged_property_rides_final_merge_as_output_not_input(models: Path):
    """The fix's mechanics, pinned at the strategy-node layer: the merge target
    is exposed only as its origin's pseudonym, so the multi-contributor FINAL
    node must carry it as an OUTPUT (resolve_concept_map's targets loop maps it
    to the producing parent via the pseudonym) and never as an INPUT (inherited
    inputs are skipped by that loop, leaving the column dangling)."""
    environment = Environment(working_path=models)
    engine = Dialects.DUCK_DB.default_executor(environment=environment)
    target = MERGED_CASES[0]
    engine.parse_text(f"import merged_property;\n{target.datasource()}")
    ds = environment.datasources[target.name]
    lineage = ds.create_update_statement(environment, None, line_no=None).as_lineage(
        environment
    )
    original_status = ds.status
    ds.status = DatasourceState.UNPUBLISHED
    try:
        node = get_query_node(environment, lineage)
    finally:
        ds.status = original_status
    output_addrs = {c.address for c in node.output_concepts}
    assert "local.measure" in output_addrs
    assert "local.measure" not in {c.address for c in node.input_concepts}
    assert any(
        "local.measure" in o.pseudonyms
        for parent in node.parents
        for o in parent.output_concepts
    ), "no parent exposes the merge origin under the target's pseudonym"


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
