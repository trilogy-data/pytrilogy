from datetime import datetime

from trilogy import Dialects
from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import Granularity, Purpose
from trilogy.core.models.author import Grain
from trilogy.hooks import DebuggingHook

SETUP = """
key order_id int;
property order_id.amount float;
property <*>.last_updated datetime;

datasource orders (
    order_id: order_id,
    amount: amount,
)
grain (order_id)
query '''
select 1 as order_id, 100.0 as amount
union all
select 2, 200.0
''';

datasource metadata (
    last_updated
)
query '''
select DATETIME '2024-01-15 00:00:00' as last_updated
''';
"""


def test_abstract_property_parsing():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(SETUP)

    env = exec.environment
    concept = env.concepts["last_updated"]
    assert concept.purpose == Purpose.PROPERTY
    assert concept.granularity == Granularity.SINGLE_ROW
    assert concept.grain == Grain(
        components={f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"}
    )


def test_basic_over_grainless_aggregates_is_single_row():
    """A BASIC concept that inlines grand-total aggregates (`sum(x)/greatest(sum(1),1)`)
    has an abstract (grand-total) grain and must be Granularity.SINGLE_ROW. Its
    `concept_arguments` flatten through the aggregates to the raw leaf columns
    (MULTI_ROW), so the "all args single-row" check fails; single-row-ness must be
    read off the abstract grain instead. If mis-tagged MULTI_ROW it is treated as a
    finer dimension in the HAVING promotion gate and silently misrouted."""
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text("""
key order_id int;
property order_id.amount float;
datasource orders (order_id: order_id, amount: amount)
grain (order_id)
query '''select 1 as order_id, 100.0 as amount union all select 2, 200.0''';

auto gavg <- sum(amount) / greatest(sum(1), 1);
auto gsum2 <- sum(amount) + sum(amount);
""")
    for name in ("gavg", "gsum2"):
        concept = exec.environment.concepts[name]
        assert concept.grain.abstract, (name, concept.grain.components)
        assert concept.granularity == Granularity.SINGLE_ROW, (
            name,
            concept.granularity,
        )


def test_abstract_property_sql_generation():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(SETUP)
    DebuggingHook()
    built = exec.environment.materialize_for_select()
    assert "local.last_updated" in built.materialized_concepts
    materialized = built.concepts["local.last_updated"]
    assert materialized.keys == {
        f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}",
    }, materialized.keys
    assert materialized.grain.abstract, materialized
    assert materialized.granularity == Granularity.SINGLE_ROW, materialized

    generated = exec.generate_sql("""
SELECT
    order_id,
    last_updated
ORDER BY order_id
;
""")[-1]
    assert "orders" in generated, generated
    assert "metadata" in generated, generated


def test_abstract_property_execution():
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(SETUP)

    results = exec.execute_text("""
SELECT
    order_id,
    last_updated
ORDER BY order_id
;
""")[0].fetchall()

    assert len(results) == 2
    assert results[0][0] == 1
    assert results[1][0] == 2
    # both rows share the single last_updated value
    assert results[0][1] == results[1][1]
    assert isinstance(results[0][1], datetime)
