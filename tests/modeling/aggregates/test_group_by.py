from trilogy import Executor, parse
from trilogy.core.enums import Derivation, FunctionType
from trilogy.core.models.author import Function, Grain
from trilogy.core.models.environment import Environment


def test_group_by(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    sum(qty) by stores.name-> total_qty
ORDER BY 
    total_qty desc
;"""

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert results[0] == (4,)

    test_select2 = """
auto total_qty <- sum(qty);

SELECT
    group total_qty by stores.name-> total_qty_stores,
    stores.name
ORDER BY 
    total_qty_stores desc
;"""

    _, statements = parse(test_select2, test_environment)
    tqs = test_environment.concepts["total_qty_stores"]
    assert isinstance(tqs.lineage, Function)
    assert tqs.lineage.operator == FunctionType.GROUP
    assert tqs.derivation == Derivation.BASIC
    assert tqs.keys == {"stores.name"}
    assert tqs.grain == Grain(components={"stores.name"})

    results = test_executor.execute_text(test_select2)[0].fetchall()

    assert results[0] == (4, "store1")
