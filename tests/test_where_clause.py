# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.parser import parse


def test_select_where(test_environment):
    declarations = """

select
    total_revenue
where
    order_id in (1,2,3)
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["order_id"]])
