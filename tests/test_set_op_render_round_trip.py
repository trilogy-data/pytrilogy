import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.render import Renderer

MODEL = """
key line_id int;
property line_id.item_id int;
property line_id.yr int;
property line_id.val int;
datasource lines (line_id: line_id, item_id: item_id, yr: yr, val: val)
grain (line_id)
query '''select * from (values (1,1,2001,10),(2,1,2001,20),(3,2,2001,5),(4,1,2002,10),(5,3,2002,7)) as t(line_id,item_id,yr,val)''';
"""

SET_OPS = {
    "union": """with c as union(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
select c.k, sum(c.k) as s order by c.k asc;""",
    "union_two_columns": """rowset c <- union(
    (where yr = 2001 select item_id -> k, sum(val) -> v),
    (where yr = 2002 select item_id -> k, sum(val) -> v)
) -> (k, v metric int);
select c.k, sum(c.v) as total order by c.k asc;""",
    "except": """with c as except(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
select c.k order by c.k asc;""",
    "intersect": """with c as intersect(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
select c.k order by c.k asc;""",
    "union_over_union": """with a as union(
    (where yr = 2001 select item_id -> k),
    (where yr = 2002 select item_id -> k)
) -> (k);
with b as union(
    (select a.k -> k),
    (where yr = 2002 select line_id -> k)
) -> (k);
select b.k, sum(b.k) as s order by b.k asc;""",
}


def _render(text: str) -> str:
    env = Environment()
    _, statements = env.parse(text)
    return Renderer(environment=env).render_statement_string(statements)


def _rows(text: str):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    return executor.execute_text(text)[-1].fetchall()


@pytest.mark.parametrize("case", sorted(SET_OPS))
def test_set_op_renders_as_its_call_and_round_trips(case):
    authored = MODEL + SET_OPS[case]
    rendered = _render(authored)
    assert "merge" not in rendered and "__arm" not in rendered, rendered
    assert _render(rendered) == rendered
    assert _rows(rendered) == _rows(authored)


def test_arm_key_output_name_is_reserved():
    with pytest.raises(InvalidSyntaxException, match="reserved"):
        Environment().parse(
            MODEL
            + "with c as union((select item_id -> __arm), (select line_id -> __arm))"
            " -> (__arm);"
        )
