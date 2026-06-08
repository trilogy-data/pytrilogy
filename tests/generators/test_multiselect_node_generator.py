from trilogy import Dialects, Environment, parse
from trilogy.core.env_processor import generate_graph
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import gen_multiselect_node
from trilogy.core.query_processor import datasource_to_cte


def test_multi_select():
    env = Environment()
    parse(
        """
key one int;
key other_one int;
     
datasource num1 (
    one:one
) 
grain (one)
address num1;
          
datasource num_other (
    other_one:other_one
)
grain (other_one)
address num_other;
          

SELECT
    one
MERGE
SELECT 
    other_one
ALIGN 
    one_key:one,other_one
;
          """,
        env,
    )

    # c = test_environment.concepts['one']
    # assert c.with_default_grain().grain.components == [c,]
    orig_env = env
    test_environment = env.materialize_for_select()
    gnode = gen_multiselect_node(
        concept=test_environment.concepts["one_key"],
        local_optional=[],
        environment=test_environment,
        g=generate_graph(test_environment),
        depth=0,
        source_concepts=search_concepts,
        history=History(base_environment=orig_env),
    )
    assert len(gnode.parents) == 2
    assert len(gnode.node_joins) == 1
    # ensure that we got sources from both parents for our merge key

    resolved = gnode.resolve()
    assert len(resolved.source_map["local.one_key"]) == 0

    cte = datasource_to_cte(resolved, {})
    assert len(cte.source_map["local.one_key"]) == 0


def test_multi_select_constant():
    env = Environment()
    parse(
        """
const one <- 1;
const other_one <- 1;


SELECT
    one
MERGE
SELECT
    other_one
ALIGN
    true_one:one,other_one
;
          """,
        env,
    )
    orig_env = env
    test_environment = env.materialize_for_select()
    gnode = gen_multiselect_node(
        concept=test_environment.concepts["true_one"],
        local_optional=[],
        environment=test_environment,
        g=generate_graph(test_environment),
        depth=0,
        source_concepts=search_concepts,
        history=History(base_environment=orig_env),
    )
    assert len(gnode.parents) == 2
    assert len(gnode.node_joins) == 0


def test_multi_select_align_hide():
    """`--alias:` on align hides the join identity from the projection but keeps
    it available as the inner-CTE join key."""
    env = Environment()
    _, statements = parse(
        """
key one int;
key other_one int;
property one.label_a string;
property other_one.label_b string;

datasource num1 (
    one:one,
    label_a:label_a
)
grain (one)
address num1;

datasource num_other (
    other_one:other_one,
    label_b:label_b
)
grain (other_one)
address num_other;

SELECT
    label_a,
    --one
MERGE
SELECT
    label_b,
    --other_one
ALIGN
    --one_key:one,other_one
;
          """,
        env,
    )
    multi = statements[-1]
    assert any(item.hidden for item in multi.align.items)
    assert "local.one_key" in multi.hidden_components

    e = Dialects.DUCK_DB.default_executor(environment=env)
    sql = e.generate_sql(statements[-1])[0]
    # final SELECT projects only the two labels, never the join identity
    select_start = sql.rfind("SELECT")
    from_start = sql.find("FROM", select_start)
    projection = sql[select_start:from_start]
    assert "label_a" in projection
    assert "label_b" in projection
    assert "one_key" not in projection
    # but the join is still wired up in an earlier clause
    assert "JOIN" in sql
    assert "one_key" in sql


_DERIVE_MODEL = """
key one int;
property one.label string;
property one.val_a int;
datasource num1 (one:one, label:label, val_a:val_a)
grain (one) address num1;

key two int;
property two.label2 string;
property two.val_b int;
datasource num2 (two:two, label2:label2, val_b:val_b)
grain (two) address num2;
"""


def _exec_multiselect(body: str) -> None:
    from trilogy import Dialects

    env = Environment()
    e = Dialects.DUCK_DB.default_executor(environment=env)
    sql = e.generate_sql(_DERIVE_MODEL + body)[-1]
    e.execute_raw_sql("CREATE TABLE num1(one int, label varchar, val_a int)")
    e.execute_raw_sql("CREATE TABLE num2(two int, label2 varchar, val_b int)")
    e.execute_raw_sql(sql).fetchall()


def test_multiselect_derive_having_order_by_derived():
    """A derived output must be referenceable from HAVING and ORDER BY, and the
    generated SQL must bind (regression: derived concepts were registered after
    those clauses hydrated, so they fell back to invalid inner-CTE columns)."""
    _exec_multiselect("""
SELECT label, sum(val_a) as a_sum,
MERGE
SELECT label2, sum(val_b) as b_sum,
ALIGN lbl: label, label2
DERIVE coalesce(a_sum, 0) as ca, coalesce(b_sum, 0) as cb
HAVING cb <= ca
ORDER BY cb desc
;
""")


def test_multiselect_derive_bare_concept_ref_actionable_error():
    """A `derive` of a bare concept reference (instead of an expression) must
    fail with guidance to carry/rename it via a SELECT arm, not the opaque
    'must be a function or conditional'."""
    import pytest

    with pytest.raises(Exception) as exc:
        parse(
            _DERIVE_MODEL + "SELECT label, sum(val_a) as a_sum, MERGE SELECT label2,"
            " sum(val_b) as b_sum, ALIGN lbl: label, label2 DERIVE a_sum as carried;",
            Environment(),
        )
    msg = str(exc.value)
    assert "carried <- local.a_sum" in msg
    assert "bare concept reference" in msg
    assert "as carried" in msg


def test_multiselect_derive_arrow_and_as_equivalent():
    """`->` and `as` are interchangeable in a derive clause."""
    from trilogy import Dialects

    arrow = "DERIVE coalesce(a_sum, 0) -> ca"
    as_form = "DERIVE coalesce(a_sum, 0) as ca"
    tmpl = (
        "SELECT label, sum(val_a) as a_sum, MERGE SELECT label2, sum(val_b) as b_sum,"
        " ALIGN lbl: label, label2 {clause} ORDER BY ca desc ;"
    )
    sqls = []
    for clause in (arrow, as_form):
        env = Environment()
        e = Dialects.DUCK_DB.default_executor(environment=env)
        sqls.append(e.generate_sql(_DERIVE_MODEL + tmpl.format(clause=clause))[-1])
    assert sqls[0] == sqls[1]
