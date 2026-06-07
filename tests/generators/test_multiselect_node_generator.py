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
