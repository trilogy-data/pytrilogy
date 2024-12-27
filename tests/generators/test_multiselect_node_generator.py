from trilogy import parse
from trilogy.core.env_processor import generate_graph
from trilogy.core.models import BoundEnvironment
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.node_generators import gen_multiselect_node
from trilogy.core.query_processor import datasource_to_cte


def test_multi_select(test_environment: BoundEnvironment, test_environment_graph):
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
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
        test_environment,
    )

    # c = test_environment.concepts['one']
    # assert c.with_default_grain().grain.components == [c,]
    gnode = gen_multiselect_node(
        concept=test_environment.concepts["one_key"],
        local_optional=[],
        environment=test_environment,
        g=generate_graph(test_environment),
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(gnode.parents) == 2
    assert len(gnode.node_joins) == 1
    # ensure that we got sources from both parents for our merge key

    resolved = gnode.resolve()
    assert len(resolved.source_map["local.one_key"]) == 0

    cte = datasource_to_cte(resolved, {})
    assert len(cte.source_map["local.one_key"]) == 0


def test_multi_select_constant(test_environment: BoundEnvironment, test_environment_graph):
    # from trilogy.core.models import AggregateWrapper

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
        test_environment,
    )
    gnode = gen_multiselect_node(
        concept=test_environment.concepts["true_one"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(gnode.parents) == 2
    assert len(gnode.node_joins) == 0
