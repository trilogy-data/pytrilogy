from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.node_generators import gen_rowset_node
from trilogy.core.processing.nodes import History
from trilogy.hooks.query_debugger import DebuggingHook


def test_gen_rowset_node_with_filter(
    test_environment: Environment, test_environment_graph
):

    test_environment.parse(
        """
                           
auto rev_sum <-sum(revenue);
                           
with p1 as 
select product_id
where rev_sum>5;
                           """
    )
    orig_env = test_environment
    test_environment = test_environment.materialize_for_select()
    node = gen_rowset_node(
        concept=test_environment.concepts["p1.product_id"],
        local_optional=[
            test_environment.concepts["local.product_id"],
            test_environment.concepts["local.rev_sum"],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=History(base_environment=orig_env),
    )

    assert "local.rev_sum" in node.output_concepts

    node.resolve()


def test_gen_rowset_node_group_parent(
    test_environment: Environment, test_environment_graph
):

    test_environment.parse(
        """
                           
auto rev_sum <-sum(revenue) by product_id;
                           
with p1 as 
select product_id
where rev_sum>5;

with p2 as 
select product_id
where rev_sum>2;
                           """
    )
    orig_env = test_environment
    test_environment = test_environment.materialize_for_select()
    _ = gen_rowset_node(
        concept=test_environment.concepts["p1.product_id"],
        local_optional=[
            test_environment.concepts["p2.product_id"],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=History(base_environment=orig_env),
    )


def test_gen_rowset_node_merge_parent(
    test_environment: Environment, test_environment_graph
):

    test_environment.parse(
        """
                           
auto rev_sum <-sum(revenue);
                           
with p1 as 
select product_id
where rev_sum>5;

with p2 as 
select product_id
where rev_sum>2;
                           """
    )
    orig_env = test_environment
    test_environment = test_environment.materialize_for_select()
    _ = gen_rowset_node(
        concept=test_environment.concepts["p1.product_id"],
        local_optional=[
            test_environment.concepts["p2.product_id"],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
                history=History(base_environment=orig_env),
    )
