from trilogy.core.models import Environment
from trilogy import Executor
from trilogy import Dialects
from trilogy.core.models import Environment
from trilogy.core.processing.node_generators.node_merge_node import (
    gen_merge_node,
    identify_ds_join_paths,
)
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.env_processor import generate_graph


def test_merge_discovery(test_environment: Environment, test_executor: Executor):
    # test keys

    test_select = """

auto product_store_name <- store_name || product_name;
auto filtered <- filter product_store_name where store_id = 2;
SELECT
    filtered
where 
    filtered
;
"""

    assert len(list(test_executor.execute_text(test_select)[0].fetchall())) == 2


def test_merge_grain():
    # test keys

    base = Environment()

    imports = Environment()

    imports.parse(
        """
key firstname string;
key lastname string;

datasource people (
    lastname:lastname,
    firstname:firstname,)
grain (lastname, firstname)
query '''
SELECT 'John' as firstname, 'Doe' as lastname
UNION
SELECT 'Jane' as firstname, 'Doe' as lastname
UNION
SELECT 'John' as firstname, 'Smith' as lastname
''';
"""
    )

    base.add_import("p1", imports)
    base.add_import("p2", imports)

    exec = Dialects.DUCK_DB.default_executor(environment=base)
    test_select = """
merge p1.firstname, p2.firstname;
merge p1.lastname, p2.lastname;

select
    p1.firstname,
    p1.lastname,
    p2.firstname,
    p2.lastname
;
"""
    sql = exec.generate_sql(test_select)

    print(sql[-1])

    assert len(list(exec.execute_text(test_select)[0].fetchall())) == 3


from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.hooks.graph_hook import GraphHook

def test_merge_grain_two():
    # test keys

    base = Environment()

    imports = Environment()

    imports.parse(
        """
key firstname string;
key lastname string;

datasource people (
    lastname:lastname,
    firstname:firstname,)
grain (lastname, firstname)
query '''
SELECT 'John' as firstname, 'Doe' as lastname
UNION
SELECT 'Jane' as firstname, 'Doe' as lastname
UNION
SELECT 'John' as firstname, 'Smith' as lastname
''';
""",
    )

    base.add_import("p1", imports)
    base.add_import("p2", imports)
    base.add_import("p3", imports)
    #merge p1.firstname, p3.firstname and p1.lastname, p3.lastname;
    base.parse("""
merge p1.firstname, p2.firstname and p1.lastname, p2.lastname;

""")

        # raw = executor.generate_sql(test)
    g = generate_graph(base)
    from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    exec = Dialects.DUCK_DB.default_executor(environment=base, hooks=[DebuggingHook()])
    test_select = """
merge p1.firstname, p2.firstname and p1.lastname, p2.lastname;

select
    p1.firstname,
    count(p2.lastname) -> lastname_count,
;
"""
    sql = exec.generate_sql(test_select)

    print(sql[-1])

    assert len(list(exec.execute_text(test_select)[0].fetchall())) == 3