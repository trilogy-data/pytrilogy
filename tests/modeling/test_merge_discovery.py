from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.hooks.query_debugger import DebuggingHook

# from pydantic.functional_validators import merge


def test_merge_discovery(test_environment: Environment, test_executor: Executor):
    # test keys

    test_select = """

auto product_store_name <- store_name || product_name;
auto filtered <- filter product_store_name  where store_id = 2;
SELECT
    filtered
where 
    filtered
;
"""

    assert len(list(test_executor.execute_text(test_select)[0].fetchall())) == 2


def v_grain():
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
merge p1.firstname into p2.firstname;
merge p1.lastname into p2.lastname;

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
    # merge p1.firstname, p3.firstname and p1.lastname, p3.lastname;
    base.parse(
        """
merge  p2.firstname into p1.firstname;
merge p2.lastname  into p1.lastname;

"""
    )

    # GraphHook().query_graph_built(g)
    exec = Dialects.DUCK_DB.default_executor(environment=base, hooks=[DebuggingHook()])
    test_select = """
merge p2.firstname into p1.firstname; 
merge p2.lastname into p1.lastname;
merge p3.firstname into p1.firstname ;
merge p3.lastname into p1.lastname ;
select
    p1.firstname,
    count(p2.lastname) -> lastname_count,
    count(p3.lastname) -> lastname_count_2,
order by
    lastname_count_2 desc
;
"""
    sql = exec.generate_sql(test_select)

    print(sql[-1])
    results = exec.execute_text(test_select)[0].fetchall()
    assert len(list(results)) == 2
    assert results[0].p1_firstname == "John"


def test_merge_no_duplication():
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
    # merge p1.firstname, p3.firstname and p1.lastname, p3.lastname;
    base.parse(
        """
merge  p2.firstname into p1.firstname;
merge p2.lastname  into p1.lastname;

"""
    )
    merge = base.merge_concept(
        base.concepts["p2.firstname"], base.concepts["p1.firstname"], []
    )
    assert not merge
    base_size = base.model_dump_json()
    for x in range(0, 10):
        merge = base.merge_concept(
            base.concepts["p2.firstname"], base.concepts["p1.firstname"], [], force=True
        )
        new_size = base.model_dump_json()
        assert len(base_size) == len(new_size)
