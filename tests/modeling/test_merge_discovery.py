from trilogy.core.models import Environment
from trilogy import Executor
from trilogy import Dialects


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
