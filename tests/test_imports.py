from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentConfig,
    LazyEnvironment,
)


def test_multi_environment():
    basic = Environment()

    basic.parse(
        """
const pi <- 3.14;
                     

""",
        namespace="math",
    )

    basic.parse(
        """
                
            select math.pi;
                """
    )

    assert basic.concepts["math.pi"].name == "pi"


def test_test_alias_free_import():
    basic = Environment(working_path=Path(__file__).parent)

    basic.parse(
        """
import test_env;

key id2 int;


""",
    )

    assert basic.concepts["id"].name == "id"
    assert basic.concepts["id2"].name == "id2"
    assert basic.concepts["id"].namespace == basic.concepts["id2"].namespace


def test_import_concept_resolution():
    basic = LazyEnvironment(
        load_path=Path(__file__).parent / "test_lazy_env.preql",
        working_path=Path(__file__).parent,
        setup_queries=[],
    )
    materialized = basic.materialize_for_select()
    assert "one.two.import_key" in materialized.materialized_concepts
    assert "two.two.import_key" in materialized.materialized_concepts


def test_import_basics():
    basic = Environment(working_path=Path(__file__).parent)

    basic.parse(
        """
import test_env;

key id2 int;


""",
    )

    assert len(basic.imports["local"]) == 1, basic.imports
    importz = basic.imports["local"][0]
    assert importz.path == Path("test_env")
    expected = Path(__file__).parent / "test_env.preql"
    assert importz.input_path == expected


def test_recursive():
    basic = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=basic)
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = exec.execute_text(
        """
import recursive_env;

where id = 1
select id, name, parent.name, child.name
order by id asc;
""",
    )[-1].fetchall()

    row = results[0]
    assert row[0] == 1
    assert row[1] == "fun"
    assert row[2] == "greatest"
    assert row[3] == "times"


def test_self_import_dict_resolver():
    self_content = """
self import as parent;
self import as child;

key id int;
property id.name string;

datasource id_source (
    id,
    name,
    parent.id,
    child.id
)
grain (id)
query '''
select 1 as id, 'fun' as name, 0 as parent_id, 2 as child_id
union all
select 2 as id, 'times' as name, 1 as parent_id, null as child_id
union all
select 0 as id, 'greatest' as name, null as parent_id, 1 as child_id
''';
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={".": self_content})
    )
    env = Environment(config=config)
    env.parse(self_content)

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    results = exec.execute_text(
        """
where id = 1
select id, name, parent.name, child.name
order by id asc;
"""
    )[-1].fetchall()

    row = results[0]
    assert row[0] == 1
    assert row[1] == "fun"
    assert row[2] == "greatest"
    assert row[3] == "times"
