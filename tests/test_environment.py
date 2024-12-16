from pathlib import Path

from trilogy import Dialects
from trilogy.core.enums import Modifier
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models import Environment


def test_environment_serialization(test_environment: Environment):

    path = test_environment.to_cache()
    print(path)
    test_environment2 = Environment.from_cache(path)
    assert test_environment2

    assert test_environment.concepts == test_environment2.concepts
    assert test_environment.datasources == test_environment2.datasources
    for k, v in test_environment.concepts.items():
        assert v == test_environment2.concepts[k]


def test_environment_from_path():
    env = Environment.from_file(Path(__file__).parent / "test_env.preql")

    assert "local.id" in env.concepts


def test_environment_invalid():
    env = Environment()
    env.concepts.fail_on_missing = False
    x = env.concepts["abc"]
    assert x.name == "abc"

    env.concepts.fail_on_missing = True
    try:
        x = env.concepts["abc"]
        assert 1 == 0
    except Exception as e:
        assert isinstance(e, UndefinedConceptException)


def test_environment_merge():
    env1: Environment
    env1, _ = Environment().parse(
        """
key  order_id int;   
                                
                                datasource orders
                                (order_id:order_id)
                                grain (order_id)
                                address orders;
"""
    )

    env2, _ = Environment().parse(
        """
                                key order_id int;
                                                                
                                datasource replacements
                                (order_id:order_id)
                                grain (order_id)
                                address replacements;

                                
"""
    )

    env1.add_import("replacements", env2)

    _ = env1.merge_concept(
        env1.concepts["replacements.order_id"],
        env1.concepts["order_id"],
        modifiers=[Modifier.PARTIAL],
    )

    assert env1.concepts["order_id"] == env1.concepts["replacements.order_id"]

    found = False
    for x in env1.datasources["replacements.replacements"].columns:
        if (
            x.alias == "order_id"
            and x.concept.address == env1.concepts["order_id"].address
        ):
            assert x.concept == env1.concepts["order_id"]
            assert x.modifiers == [Modifier.PARTIAL]
            found = True
    assert found


def test_environment_select_promotion():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
const x <- 6;

select x+2 as y;

select y;            
                    """
    ).fetchall()

    assert results[0].y == 8
