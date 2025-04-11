from pathlib import Path

from pytest import raises

from trilogy import Dialects
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.exceptions import (
    FrozenEnvironmentException,
    UndefinedConceptException,
)
from trilogy.core.models.author import Concept
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment, LazyEnvironment


def test_concept_rehydration():
    test = {
        "name": "order_timestamp.month_start",
        "datatype": "date",
        "purpose": "property",
        "derivation": "root",
        "granularity": "multi_row",
        "metadata": {
            "description": "Auto-derived from a local.order_timestamp. The date truncated to the month.",
            "line_number": None,
            "concept_source": "auto_derived",
        },
        "lineage": {
            "operator": "date_truncate",
            "arg_count": 2,
            "output_datatype": "date",
            "output_purpose": "property",
            "valid_inputs": [
                ["datetime", "string", "timestamp", "date"],
                ["date_part"],
            ],
            "arguments": [
                {
                    "address": "local.order_timestamp",
                    "datatype": "timestamp",
                    "metadata": {
                        "description": None,
                        "line_number": None,
                        "concept_source": "manual",
                    },
                },
                "month",
            ],
        },
        "namespace": "local",
        "keys": ["local.order_timestamp"],
        "grain": {"components": ["local.order_id"], "where_clause": None},
        "modifiers": [],
        "pseudonyms": [],
    }
    Concept.model_validate(test)
    test["lineage"]["arguments"] = [
        {
            "address": "local.order_timestamp",
            "datatype": "timestamp",
            "metadata": {
                "description": None,
                "line_number": None,
                "concept_source": "manual",
            },
        },
        "monthz",
    ]
    with raises(TypeError):
        Concept.model_validate(test)


def test_environment_serialization(test_environment: Environment):

    path = test_environment.to_cache()
    test_environment2 = Environment.from_cache(path)
    assert test_environment2

    assert test_environment.concepts == test_environment2.concepts
    assert test_environment.datasources == test_environment2.datasources
    for k, v in test_environment.concepts.items():
        assert v == test_environment2.concepts[k]


def test_environment_from_path():
    env = Environment.from_file(Path(__file__).parent / "test_env.preql")

    assert "local.id" in env.concepts


def test_lazy_environment_from_path():
    env = LazyEnvironment(load_path=Path(__file__).parent / "test_env.preql")

    assert not env.loaded

    _ = len(env.concepts)

    assert env.loaded

    env2 = Environment.from_file(Path(__file__).parent / "test_env.preql")

    assert env.concepts == env2.concepts


def test_frozen_environment():
    env = Environment.from_file(Path(__file__).parent / "test_env.preql")

    exec = Dialects.DUCK_DB.default_executor(environment=env)

    run = exec.execute_query(
        """select id+5 ->id_plus_5 order by id_plus_5 asc;"""
    ).fetchall()
    assert run[0].id_plus_5 == 6
    env.freeze()
    with raises(FrozenEnvironmentException):
        env.add_concept(
            Concept(name="test", datatype=DataType.INTEGER, purpose=Purpose.KEY)
        )


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
