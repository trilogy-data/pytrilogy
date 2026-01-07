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


def test_user_concepts():
    """Test that user_concepts filters out internal concepts."""
    env, _ = Environment().parse(
        """
key user_id int;
property user_id.name string;
"""
    )
    user_concepts = env.user_concepts()
    addresses = [c.address for c in user_concepts]

    # Should include user-defined concepts
    assert "local.user_id" in addresses
    assert "local.name" in addresses

    # Should not include internal concepts
    for c in user_concepts:
        assert not c.namespace.startswith("__preql_internal")
        assert not c.name.startswith("_")


def test_concepts_at_line():
    """Test finding concepts at a specific line number."""
    env, _ = Environment().parse(
        """key user_id int;
property user_id.name string;
key order_id int;
"""
    )
    # Line 1 should have user_id
    concepts_line_1 = env.concepts_at_line(1)
    assert len(concepts_line_1) >= 1
    assert any(c.name == "user_id" for c in concepts_line_1)

    # Line 2 should have name
    concepts_line_2 = env.concepts_at_line(2)
    assert len(concepts_line_2) >= 1
    assert any(c.name == "name" for c in concepts_line_2)


def test_resolve_concept_simple():
    """Test resolving simple concept references."""
    env, _ = Environment().parse(
        """
key user_id int;
key order_id int;
"""
    )
    # Simple name lookup
    concept = env.resolve_concept("user_id")
    assert concept is not None
    assert concept.name == "user_id"

    # Fully qualified lookup
    concept = env.resolve_concept("local.order_id")
    assert concept is not None
    assert concept.name == "order_id"

    # Non-existent concept
    concept = env.resolve_concept("nonexistent")
    assert concept is None


def test_metadata_column_positions():
    """Test that metadata captures column position information."""
    env, _ = Environment().parse(
        """key user_id int;
property user_id.name string;
"""
    )
    user_id = env.concepts.get("local.user_id")
    assert user_id is not None
    assert user_id.metadata is not None
    assert user_id.metadata.line_number == 1
    # Column positions should be captured
    assert user_id.metadata.column is not None
