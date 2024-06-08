from preql.core.models import Environment
from pathlib import Path


def test_environment_serialization(test_environment: Environment):
    str(test_environment)
    path = test_environment.to_cache()

    test_environment2 = Environment.from_cache(path)
    assert test_environment2

    assert test_environment.concepts == test_environment2.concepts
    assert test_environment.datasources == test_environment2.datasources
    for k, v in test_environment.concepts.items():
        assert v == test_environment2.concepts[k]


def test_environment_from_path():

    env = Environment.from_file(Path(__file__).parent / "test_env.preql")

    assert "id" in env.concepts
