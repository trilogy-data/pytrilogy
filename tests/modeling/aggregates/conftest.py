from pytest import fixture

from preql import Environment
from preql.core.env_processor import generate_graph
from preql import Dialects
from preql.hooks.query_debugger import DebuggingHook
from logging import INFO
from pathlib import Path


@fixture(scope="session")
def test_environment():
    env = Environment().from_file(Path(__file__).parent / "orders.preql")

    yield env


@fixture(scope="session")
def test_executor(test_environment: Environment):
    yield Dialects.DUCK_DB.default_executor(
        environment=test_environment, hooks=[DebuggingHook(level=INFO)]
    )


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
