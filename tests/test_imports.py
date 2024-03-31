from preql import Environment
from preql.core.models import LazyEnvironment
from pathlib import Path


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


def test_lazy_import():
    base = Environment()

    env = LazyEnvironment(
        load_path=Path(__file__).parent / "stack_overflow/stackoverflow.preql",
        working_path=Path(__file__).parent / "stack_overflow/",
    )

    assert len(env.concepts) > len(base.concepts)
