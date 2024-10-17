from trilogy import Environment
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
