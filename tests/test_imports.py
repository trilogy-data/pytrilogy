from pathlib import Path

from trilogy.core.models.environment import Environment, LazyEnvironment


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
