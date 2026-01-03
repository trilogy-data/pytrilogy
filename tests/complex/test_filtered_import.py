from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.models.author import FilterItem


def test_filtered_import_parsing() -> None:
    """Test that filtered import syntax is parsed correctly and creates FilterItem lineage."""
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    # Just parse the import to verify concepts are set up correctly
    exec.parse_text("import orders;")
    env = exec.environment

    # store.store_id should be a filtered concept
    store_concept = env.concepts["store.store_id"]
    assert store_concept.derivation == Derivation.FILTER
    assert isinstance(store_concept.lineage, FilterItem)
    # The hidden base should exist
    assert "store._store_id" in env.concepts

    # store_comp.store_id should be a normal (unfiltered) concept
    store_comp_concept = env.concepts["store_comp.store_id"]
    assert store_comp_concept.derivation != Derivation.FILTER


def test_filtered_import() -> None:
    """Test that filtered import produces correct query results."""
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    # First test: just count the store_comp (unfiltered) - should be 3
    sql = exec.execute_text(
        """
import orders;

select
    store_comp.store_id.count
;"""
    )[-1]
    results = sql.fetchall()
    output = results[0]
    assert output["store_comp_store_id_count"] == 3


def test_filtered_import_filtered_count() -> None:
    """Test that filtered import produces correct filtered count."""
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    # Count the filtered store.store_id - should be 2 (only stores with orders)
    sql = exec.execute_text(
        """
import orders;

select
    store.store_id.count
;"""
    )[-1]
    results = sql.fetchall()
    output = results[0]
    assert output["store_store_id_count"] == 2
