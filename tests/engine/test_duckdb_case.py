from pathlib import Path

from trilogy import Dialects
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import Factory
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import get_upstream_concepts
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.query_processor import process_query
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse_text


def test_case_group():

    default_duckdb_engine = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    test = """
const x <- 1;
const x2 <- x+1;

auto orid <- unnest([1,2,3,6,10]);
property orid.mod_two <- orid % 2;

property orid.cased <-CASE WHEN mod_two = 0 THEN 1 ELSE 0 END;

auto total_mod_two <- sum(cased);

select
    total_mod_two
  ;
    """
    factory = Factory(environment=default_duckdb_engine.environment)
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    cased = factory.build(default_duckdb_engine.environment.concepts["cased"])
    total = factory.build(default_duckdb_engine.environment.concepts["total_mod_two"])
    assert cased.purpose == Purpose.PROPERTY
    assert cased.keys == {"local.orid"}
    assert total.derivation == Derivation.AGGREGATE
    x = resolve_function_parent_concepts(
        total, environment=default_duckdb_engine.environment
    )
    assert len(cased.concept_arguments) == 1
    assert "local.orid" in get_upstream_concepts(cased)

    assert "local.cased" in x
    assert "local.orid" in x
    assert results[0] == (3,)


def test_simple_case_duckdb():
    """Test simple CASE syntax execution in DuckDB."""
    executor = Dialects.DUCK_DB.default_executor()

    test = """
auto category <- unnest(['Seafood', 'Beverages', 'Meat', 'Dairy']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket
order by category asc;
    """
    results = executor.execute_text(test)[0].fetchall()
    assert len(results) == 4
    assert results[0] == ("Beverages", "drink")
    assert results[1] == ("Dairy", "other")
    assert results[2] == ("Meat", "other")
    assert results[3] == ("Seafood", "sea")


def test_simple_case_duckdb_function():
    """Test simple CASE syntax execution in DuckDB."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    test = """
import functions as fun;
auto category <- unnest(['Seafood', 'Beverages', 'Meat', 'Dairy']);


select
    category,
    @fun.is_sea(category) as bucket
order by category asc;
    """
    results = executor.execute_text(test)[0].fetchall()
    assert len(results) == 4
    assert results[3] == ("Seafood", "sea")


def test_simple_case_duckdb_uses_native_syntax():
    """Test that DuckDB uses native simple CASE syntax (not expanded)."""
    env, parsed = parse_text("""
auto category <- unnest(['Seafood', 'Beverages']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket;
    """)
    select = parsed[-1]
    dialect = DuckDBDialect()

    processed = process_query(env, select)
    compiled = dialect.compile_statement(processed)
    assert "CASE" in compiled
    assert "WHEN 'Seafood' THEN" in compiled
    assert "= 'Seafood'" not in compiled


def test_simple_case_bigquery_expands_syntax():
    """Test that BigQuery expands simple CASE to searched CASE."""
    env, parsed = parse_text("""
auto category <- unnest(['Seafood', 'Beverages']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket;
    """)
    select = parsed[-1]
    dialect = BigqueryDialect()

    processed = process_query(env, select)
    compiled = dialect.compile_statement(processed)
    assert "CASE" in compiled
    assert "= 'Seafood'" in compiled
    assert "WHEN" in compiled
