from trilogy import Environment
from trilogy.constants import Parsing
from trilogy.core.models.environment import DictImportResolver, EnvironmentConfig
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parser import parse_text

PARSE_CONFIG = Parsing(strict_name_shadow_enforcement=True)


SOURCES = {
    "core": """
key city enum<string>['USNYC', 'USBTV'];
param data_version string default '2';
""",
    "tree_common": """
import core;

key tree_id string;
key species string;
""",
    "usbtv.burlington_tree_info": """
import tree_common;

partial datasource burlington_tree_info (
    tree_id,
    city,
    species,
)
grain (tree_id)
complete where city = 'USBTV'
file f`https://storage.googleapis.com/trilogy_public_models/duckdb/trees/usbtv_tree_info_v{data_version}.parquet`:f`gcs://trilogy_public_models/duckdb/trees/usbtv_tree_info_v{data_version}.parquet`;
""",
    "usnyc.nyc_tree_info": """
import tree_common;

partial datasource nyc_tree_info (
    tree_id,
    city,
    species,
)
grain (tree_id)
complete where city = 'USNYC'
file f`https://storage.googleapis.com/trilogy_public_models/duckdb/trees/usnyc_tree_info_v{data_version}.parquet`:f`gcs://trilogy_public_models/duckdb/trees/usnyc_tree_info_v{data_version}.parquet`;
""",
    "tree_info": """
import tree_common;
import usbtv.burlington_tree_info;
import usnyc.nyc_tree_info;

datasource tree_info (
    tree_id,
    city,
    species,
)
grain (tree_id)
file f`https://storage.googleapis.com/trilogy_public_models/duckdb/trees/full_tree_info_v{data_version}.parquet`:f`gcs://trilogy_public_models/duckdb/trees/full_tree_info_v{data_version}.parquet`;
""",
    "tree_enrichment": """
import tree_info;

property species.fire_risk enum<string>['low', 'moderate', 'high'];

datasource tree_enrichment (
    species,
    ?fire_risk,
)
grain (species)
file f`https://storage.googleapis.com/trilogy_public_models/duckdb/trees/tree_enrichment_v{data_version}.parquet`:f`gcs://trilogy_public_models/duckdb/trees/tree_enrichment_v{data_version}.parquet`;
""",
}


BASE_QUERY = "import tree_enrichment;\nSELECT count(tree_id) as tree_count ORDER BY tree_count DESC;"
INLINE_QUERY = (
    "import tree_enrichment;\n"
    "SELECT count(tree_id) as tree_count WHERE city = 'USBTV' ORDER BY tree_count DESC;"
)
FILTER_QUERY = "WHERE (city = 'USBTV') SELECT 1 as __ftest;"


def build_environment() -> Environment:
    resolver = DictImportResolver(content=SOURCES)
    return Environment(config=EnvironmentConfig(import_resolver=resolver))


def compile_inline_sql() -> str:
    env = build_environment()
    env, parsed = parse_text(INLINE_QUERY, env, parse_config=PARSE_CONFIG)
    dialect = DuckDBDialect()
    processed = dialect.generate_queries(env, [parsed[-1]])[-1]
    return dialect.compile_statement(processed)


def compile_appended_filter_sql() -> str:
    env = build_environment()
    env, parsed = parse_text(BASE_QUERY, env, parse_config=PARSE_CONFIG)
    statement = parsed[-1]

    _, filter_parsed = parse_text(FILTER_QUERY, env, parse_config=PARSE_CONFIG)
    where_clause = filter_parsed[-1].where_clause

    assert statement.where_clause is None
    assert where_clause is not None

    statement.where_clause = where_clause

    dialect = DuckDBDialect()
    processed = dialect.generate_queries(env, [statement])[-1]
    return dialect.compile_statement(processed)


def test_inline_where_uses_partial_datasource():
    sql = compile_inline_sql()

    assert "usbtv_tree_info_v2.parquet" in sql
    assert "full_tree_info_v2.parquet" not in sql


def test_appended_filter_should_also_use_partial_datasource():
    sql = compile_appended_filter_sql()

    assert "usbtv_tree_info_v2.parquet" in sql
    assert "full_tree_info_v2.parquet" not in sql