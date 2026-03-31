from pathlib import Path

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.single_execution import execute_script_for_refresh

PREQL_PATH = Path(__file__).parent / "landmark_info.preql"
TREE_PATH = Path(__file__).parent / "tree_enrichment.preql"
GRAINLESS_PATH = Path(__file__).parent / "sf_landmarks_grainless.preql"
MULTI_ENUM_PATH = Path(__file__).parent / "multi_enum_union.preql"
MULTI_ENUM_CORRECTNESS_PATH = Path(__file__).parent / "boston_multi_enum.preql"


def _make_executor():
    return Dialects.DUCK_DB.default_executor(
        working_path=Path(__file__).parent,
        conf=DuckDBConfig(enable_python_datasources=True, enable_gcs=True),
    )


def test_landmark_info_geometry_cast_in_sql():
    """geometry must be computed via ST_GeomFromText, not selected raw from the union CTE."""
    executor = _make_executor()
    with open(PREQL_PATH) as f:
        executor.parse_text(f.read())

    datasource = executor.environment.datasources["landmark_info"]
    sql = executor.update_datasource(datasource, dry_run=True)

    assert sql is not None, "Expected SQL to be generated"
    assert "ST_GeomFromText" in sql, (
        "geometry column must be computed via ST_GeomFromText(geometry_raw), "
        f"not referenced directly from the union CTE.\nSQL:\n{sql}"
    )


def test_query_fetch():
    executor = _make_executor()
    with open(TREE_PATH) as f:
        executor.parse_text(f.read())
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = executor.generate_sql(
        "SELECT  tree_id,  common_name,  diameter_at_breast_height,  latitude,  longitude WHERE city = 'USBOS' AND diameter_at_breast_height >= 48 LIMIT 100;"
    )[-1]
    # With city='USBOS', only the Boston source should appear — SF and NYC are mutually
    # exclusive with this filter and must be dropped by create_union_datasource.
    assert "sf_tree_info" not in results, results
    assert "nyc_tree_info" not in results, results
    assert "boston" in results.lower(), results


def test_probe_script_runs():
    """Directly invoke the grainless probe script via subprocess to surface raw errors."""
    import subprocess

    probe = Path(__file__).parent / "sf_landmarks_grainless_probe.py"
    result = subprocess.run(
        ["uv", "run", "--no-project", str(probe)],
        capture_output=True,
    )
    assert result.returncode == 0, (
        f"probe script exited {result.returncode}\n"
        f"stdout: {result.stdout.decode()}\n"
        f"stderr: {result.stderr.decode()}"
    )


def test_can_refresh():
    """Grainless root probe datasource must be resolvable when refreshing a partial datasource.

    Reproduces: 'Could not resolve connections for query' when the root datasource
    providing a freshness concept has no grain (scalar/single-row) and the target
    partial datasource includes a BASIC-derived column alongside root columns.
    """
    executor = Dialects.DUCK_DB.default_executor(
        working_path=GRAINLESS_PATH.parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    execute_script_for_refresh(
        executor, ScriptNode(path=GRAINLESS_PATH), quiet=True, dry_run=True
    )


def test_multi_enum_union_sourcing():
    """Both partial sources must appear in the union when non_partial_for has two enum conditions.

    Reproduces: only one source being picked arbitrarily when the discriminating
    enum key is not the first concept argument in the compound non_partial_for clause.
    """
    executor = Dialects.DUCK_DB.default_executor(
        working_path=MULTI_ENUM_PATH.parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    with open(MULTI_ENUM_PATH) as f:
        executor.parse_text(f.read())

    datasource = executor.environment.datasources["combined_output"]
    sql = executor.update_datasource(datasource, dry_run=True)

    assert sql is not None, "Expected SQL to be generated"
    assert "raw_alpha" in sql, f"alpha source missing from SQL:\n{sql}"
    assert "raw_beta" in sql, f"beta source missing from SQL:\n{sql}"


def test_multi_enum_correctness():
    """Validate that multi-enum complete sources can be resolved together."""
    executor = Dialects.DUCK_DB.default_executor(
        working_path=MULTI_ENUM_CORRECTNESS_PATH.parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    queries = execute_script_for_refresh(
        executor,
        ScriptNode(path=MULTI_ENUM_CORRECTNESS_PATH),
        quiet=True,
        dry_run=True,
        force_sources={"boston_tree_info"},
    )
    for q in queries.refresh_queries:
        assert "arboretum_raw_tree_info" in q.sql, q.sql
        assert "city_raw_tree_info" in q.sql, q.sql


def test_multi_condition_resolution():
    """Validate that multi-enum complete sources can be resolved together."""
    executor = Dialects.DUCK_DB.default_executor(
        working_path=MULTI_ENUM_CORRECTNESS_PATH.parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )

    from trilogy.hooks import DebuggingHook

    DebuggingHook()

    sql = executor.generate_sql(
        """
import tree_enrichment;

where city = 'USNYC' and tree_category='deciduous'
select
    count(tree_id) as total_trees;


"""
    )

    assert "NVALID_REFERENCE_BUG" not in sql[-1], sql[-1]


def test_exact_match_resolution():
    """Partial datasource exact-match must not be disqualified when the WHERE clause
    contains a condition on a concept from a joined datasource.

    Reproduces: sf_tree_info (complete where city='USSFO') being rejected as an exact
    match when the query also has `native_status IS NOT NULL` — a condition whose concept
    lives in tree_enrichment, not sf_tree_info.  The system was falling through to the
    full tree_info datasource instead.
    """
    query = """
import tree_enrichment;

SELECT native_status, count(tree_id) as tree_count
WHERE city = 'USSFO' and native_status IS NOT NULL
ORDER BY tree_count DESC;"""

    base = Dialects.DUCK_DB.default_executor(
        working_path=Path(__file__).parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )

    generated = base.generate_sql(query)[-1]
    assert "full_tree_info" not in generated, generated


def test_exact_match_merge_preserves_subgraph_filters():
    """Filters outside non_partial_for must still reach every relevant subgraph.

    Reproduces: `species='Oak'` being pushed only into the tree-info aggregate branch
    after exact-match resolution on `city='USBOS'`, leaving tree_enrichment unfiltered.
    """
    query = """
import tree_enrichment;

where city = 'USBOS' and species = 'Oak'
select
    species,
    common_names,
    tree_category,
    count(tree_id) by species as tree_count;
"""

    base = Dialects.DUCK_DB.default_executor(
        working_path=Path(__file__).parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )

    generated = base.generate_sql(query)[-1]
    assert '"tree_enrichment"."species" = \'Oak\'' in generated, generated
    assert '"boston_tree_info"."species" = \'Oak\'' in generated, generated
