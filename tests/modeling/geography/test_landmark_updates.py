from pathlib import Path

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig

PREQL_PATH = Path(__file__).parent / "landmark_info.preql"
TREE_PATH = Path(__file__).parent / "tree_enrichment.preql"


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

    results = executor.generate_sql(
        "SELECT  tree_id,  common_name,  diameter_at_breast_height,  latitude,  longitude WHERE city = 'USBOS' AND diameter_at_breast_height >= 48 LIMIT 100;"
    )[-1]
    # With city='USBOS', only the Boston source should appear — SF and NYC are mutually
    # exclusive with this filter and must be dropped by create_union_datasource.
    assert "sf_tree_info" not in results, results
    assert "nyc_tree_info" not in results, results
    assert "boston" in results.lower(), results
