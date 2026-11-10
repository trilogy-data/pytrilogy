from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment

TABLES = {
    "full_tree_info": "SELECT * FROM (VALUES ('t1', 'Acer rubrum', 10.0),"
    " ('t2', 'Pinus nigra', 10.0), ('t-new', 'Novus', 10.0))"
    " t(tree_id, species, diameter_at_breast_height)",
    "tree_enrichment": "SELECT * FROM (VALUES ('Acer rubrum', 'Acer', 'broadleaf'),"
    " ('Pinus nigra', 'Pinus', 'conifer')) t(species, genus, tree_form)",
    "published_tree_heights": "SELECT * FROM (VALUES ('t1', 1.0), ('t2', 1.0),"
    " ('t-new', 1.0)) t(tree_id, height_crown_radius_m)",
    "crown_width_coefficients": "SELECT * FROM (VALUES ('genus', 'Acer', 0.6, 0.3,"
    " 100.0)) t(level, taxon, b, scale, dbh_max_cm)",
    "crown_width_fallbacks": "SELECT * FROM (VALUES"
    " ('division', 'Angiosperm', 0.6, 0.3, 100.0),"
    " ('division', 'Gymnosperm', 0.5, 0.3, 100.0),"
    " ('global', 'all', 0.55, 0.3, 100.0)) t(level, taxon, b, scale, dbh_max_cm)",
}

MODEL = """
key tree_id string;
key species string;
key genus string;
property tree_id.diameter_at_breast_height float;
property species.tree_form string;
property tree_id.height_crown_radius_m float;
root datasource full_tree_info (tree_id: tree_id, species: species,
    diameter_at_breast_height: ?diameter_at_breast_height)
grain (tree_id) file `{dir}/full_tree_info.parquet`;
root datasource tree_enrichment (species: ~species, genus: ?genus, tree_form: ?tree_form)
grain (species) file `{dir}/tree_enrichment.parquet`;
root datasource published_tree_heights (tree_id: ~tree_id,
    height_crown_radius_m: ?height_crown_radius_m)
grain (tree_id) file `{dir}/published_tree_heights.parquet`;
property genus.crown_coefficient_level string;
property genus.crown_fit_b float;
property genus.crown_fit_scale float;
property genus.crown_fit_dbh_max_cm float;
root datasource crown_width_coefficients (level: crown_coefficient_level, taxon: genus,
    b: crown_fit_b, scale: crown_fit_scale, dbh_max_cm: crown_fit_dbh_max_cm)
grain (genus) file `{dir}/crown_width_coefficients.parquet`
where crown_coefficient_level = 'genus';
{decl} crown_fallback_class <- case
    when species = 'Unknown' then 'all'
    when tree_form = 'conifer' then 'Gymnosperm'
    else 'Angiosperm'
end;
def class_row(value, taxon, cls) -> max(value ? taxon = cls) by *;
def for_class(value, taxon) -> case
    when crown_fallback_class = 'Angiosperm' then @class_row(value, taxon, 'Angiosperm')
    when crown_fallback_class = 'Gymnosperm' then @class_row(value, taxon, 'Gymnosperm')
    when crown_fallback_class = 'all' then @class_row(value, taxon, 'all')
    else null
end;
key crown_fallback_taxon string;
property crown_fallback_taxon.crown_fallback_row_level string;
property crown_fallback_taxon.crown_fallback_row_b float;
property crown_fallback_taxon.crown_fallback_row_scale float;
property crown_fallback_taxon.crown_fallback_row_dbh_max_cm float;
root datasource crown_width_fallbacks (level: crown_fallback_row_level,
    taxon: crown_fallback_taxon, b: crown_fallback_row_b,
    scale: crown_fallback_row_scale, dbh_max_cm: crown_fallback_row_dbh_max_cm)
grain (crown_fallback_taxon) file `{dir}/crown_width_fallbacks.parquet`;
auto crown_fallback_b <- @for_class(crown_fallback_row_b, crown_fallback_taxon);
auto crown_fallback_scale <- @for_class(crown_fallback_row_scale, crown_fallback_taxon);
auto crown_fallback_dbh_max_cm <- @for_class(crown_fallback_row_dbh_max_cm,
    crown_fallback_taxon);
auto crown_scale <- coalesce(crown_fit_scale, crown_fallback_scale);
def crown_width_at(dbh) -> 2 * crown_scale * dbh;
auto dbh_cm <- diameter_at_breast_height;
auto measured_stem_crown_width_m <- @crown_width_at(dbh_cm);
auto height_shaded <- height_crown_radius_m > measured_stem_crown_width_m;
"""


@pytest.fixture(scope="module")
def parquet_dir(tmp_path_factory) -> str:
    out = tmp_path_factory.mktemp("trees")
    for name, sql in TABLES.items():
        target = Path(out, f"{name}.parquet").as_posix()
        duckdb.execute(f"COPY ({sql}) TO '{target}' (FORMAT PARQUET)")
    return out.as_posix()


@pytest.mark.parametrize("decl", ["auto", "key"])
def test_derived_key_over_partial_lookup_keeps_unmatched_rows(
    parquet_dir: str, decl: str
):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.parse_text(MODEL.replace("{decl}", decl).replace("{dir}", parquet_dir))
    rows = ex.execute_text(
        "select tree_id, height_shaded where tree_id is not null order by tree_id asc;"
    )[-1].fetchall()
    assert rows == [("t-new", False), ("t1", False), ("t2", False)]
