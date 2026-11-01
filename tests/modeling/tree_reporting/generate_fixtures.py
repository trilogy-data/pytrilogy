"""Writes the deterministic parquet fixtures the vendored tree models read.

Run from the repo root: `python tests/modeling/tree_reporting/generate_fixtures.py`.

`models/fixtures/` holds what the browser reads: the per-city published tree
parquets, their cross-city union, the species and ecoregion dimensions and the
dashboard summaries. Every summary is computed here from the published trees
with plain DuckDB SQL, so a summary-served answer must equal one read off the
trees. `build_fixtures/` holds the raw partitions a city build reads; the
tests swap each `file` script address for one of these.
"""

import math
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).parent
FIXTURES = ROOT / "models" / "fixtures"
BUILD_FIXTURES = ROOT / "build_fixtures"
UPDATED = datetime(2026, 10, 1, 12, 0, 0)

TEMPERATE = "Temperate Broadleaf & Mixed Forests"
MEDITERRANEAN = "Mediterranean Forests, Woodlands & Scrub"
GRASSLAND = "Temperate Grasslands, Savannas & Shrublands"

# 999 is native to no species: the native-range build must not invent a row for it (#835).
ECOREGIONS = [
    (423, "California interior chaparral and woodlands", MEDITERRANEAN, "nearctic"),
    (425, "California montane chaparral and woodlands", MEDITERRANEAN, "nearctic"),
    (339, "Northeast US Coastal forests", TEMPERATE, "nearctic"),
    (664, "European Atlantic mixed forests", TEMPERATE, "palearctic"),
    (387, "Central US forest-grasslands transition", GRASSLAND, "nearctic"),
    (
        999,
        "Unreferenced moist forests",
        "Tropical & Subtropical Moist Broadleaf Forests",
        "neotropical",
    ),
]

# species, common names, form, native ecoregions, zone min/max, water, drought,
# sun, lifespan min/max, growth, wildlife, fire
SPECIES = [
    (
        "Quercus agrifolia",
        ["Coast live oak", "California live oak"],
        "broadleaf",
        [339, 425, 423],
        8,
        10,
        "low",
        "high",
        ["full_sun"],
        150,
        250,
        "slow",
        "high",
        "low",
    ),
    (
        "Platanus x hispanica",
        ["London plane"],
        "broadleaf",
        [339, 425],
        5,
        9,
        "high",
        "low",
        ["full_sun", "partial_shade"],
        50,
        100,
        "fast",
        "moderate",
        "moderate",
    ),
    (
        "Acer rubrum",
        ["Red maple"],
        "spreading",
        [339],
        3,
        9,
        "moderate",
        "moderate",
        ["partial_shade"],
        60,
        120,
        "moderate",
        "moderate",
        "moderate",
    ),
    (
        "Tilia cordata",
        ["Small-leaved lime"],
        "columnar",
        [664],
        3,
        7,
        "moderate",
        "low",
        ["shade"],
        20,
        40,
        "moderate",
        "low",
        "high",
    ),
    (
        "Unknown",
        ["Species not recorded"],
        "default",
        None,
        None,
        None,
        None,
        None,
        [],
        None,
        None,
        None,
        None,
        None,
    ),
    (
        "Acer platanoides",
        ["Norway maple"],
        "spreading",
        [664],
        4,
        7,
        "moderate",
        "moderate",
        ["full_sun", "partial_shade"],
        100,
        150,
        "fast",
        "low",
        "low",
    ),
    (
        "Platanus occidentalis",
        ["American sycamore"],
        "broadleaf",
        [339, 387],
        4,
        9,
        "high",
        "moderate",
        ["full_sun"],
        200,
        250,
        "fast",
        "high",
        "low",
    ),
    (
        "Gleditsia triacanthos",
        ["Honey locust"],
        "spreading",
        [387],
        3,
        8,
        "low",
        "high",
        ["full_sun"],
        100,
        120,
        "fast",
        "moderate",
        "moderate",
    ),
    (
        "Zelkova serrata",
        [],
        "broadleaf",
        [],
        5,
        8,
        None,
        "high",
        [],
        None,
        None,
        "moderate",
        None,
        None,
    ),
    (
        "Prunus serrulata",
        ["Japanese cherry"],
        "ornamental",
        None,
        5,
        8,
        "moderate",
        "low",
        ["full_sun"],
        15,
        30,
        "moderate",
        "moderate",
        "low",
    ),
    (
        "Malus",
        ["Crabapple"],
        "ornamental",
        None,
        None,
        8,
        "moderate",
        None,
        ["full_sun"],
        None,
        60,
        None,
        "high",
        None,
    ),
    (
        "Dead",
        ["Dead tree"],
        "default",
        None,
        None,
        None,
        None,
        None,
        [],
        None,
        None,
        None,
        None,
        None,
    ),
]

# Trees per city, by species. Every species has an enrichment row, as the model
# declares (`tree_enrichment` binds `species` complete); sentinels included.
CITY_SPECIES = {
    "USBOS": [
        ("Acer platanoides", 6),
        ("Platanus x hispanica", 5),
        ("Acer rubrum", 4),
        ("Tilia cordata", 3),
        ("Gleditsia triacanthos", 3),
        ("Platanus occidentalis", 2),
        ("Quercus agrifolia", 1),
        ("Zelkova serrata", 1),
        ("Prunus serrulata", 1),
        ("Malus", 1),
        ("Unknown", 1),
        ("Dead", 1),
    ],
    "USSFO": [
        ("Quercus agrifolia", 3),
        ("Platanus x hispanica", 2),
        ("Tilia cordata", 2),
        ("Acer rubrum", 1),
        ("Unknown", 1),
        ("Prunus serrulata", 1),
    ],
    "USSTO": [
        ("Gleditsia triacanthos", 3),
        ("Acer platanoides", 2),
        ("Platanus occidentalis", 1),
        ("Malus", 1),
        ("Acer rubrum", 1),
    ],
}
CITY_ORIGIN = {
    "USBOS": (42.350, -71.060),
    "USSFO": (37.770, -122.410),
    "USSTO": (42.910, -89.210),
}
CITY_SOURCES = {
    "USBOS": [
        "CITY_OF_BOSTON",
        "CITY_OF_BOSTON",
        "CAMBRIDGE",
        "OSM_USBOS",
        "COMMUNITY_USBOS",
        "PREDICTED_USBOS",
    ],
    "USSFO": ["SF_OPENDATA", "SF_OPENDATA", "OSM_USSFO", "COMMUNITY_USSFO"],
    "USSTO": ["STOUGHTON_OPENDATA", "STOUGHTON_OPENDATA", "OSM_USSTO"],
}


def published_tree(city: str, i: int, species: str) -> dict:
    lat0, lon0 = CITY_ORIGIN[city]
    # Trees share fine (3dp) cells in pairs and coarse (2dp) cells in tens; the
    # offsets stay clear of the rounding midpoints.
    positioned = i % 11 != 10
    plant = None if i % 6 == 5 else date(1950 + (i * 7) % 75, 1 + i % 12, 1 + i % 27)
    # Neighbours (mostly one species) share a plant year, so a re-sum that fans out shows.
    if i % 4 in (2, 3):
        plant = date(1990 + i // 4, 3 + i % 4, 1)
    if i == 3:
        plant = date(1750, 5, 1)
    if i == 8:
        plant = date(2028, 4, 1)
    dbh = None if i % 5 == 4 else round((i * 3.7) % 45 + 1.0, 1)
    tree_id = f"{city.lower()}-{i + 1}"
    return {
        "tree_id": tree_id,
        "city": city,
        "data_source": CITY_SOURCES[city][i % len(CITY_SOURCES[city])],
        "species": species,
        "tree_name": f"{species} #{i + 1}",
        "plant_date": plant,
        "diameter_at_breast_height": dbh,
        "latitude": (
            lat0 + (i // 2) * 0.0011 + (i % 2) * 0.0002 + 0.0001 if positioned else None
        ),
        "longitude": lon0 - (i // 2) * 0.0013 - 0.0001 if positioned else None,
        "submission_photo_url": "https://example.invalid/p.jpg" if i % 9 == 4 else None,
        "cultivar": (
            "Crimson King" if species == "Acer platanoides" and i % 2 == 0 else None
        ),
        "crown_width_m": 4.5 if i % 4 == 1 else None,
        "cluster_id": tree_id,
        "merged_sources": CITY_SOURCES[city][i % len(CITY_SOURCES[city])],
        "merged_tree_ids": None,
        "reported_missing": False,
    }


def published_trees(city: str) -> list[dict]:
    names = [name for name, count in CITY_SPECIES[city] for _ in range(count)]
    return [published_tree(city, i, name) for i, name in enumerate(names)]


def enrichment_table() -> pa.Table:
    rows = []
    for (
        species,
        names,
        form,
        eco,
        zmin,
        zmax,
        water,
        drought,
        sun,
        lmin,
        lmax,
        growth,
        wildlife,
        fire,
    ) in SPECIES:
        rows.append(
            {
                "species": species,
                "genus": species.split(" ")[0] if species != "Unknown" else None,
                "species_epithet": species.split(" ")[-1] if " " in species else None,
                "family": None,
                "synonyms": [],
                "common_names": names,
                "description": f"Fixture description of {species}.",
                "is_evergreen": species == "Quercus agrifolia",
                "mature_height_min_ft": 20.0,
                "mature_height_max_ft": 60.0,
                "canopy_spread_min_ft": 15.0,
                "canopy_spread_max_ft": 40.0,
                "growth_rate": growth,
                "lifespan_min_years": lmin,
                "lifespan_max_years": lmax,
                "drought_tolerance": drought,
                "water_needs": water,
                "sun_exposure": sun,
                "soil_preferences": ["loam"],
                "root_behavior": "moderate",
                "coastal_tolerance": False,
                "salt_tolerance": False,
                "pollution_tolerance": "moderate",
                "bloom_months": [4, 5],
                "wildlife_value": wildlife,
                "fire_risk": fire,
                "tree_form": form,
                "usda_zone_min": zmin,
                "usda_zone_max": zmax,
                "native_ecoregions": eco,
                "enriched_at": UPDATED,
                "photo_url": f"https://example.invalid/{species.split(' ')[0].lower()}.jpg",
                "photo_license": "cc-by",
                "photo_attribution": "Fixture",
                "trunk_photo_url": None,
                "trunk_photo_license": None,
                "trunk_photo_attribution": None,
            }
        )
    schema = pa.schema(
        [
            ("species", pa.string()),
            ("genus", pa.string()),
            ("species_epithet", pa.string()),
            ("family", pa.string()),
            ("synonyms", pa.list_(pa.string())),
            ("common_names", pa.list_(pa.string())),
            ("description", pa.string()),
            ("is_evergreen", pa.bool_()),
            ("mature_height_min_ft", pa.float64()),
            ("mature_height_max_ft", pa.float64()),
            ("canopy_spread_min_ft", pa.float64()),
            ("canopy_spread_max_ft", pa.float64()),
            ("growth_rate", pa.string()),
            ("lifespan_min_years", pa.int64()),
            ("lifespan_max_years", pa.int64()),
            ("drought_tolerance", pa.string()),
            ("water_needs", pa.string()),
            ("sun_exposure", pa.list_(pa.string())),
            ("soil_preferences", pa.list_(pa.string())),
            ("root_behavior", pa.string()),
            ("coastal_tolerance", pa.bool_()),
            ("salt_tolerance", pa.bool_()),
            ("pollution_tolerance", pa.string()),
            ("bloom_months", pa.list_(pa.int64())),
            ("wildlife_value", pa.string()),
            ("fire_risk", pa.string()),
            ("tree_form", pa.string()),
            ("usda_zone_min", pa.int64()),
            ("usda_zone_max", pa.int64()),
            ("native_ecoregions", pa.list_(pa.int64())),
            ("enriched_at", pa.timestamp("us")),
            ("photo_url", pa.string()),
            ("photo_license", pa.string()),
            ("photo_attribution", pa.string()),
            ("trunk_photo_url", pa.string()),
            ("trunk_photo_license", pa.string()),
            ("trunk_photo_attribution", pa.string()),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def ecoregion_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "ecoregion_id": eid,
                "ecoregion_name": name,
                "realm": realm,
                "biome": biome,
                "biome_code": 1,
                "realm_code": realm[:2].upper(),
                "country_codes": ["US"],
                "centroid_lat": 40.0,
                "centroid_lon": -100.0,
                "geometry_geojson": None,
                "bbox_min_lat": 30.0,
                "bbox_min_lon": -110.0,
                "bbox_max_lat": 50.0,
                "bbox_max_lon": -90.0,
                "is_terrestrial": True,
                "source_version": "fixture",
                "ecoregion_data_updated_through": UPDATED,
            }
            for eid, name, biome, realm in ECOREGIONS
        ]
    )


TREE_SCHEMA = [
    ("tree_id", pa.string()),
    ("city", pa.string()),
    ("data_source", pa.string()),
    ("species", pa.string()),
    ("tree_name", pa.string()),
    ("plant_date", pa.date32()),
    ("diameter_at_breast_height", pa.float64()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("submission_photo_url", pa.string()),
    ("cultivar", pa.string()),
    ("crown_width_m", pa.float64()),
]
PUBLISHED_SCHEMA = TREE_SCHEMA + [
    ("cluster_id", pa.string()),
    ("merged_sources", pa.string()),
    ("merged_tree_ids", pa.string()),
    ("reported_missing", pa.bool_()),
]


def write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def published_table(city: str) -> pa.Table:
    code = city.lower()
    rows = [
        {**row, f"{code}_published_data_updated_through": UPDATED}
        for row in published_trees(city)
    ]
    extra = [(f"{code}_published_data_updated_through", pa.timestamp("us"))]
    if city == "USBOS":
        rows = [
            {
                **row,
                "usbos_border_duplicate": False,
                "height_m": 12.5 if i % 3 == 0 else None,
            }
            for i, row in enumerate(rows)
        ]
        extra += [("usbos_border_duplicate", pa.bool_()), ("height_m", pa.float64())]
    return pa.Table.from_pylist(rows, schema=pa.schema(PUBLISHED_SCHEMA + extra))


SUMMARIES = {
    "tree_rollup_species": "",
    "tree_rollup_year": ", year(plant_date) AS plant_year",
    "tree_rollup_dbh": """, CASE WHEN diameter_at_breast_height IS NULL THEN NULL
        WHEN diameter_at_breast_height < 6 THEN 1 WHEN diameter_at_breast_height < 12 THEN 2
        WHEN diameter_at_breast_height < 24 THEN 3 WHEN diameter_at_breast_height < 36 THEN 4
        ELSE 5 END AS dbh_class""",
    "tree_rollup_cell": ", round(latitude, 3) AS map_cell_latitude, round(longitude, 3) AS map_cell_longitude",
    "tree_rollup_coarse_cell": ", round(latitude, 2) AS map_coarse_cell_latitude, round(longitude, 2) AS map_coarse_cell_longitude",
}


def write_summaries(con: duckdb.DuckDBPyConnection) -> None:
    for name, buckets in SUMMARIES.items():
        sql = f"""SELECT city, species{buckets}, count(tree_id) AS rollup_tree_count,
            TIMESTAMP '{UPDATED}' AS rollup_inputs_published_through
            FROM trees GROUP BY ALL ORDER BY ALL"""
        write(con.sql(sql).to_arrow_table(), FIXTURES / f"{name}_v2.parquet")
    write(
        con.sql(
            f"""SELECT city, avg(latitude) AS city_latitude, avg(longitude) AS city_longitude,
        TIMESTAMP '{UPDATED}' AS rollup_inputs_published_through FROM trees GROUP BY city ORDER BY city"""
        ).to_arrow_table(),
        FIXTURES / "tree_rollup_city_v2.parquet",
    )
    write(
        con.sql(
            f"""SELECT e.species, any_value(e.native_ecoregions) AS native_ecoregions,
            list_sort(list_distinct(list(r.biome) FILTER (r.biome IS NOT NULL))) AS native_biomes,
            list_sort(list_distinct(list(r.realm) FILTER (r.realm IS NOT NULL))) AS native_realms,
            TIMESTAMP '{UPDATED}' AS rollup_inputs_published_through
        FROM enrichment e
        LEFT JOIN (SELECT species, unnest(native_ecoregions) AS eid FROM enrichment) n ON n.species = e.species
        LEFT JOIN ecoregions r ON r.ecoregion_id = n.eid
        GROUP BY e.species ORDER BY e.species"""
        ).to_arrow_table(),
        FIXTURES / "species_native_range_v2.parquet",
    )


def write_dashboard_fixtures() -> None:
    con = duckdb.connect()
    union = []
    for city in CITY_SPECIES:
        table = published_table(city)
        write(table, FIXTURES / f"{city.lower()}_tree_info_v2.parquet")
        union.append(table.select([name for name, _ in TREE_SCHEMA]))
    trees = pa.concat_tables(union).append_column(
        "latest_update_through",
        pa.array([UPDATED] * sum(t.num_rows for t in union), pa.timestamp("us")),
    )
    enrichment, ecoregions = enrichment_table(), ecoregion_table()
    write(trees, FIXTURES / "full_tree_info_v2.parquet")
    write(enrichment, FIXTURES / "tree_enrichment_v2.parquet")
    write(ecoregions, FIXTURES / "ecoregion_info_v2.parquet")
    con.register("trees", trees)
    con.register("enrichment", enrichment)
    con.register("ecoregions", ecoregions)
    write_summaries(con)


# --- City builds -------------------------------------------------------------
#
# Raw partition rows, one tuple per row: (script, tree_id, city, source, species,
# dbh, latitude, longitude). A duplicate sits on exactly the coordinates of the
# row that absorbs it, so it shares every grid cell; distinct trees are ~500m
# apart. `.parquet` scripts are staged files the browser reads too.
RAW_ROWS = [
    (
        "stoughton_tree_info",
        "sto-m1",
        "USSTO",
        "STOUGHTON_OPENDATA",
        "Acer platanoides",
        10.0,
        42.9101,
        -89.2101,
    ),
    (
        "stoughton_tree_info",
        "sto-m2",
        "USSTO",
        "STOUGHTON_OPENDATA",
        "Gleditsia triacanthos",
        -1.0,
        42.9151,
        -89.2151,
    ),
    (
        "stoughton_tree_info",
        "sto-m3",
        "USSTO",
        "STOUGHTON_OPENDATA",
        "Malus",
        8.0,
        42.9201,
        -89.2201,
    ),
    (
        "ussto_osm_staging.parquet",
        "sto-o1",
        "USSTO",
        "OSM_USSTO",
        None,
        None,
        42.9101,
        -89.2101,
    ),
    (
        "ussto_osm_staging.parquet",
        "sto-o2",
        "USSTO",
        "OSM_USSTO",
        "Acer rubrum",
        None,
        42.9301,
        -89.2301,
    ),
    (
        "community_tree_info",
        "sto-c1",
        "USSTO",
        "COMMUNITY_USSTO",
        "Gleditsia triacanthos",
        12.0,
        42.9151,
        -89.2151,
    ),
    (
        "community_tree_info",
        "sto-c2",
        "USSTO",
        "COMMUNITY_USSTO",
        "Acer rubrum",
        3.0,
        42.9401,
        -89.2401,
    ),
    (
        "sf_tree_info",
        "sfo-m1",
        "USSFO",
        "SF_OPENDATA",
        "Quercus agrifolia",
        20.0,
        37.7701,
        -122.4101,
    ),
    (
        "sf_tree_info",
        "sfo-m2",
        "USSFO",
        "SF_OPENDATA",
        "Tilia cordata",
        9.0,
        37.7751,
        -122.4151,
    ),
    (
        "ussfo_osm_staging.parquet",
        "sfo-o1",
        "USSFO",
        "OSM_USSFO",
        "Quercus",
        None,
        37.7701,
        -122.4101,
    ),
    (
        "satellite_tree_info",
        "sfo-s1",
        "USSFO",
        "SATELLITE_USSFO",
        "Tilia",
        None,
        37.7751,
        -122.4151,
    ),
    (
        "satellite_tree_info",
        "sfo-s2",
        "USSFO",
        "SATELLITE_USSFO",
        "Acer",
        None,
        37.7801,
        -122.4201,
    ),
    (
        "community_tree_info",
        "sfo-c1",
        "USSFO",
        "COMMUNITY_USSFO",
        "Acer rubrum",
        4.0,
        37.7851,
        -122.4251,
    ),
    (
        "boston_tree_info",
        "bos-m1",
        "USBOS",
        "CITY_OF_BOSTON",
        "Acer rubrum",
        10.0,
        42.3501,
        -71.0601,
    ),
    (
        "boston_tree_info",
        "bos-m2",
        "USBOS",
        "CITY_OF_BOSTON",
        "Acer rubrum",
        None,
        42.3551,
        -71.0651,
    ),
    (
        "cambridge_tree_info",
        "bos-k1",
        "USBOS",
        "CAMBRIDGE",
        "Acer rubrum",
        14.0,
        42.3601,
        -71.0701,
    ),
    (
        "cambridge_tree_info",
        "bos-k2",
        "USBOS",
        "CAMBRIDGE",
        "Acer saccharinum",
        20.0,
        42.3501,
        -71.0601,
    ),
    (
        "arboretum_tree_info",
        "bos-a1",
        "USBOS",
        "ARNOLD_ARBORETUM",
        "Tilia cordata",
        30.0,
        42.3651,
        -71.0751,
    ),
    (
        "brookline_tree_info",
        "bos-b1",
        "USBOS",
        "BROOKLINE",
        "Gleditsia triacanthos",
        6.0,
        42.3701,
        -71.0801,
    ),
    (
        "somerville_tree_info",
        "bos-v1",
        "USBOS",
        "SOMERVILLE",
        "Platanus x hispanica",
        16.0,
        42.3751,
        -71.0851,
    ),
    (
        "belmont_tree_info",
        "bos-e1",
        "USBOS",
        "BELMONT",
        "Acer platanoides",
        11.0,
        42.3801,
        -71.0901,
    ),
    (
        "watertown_tree_info",
        "bos-w1",
        "USBOS",
        "WATERTOWN",
        "Zelkova serrata",
        7.0,
        42.3851,
        -71.0951,
    ),
    (
        "usbos_osm_staging.parquet",
        "bos-o1",
        "USBOS",
        "OSM_USBOS",
        None,
        None,
        42.3551,
        -71.0651,
    ),
    (
        "usbos_osm_staging.parquet",
        "bos-o2",
        "USBOS",
        "OSM_USBOS",
        "Malus",
        None,
        42.3901,
        -71.1001,
    ),
    (
        "predicted_tree_info",
        "bos-p1",
        "USBOS",
        "PREDICTED_USBOS",
        "Acer",
        None,
        42.3901,
        -71.1001,
    ),
    (
        "predicted_tree_info",
        "bos-p2",
        "USBOS",
        "PREDICTED_USBOS",
        "Quercus",
        None,
        42.3951,
        -71.1051,
    ),
    (
        "satellite_tree_info",
        "bos-s1",
        "USBOS",
        "SATELLITE_USBOS",
        "Tilia",
        None,
        42.4001,
        -71.1101,
    ),
]
# The trees each build publishes: cluster survivors, minus `sto-m3` (an approved
# report says it is gone) and `bos-k2` (a Cambridge row on a Boston tree of the
# same genus: a border duplicate).
EXPECTED_SURVIVORS = {
    "USSTO": {"sto-m1", "sto-m2", "sto-o2", "sto-c2"},
    "USSFO": {"sfo-m1", "sfo-m2", "sfo-s2", "sfo-c1"},
    "USBOS": {
        "bos-m1",
        "bos-m2",
        "bos-k1",
        "bos-a1",
        "bos-b1",
        "bos-v1",
        "bos-e1",
        "bos-w1",
        "bos-o2",
        "bos-p2",
        "bos-s1",
    },
}
# Boston imputes a missing dbh from the (city, species) mean: Acer rubrum is 10 and 14.
EXPECTED_DBH = {"bos-m2": 12.0, "sto-m2": 12.0, "sto-m1": 10.0}
MODIFICATIONS = [
    {
        "tree_id": "sto-m3",
        "species": None,
        "diameter_at_breast_height": None,
        "latitude": None,
        "longitude": None,
        "missing": True,
    },
]
TIMESTAMP_SCRIPTS = {
    "community_update_time": [
        "ussfo_community_data_updated_through",
        "usbos_community_data_updated_through",
        "ussto_community_data_updated_through",
    ],
    "satellite_update_time": [
        "ussfo_satellite_data_updated_through",
        "usbos_satellite_data_updated_through",
        "usnyc_satellite_data_updated_through",
    ],
    "predicted_update_time": ["usbos_predicted_data_updated_through"],
    **{
        name: ["data_updated_through"]
        for name in [
            "stoughton_update_time",
            "stoughton_osm_probe",
            "sf_update_time",
            "sf_osm_probe",
            "boston_update_time",
            "arboretum_update_time",
            "cambridge_update_time",
            "brookline_update_time",
            "somerville_update_time",
            "boston_osm_probe",
            "boston_canopy_probe",
            "prediction_inputs_probe",
        ]
    },
}
# Boston's city portal spells four columns its own way (`city_raw_tree_info`).
BOSTON_CITY_COLUMNS = {
    "tree_id": "id",
    "species": "spp_bot",
    "plant_date": "date_plant",
    "diameter_at_breast_height": "dbh",
}


def canopy_cell(latitude: float, longitude: float) -> int:
    return math.floor(longitude / 0.00003) * 100000000 + math.floor(latitude / 0.00002)


def raw_table(rows: list[tuple], rename: dict[str, str]) -> pa.Table:
    records = [
        {
            "tree_id": tree_id,
            "city": city,
            "data_source": source,
            "species": species,
            "tree_name": None,
            "plant_date": date(2001, 1, 1),
            "diameter_at_breast_height": dbh,
            "latitude": lat,
            "longitude": lon,
            "submission_photo_url": None,
            "cultivar": None,
            "crown_width_m": None,
        }
        for _, tree_id, city, source, species, dbh, lat, lon in rows
    ]
    schema = pa.schema(TREE_SCHEMA)
    table = pa.Table.from_pylist(records, schema=schema)
    return table.rename_columns([rename.get(name, name) for name in table.column_names])


def write_build_fixtures() -> None:
    for script in sorted({row[0] for row in RAW_ROWS}):
        rows = [row for row in RAW_ROWS if row[0] == script]
        table = raw_table(
            rows, BOSTON_CITY_COLUMNS if script == "boston_tree_info" else {}
        )
        write(
            table,
            (
                FIXTURES / script
                if script.endswith(".parquet")
                else BUILD_FIXTURES / f"{script}.parquet"
            ),
        )
    for script, columns in TIMESTAMP_SCRIPTS.items():
        write(
            pa.table(
                {column: pa.array([UPDATED], pa.timestamp("us")) for column in columns}
            ),
            BUILD_FIXTURES / f"{script}.parquet",
        )
    write(
        pa.Table.from_pylist(
            MODIFICATIONS,
            schema=pa.schema(
                [
                    ("tree_id", pa.string()),
                    ("species", pa.string()),
                    ("diameter_at_breast_height", pa.float64()),
                    ("latitude", pa.float64()),
                    ("longitude", pa.float64()),
                    ("missing", pa.bool_()),
                ]
            ),
        ),
        BUILD_FIXTURES / "tree_modification_info.parquet",
    )
    write(
        pa.table(
            {
                "canopy_cell": pa.array([canopy_cell(42.3501, -71.0601)], pa.int64()),
                "canopy_crown_id": ["BOSTON_LIDAR_2024-1"],
                "canopy_height_m": [14.0],
                "canopy_crown_radius_m": [3.0],
                "canopy_source": ["BOSTON_LIDAR_2024"],
            }
        ),
        FIXTURES / "usbos_canopy_height.parquet",
    )


if __name__ == "__main__":
    write_dashboard_fixtures()
    write_build_fixtures()
