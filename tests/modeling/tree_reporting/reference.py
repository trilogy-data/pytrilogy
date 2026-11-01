"""The trees a dashboard state counts, derived from the fixtures without the planner.

A port of the app's `expectedTrees`/`checkResult` (src/src/tests/dashboardExecution.ts):
the oracle environment answers the same query without summaries, but both can
share a fan-out, so a few charts are also checked against these counts.
"""

from datetime import date
from typing import Any

from tests.modeling.tree_reporting.generate_fixtures import (
    CITY_SPECIES,
    ECOREGIONS,
    SPECIES,
    published_trees,
)

SENTINELS = {"Unknown", "Palm", "Shrub", "Cactus", "Dead"}
ENRICHMENT = {row[0]: row for row in SPECIES}
ECOREGION = {eid: (biome, realm) for eid, _, biome, realm in ECOREGIONS}
TREES = [tree for city in CITY_SPECIES for tree in published_trees(city)]
FIT = {"Native", "Same biome, non-native"}


def species_value(tree: dict, index: int) -> Any:
    row = ENRICHMENT.get(tree["species"])
    return row[index] if row else None


def native_locality_bucket(tree: dict, ctx: dict) -> str:
    eco = species_value(tree, 3) or []
    if ctx["active_city_ecoregion"] in eco:
        return "Native"
    if any(ECOREGION[e][0] == ctx["active_city_biome"] for e in eco):
        return "Same biome, non-native"
    if any(ECOREGION[e][1] == ctx["active_city_realm"] for e in eco):
        return "Native Region, Different Biome"
    return "Non-Native, Different Biome"


def hardiness_fit_bucket(tree: dict, ctx: dict) -> str:
    low, high, zone = (
        species_value(tree, 4),
        species_value(tree, 5),
        ctx["active_city_usda_zone"],
    )
    if low is None or high is None:
        return "Unknown"
    if zone < low or zone > high:
        return "Outside zone"
    return "Edge of tolerance" if zone in (low, high) else "Well within zone"


def water_resilience_bucket(tree: dict) -> str:
    water, drought = species_value(tree, 6), species_value(tree, 7)
    if water == "high" and drought == "low":
        return "High water / low drought tolerance"
    if water == "low" and drought == "high":
        return "Low water / high drought tolerance"
    return "Unknown" if water is None or drought is None else "Moderate / mixed"


def lifespan_bucket(tree: dict) -> str:
    low, high = species_value(tree, 9), species_value(tree, 10)
    years = high if high is not None else low
    if years is None:
        return "Unknown"
    return (
        "Short-lived (<50y)"
        if years < 50
        else "Medium-lived (50-149y)" if years < 150 else "Long-lived (150+y)"
    )


def sun_exposure_label(tree: dict) -> str | None:
    sun = species_value(tree, 8) or []
    for value, label in (
        ("shade", "Shade"),
        ("partial_shade", "Partial shade"),
        ("full_sun", "Full sun"),
    ):
        if value in sun:
            return label
    return None


def dbh_class(tree: dict) -> int | None:
    dbh = tree["diameter_at_breast_height"]
    if dbh is None:
        return None
    return next(
        (i for i, bound in enumerate((6, 12, 24, 36), start=1) if dbh < bound), 5
    )


DBH_LABELS = {1: 'Under 6"', 2: '6–12"', 3: '12–24"', 4: '24–36"', 5: 'Over 36"'}


def field_value(tree: dict, field: str, ctx: dict) -> Any:
    year = tree["plant_date"].year if tree["plant_date"] else None
    values = {
        "species": lambda: tree["species"],
        "tree_form": lambda: species_value(tree, 2),
        "growth_rate": lambda: species_value(tree, 11),
        "wildlife_value": lambda: species_value(tree, 12),
        "fire_risk": lambda: species_value(tree, 13),
        "native_locality_bucket": lambda: native_locality_bucket(tree, ctx),
        "hardiness_fit_bucket": lambda: hardiness_fit_bucket(tree, ctx),
        "water_resilience_bucket": lambda: water_resilience_bucket(tree),
        "lifespan_bucket": lambda: lifespan_bucket(tree),
        "sun_exposure_label": lambda: sun_exposure_label(tree),
        "plant_year": lambda: year if year and 1800 < year < 2030 else None,
        "planted_year": lambda: (
            year if year and 1700 < year <= date.today().year else None
        ),
        "dbh_class_label": lambda: DBH_LABELS.get(dbh_class(tree) or 0),
    }
    return values[field]()


def in_state(tree: dict, state: dict, ctx: dict) -> bool:
    if state["city"] and tree["city"] != state["city"]:
        return False
    if state["genus"] and tree["species"].split(" ")[0] != state["genus"]:
        return False
    if state["species"] and tree["species"] != state["species"]:
        return False
    return all(
        field_value(tree, f["field"], ctx) == f["value"] for f in state["crossFilters"]
    )


def expected_trees(state: dict, ctx: dict) -> list[dict]:
    return [tree for tree in TREES if in_state(tree, state, ctx)]


BUCKET_CHARTS = {
    "summary:hardiness-fit": "hardiness_fit_bucket",
    "summary:water-resilience": "water_resilience_bucket",
    "summary:lifespan-profile": "lifespan_bucket",
    "summary:sun-exposure": "sun_exposure_label",
    "summary:tree-form": "tree_form",
    "summary:wildlife-value": "wildlife_value",
    "summary:growth-rate": "growth_rate",
    "summary:fire-risk": "fire_risk",
    "summary:native-locality": "native_locality_bucket",
    "summary:plant-year": "plant_year",
    "species:sp-hardiness-fit": "hardiness_fit_bucket",
    "species:sp-water-resilience": "water_resilience_bucket",
    "species:sp-lifespan": "lifespan_bucket",
    "species:sp-tree-form": "tree_form",
    "species:sp-wildlife-value": "wildlife_value",
    "species:sp-growth-rate": "growth_rate",
    "species:sp-fire-risk": "fire_risk",
    "species:sp-age-distribution": "planted_year",
    "species:sp-size-distribution": "dbh_class_label",
}


def check(case: dict, ctx: dict, rows: list[dict[str, Any]]) -> str | None:
    """`checkResult`: a message when the rows disagree with the fixtures."""
    expected = expected_trees(case["state"], ctx)
    chart = case["id"].split(" [")[0]
    first = rows[0] if rows else {}
    # The app reads a null count as zero.
    if chart in ("summary:total-trees", "species:sp-total-trees") and (
        first.get("total_trees") or 0
    ) != len(expected):
        return f"total_trees = {first.get('total_trees')}, fixtures say {len(expected)}"
    if chart == "summary:unique-species":
        distinct = {t["species"] for t in expected if t["species"] not in SENTINELS}
        if first.get("unique_species") != len(distinct):
            return f"unique_species = {first.get('unique_species')}, fixtures say {len(distinct)}"
    if chart in ("summary:local-native-share", "species:sp-biome-fit") and expected:
        wanted = sum(native_locality_bucket(t, ctx) in FIT for t in expected) / len(
            expected
        )
        # `::float` renders as a 32-bit REAL.
        if (
            first.get("biome_fit_pct") is None
            or abs(first["biome_fit_pct"] - wanted) > 1e-6
        ):
            return (
                f"biome_fit_pct = {first.get('biome_fit_pct')}, fixtures say {wanted}"
            )
    if chart in ("summary:tree-dot-map", "species:tree-dot-map"):
        digits = 3 if case["state"]["city"] else 2
        positioned = [t for t in expected if t["latitude"] is not None]
        cells = {
            (round(t["latitude"], digits), round(t["longitude"], digits))
            for t in positioned
        }
        counted = sum(row["tree_count"] for row in rows)
        if len(rows) != len(cells) or counted != len(positioned):
            return f"dot map: {len(rows)} cells holding {counted} trees, fixtures say {len(cells)} holding {len(positioned)}"
    for row in rows:
        if (row.get("tree_count") or 0) > len(expected):
            return f"tree_count = {row['tree_count']} exceeds the {len(expected)} trees in scope: a fan-out"
    field = BUCKET_CHARTS.get(chart)
    if field:
        wanted_counts: dict[str, int] = {}
        for tree in expected:
            bucket = field_value(tree, field, ctx)
            if bucket is not None:
                wanted_counts[str(bucket)] = wanted_counts.get(str(bucket), 0) + 1
        got: dict[str, int] = {}
        for row in rows:
            if row[field] is not None:
                got[str(row[field])] = got.get(str(row[field]), 0) + row["tree_count"]
        if got != wanted_counts:
            return f"{field} counts {got}, fixtures say {wanted_counts}"
    return None
