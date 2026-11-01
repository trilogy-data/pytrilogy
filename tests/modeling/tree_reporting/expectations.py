"""Pinned current behaviour, keyed `<city> <case id>`, against pytrilogy main 564f57c.

`SERVED`: every case not listed reads only count summaries ("summary"). A case
listed here reads the tree file ("trees"), both ("mixed"), or neither. Each miss
is a summary-coverage gap tracked in #827; a fix flips it, and the pin is then
deleted, never edited to "trees".

`WRONG_ANSWERS`: strict xfails for answers that differ from the trees today.
"""


def keyed(city: str, charts: list[str], variants: list[str]) -> list[str]:
    return [f"{city} {chart}{variant}" for chart in charts for variant in variants]


SELECTORS = [
    "species:selector-genus-top",
    "species:selector-genus-all",
    "species:selector-species-top",
    "species:selector-species-all",
]
SPECIES_VARIANTS = ["", " [cross:native_locality_bucket]", " [species]", " [cultivar]"]

SERVED: dict[str, str] = {
    # #827 G2/G3: the same column selected twice (option_value, option_label).
    **{
        key: "trees"
        for key in keyed("USBOS", SELECTORS, ["", " [cross:native_locality_bucket]"])
    },
    **{key: "trees" for key in keyed("USBOS", SELECTORS[2:], [" [cultivar]"])},
    **{key: "trees" for key in keyed("ALL", SELECTORS, [""])},
    # #827 K3: a ratio over the total beside coalesce(common_names[1], species).
    **{
        key: "trees"
        for key in keyed("USBOS", ["species:sp-top-species"], SPECIES_VARIANTS)
    },
    "ALL species:sp-top-species": "trees",
    # #827 filtered aggregates: count_distinct(tree_id ? <bucket>) / count_distinct(tree_id).
    **{
        key: "trees"
        for key in keyed(
            "USBOS",
            ["summary:local-native-share"],
            ["", " [cross:species]", " [cross:native_locality_bucket]"],
        )
    },
    **{
        key: "trees"
        for key in keyed("USBOS", ["species:sp-biome-fit"], SPECIES_VARIANTS)
    },
    # #827 key + property with no aggregate under a partition (city) filter.
    **{key: "trees" for key in keyed("USBOS", ["species:context"], SPECIES_VARIANTS)},
    # No city filter and no tree concept: answered from the species dimension alone.
    "ALL species:context": "neither",
}

NULL_CELL = (
    "unfiled: tree_rollup_source binds map_cell_* / map_coarse_cell_* without `?`, so the summary plan drops "
    "`WHERE map_cell_latitude IS NOT NULL` and returns the unpositioned trees as a NULL cell; the tree plan keeps it"
)

WRONG_ANSWERS: dict[str, str] = {
    **{
        key: NULL_CELL
        for key in keyed(
            "USBOS", ["summary:tree-dot-map", "species:tree-dot-map"], [""]
        )
    },
    "USBOS species:tree-dot-map [species]": NULL_CELL,
    **{
        key: NULL_CELL
        for key in keyed("ALL", ["summary:tree-dot-map", "species:tree-dot-map"], [""])
    },
}
