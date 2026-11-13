"""A value at a coarser grain than the rows it is read beside (a class
fallback per species or genus, off a `by *` constant) is computed once per
that grain and joined back to the rows on it; the rows never pair with it on
anything else, so no CTE repeats a tree (#964)."""

import re

import pytest

from tests.helpers.planning import plan
from tests.helpers.rows import executor_for, fetch_rows
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.constants import DepthLabel
from trilogy.core.processing.v4_helper.group_graph import _source_asked_root_keys
from trilogy.core.processing.v4_helper.join_plan import _relevant_carry_keys
from trilogy.core.processing.v4_helper.models import (
    GroupAttrs,
    GroupInputContract,
    InputChannel,
)

SPECIES = """
key tree_id string;
key species string;
property tree_id.city string;
property tree_id.species string;
property species.genus string;
property species.tree_class string;
property tree_id.dbh float;

key fit_class string;
property fit_class.fit_a float;

datasource trees (tree_id, city, species, genus, tree_class, dbh) grain (tree_id)
query '''select * from (values ('t1','bos','acer rubrum','acer','broadleaf', 10.0),
('t2','bos','pinus strobus','pinus','conifer', 20.0),
('t3','bos','acer rubrum','acer','broadleaf', 30.0),
('t4','bos','acer rubrum','acer','broadleaf', 40.0),
('t5','sfo','acer rubrum','acer','broadleaf', 50.0)) t(tree_id, city, species, genus,
tree_class, dbh)''';

datasource class_fits (fit_class, a: fit_a) grain (fit_class)
query '''select * from (values ('broadleaf', 1.0), ('conifer', 2.0), ('palm', 3.0))
t(fit_class, a)''';

auto broadleaf_a <- max(fit_a ? fit_class = 'broadleaf') by *;
auto conifer_a <- max(fit_a ? fit_class = 'conifer') by *;
auto class_a <- case when tree_class = 'broadleaf' then broadleaf_a
    when tree_class = 'conifer' then conifer_a end;
auto genus_n <- count(tree_id) by genus;
auto genus_a <- case when genus_n > 3 then 1.5 end;
auto tree_a <- coalesce(genus_a, class_a);
auto pred <- dbh * tree_a;
"""

GENUS = """
key tree_id string;
key species string;
key genus string;
property tree_id.city string;
property tree_id.species string;
property species.genus string;
property genus.tree_class string;

key fit_class string;
property fit_class.fit_a float;

datasource trees (tree_id, city, species) grain (tree_id)
query '''select * from (values ('t1','bos','acer rubrum'), ('t2','bos','pinus strobus'),
('t3','sfo','acer rubrum'), ('t4','bos','acer saccharum')) t(tree_id, city, species)''';
datasource species_info (species, genus) grain (species)
query '''select * from (values ('acer rubrum','acer'), ('acer saccharum','acer'),
('pinus strobus','pinus')) t(species, genus)''';
datasource genus_info (genus, tree_class) grain (genus)
query '''select * from (values ('acer','broadleaf'), ('pinus','conifer')) t(genus,
tree_class)''';
datasource class_fits (fit_class, a: fit_a) grain (fit_class)
query '''select * from (values ('broadleaf', 1.0), ('conifer', 2.0)) t(fit_class, a)''';

auto broadleaf_a <- max(fit_a ? fit_class = 'broadleaf') by *;
auto conifer_a <- max(fit_a ? fit_class = 'conifer') by *;
auto class_a <- case when tree_class = 'broadleaf' then broadleaf_a
    when tree_class = 'conifer' then conifer_a end;
auto species_city_n <- count(tree_id) by city, species;
auto rare <- species_city_n < 2;
"""

CTE = re.compile(r"^(\w+) as (?:MATERIALIZED )?\(", re.MULTILINE)
BODY = re.compile(
    r"^(\w+) as (?:MATERIALIZED )?\((.*?)\)(?=,\n\w+ as |\nSELECT)",
    re.MULTILINE | re.DOTALL,
)

CASES = [
    (
        SPECIES,
        "select tree_id, city, species, pred;",
        [
            ("t1", "bos", "acer rubrum", 15.0),
            ("t2", "bos", "pinus strobus", 40.0),
            ("t3", "bos", "acer rubrum", 45.0),
            ("t4", "bos", "acer rubrum", 60.0),
            ("t5", "sfo", "acer rubrum", 75.0),
        ],
    ),
    (
        GENUS,
        "select tree_id, class_a;",
        [("t1", 1.0), ("t2", 2.0), ("t3", 1.0), ("t4", 1.0)],
    ),
    (
        GENUS,
        "select tree_id, class_a, rare;",
        [("t1", 1.0, True), ("t2", 2.0, True), ("t3", 1.0, True), ("t4", 1.0, True)],
    ),
    (
        GENUS,
        "select tree_id, city, species, genus, class_a, rare;",
        [
            ("t1", "bos", "acer rubrum", "acer", 1.0, True),
            ("t2", "bos", "pinus strobus", "pinus", 2.0, True),
            ("t3", "sfo", "acer rubrum", "acer", 1.0, True),
            ("t4", "bos", "acer saccharum", "acer", 1.0, True),
        ],
    ),
]


def _widest_cte(executor, sql: str) -> int:
    head = sql[: sql.rindex("\nSELECT")]
    return max(
        executor.execute_raw_sql(f"{head}\nSELECT count(*) FROM {name}").fetchone()[0]
        for name in CTE.findall(sql)
    )


def _float(v):
    return float(v) if v is not None and not isinstance(v, (str, bool)) else v


@pytest.mark.parametrize("model,query,expected", CASES)
def test_coarse_value_is_joined_back_on_its_grain(model, query, expected) -> None:
    executor = executor_for(model)
    sql = executor.generate_sql(query)[-1]
    rows = sorted(tuple(_float(v) for v in r) for r in fetch_rows(executor, query))
    assert rows == expected, sql
    assert _widest_cte(executor, sql) <= len(expected), sql


def test_species_value_reads_one_row_per_species() -> None:
    executor = executor_for(SPECIES)
    sql = executor.generate_sql("select tree_id, city, species, pred;")[-1]
    head = sql[: sql.rindex("\nSELECT")]
    computed = [
        name
        for name, body in BODY.findall(sql)
        if '"class_a"' in body.split("FROM")[0] and '"tree_id"' not in body
    ]
    assert computed, sql
    for name in computed:
        rows = executor.execute_raw_sql(f"{head}\nSELECT count(*) FROM {name}")
        assert rows.fetchone()[0] == 2, sql


def test_genus_value_group_emits_its_input_grain() -> None:
    info, _ = plan(GENUS, "select tree_id, class_a;")
    groups = [
        a
        for a in info.group_attrs.values()
        if a.derivation == Derivation.BASIC and "local.class_a" in a.primary_members
    ]
    assert groups and all(a.emitted_grain == frozenset({"local.genus"}) for a in groups)


def _attrs(**kwargs) -> GroupAttrs:
    return GroupAttrs(depth_label=DepthLabel.STAR, **kwargs)


def test_emitted_grain_holds_the_keys_a_row_stream_is_stitched_on() -> None:
    row = GroupInputContract("root", "g", channel=InputChannel.ROW_STREAM)
    exists = GroupInputContract("root", "g", channel=InputChannel.EXISTENCE)
    stitched = _attrs(
        derivation=Derivation.BASIC,
        grain_components=frozenset({"local.genus"}),
        input_grain=frozenset({"local.genus"}),
        input_contracts=(row,),
    )
    assert stitched.emitted_grain == frozenset({"local.genus"})
    probed = _attrs(
        derivation=Derivation.BASIC,
        grain_components=frozenset({"local.genus"}),
        input_grain=frozenset({"local.genus"}),
        input_contracts=(exists,),
    )
    assert probed.emitted_grain == frozenset()


def test_root_pairs_on_a_key_its_rows_determine() -> None:
    _, environment = plan(GENUS, "select tree_id, class_a;")
    tree_id = environment.concepts["local.tree_id"]
    city = environment.concepts["local.city"]
    candidates = frozenset({"local.genus", "local.fit_class"})
    assert _relevant_carry_keys(
        environment, [tree_id], candidates, frozenset()
    ) == frozenset({"local.genus"})
    assert _relevant_carry_keys(environment, [city], candidates, frozenset()) == (
        frozenset()
    )


def test_root_sources_the_keys_its_readers_ask() -> None:
    _, environment = plan(GENUS, "select tree_id, class_a;")
    asked = GroupInputContract(
        "root", "reader", required_grain=frozenset({"local.genus", "local.fit_class"})
    )
    attrs = {
        "root": _attrs(derivation=Derivation.ROOT, output_concepts=("local.tree_id",)),
        "reader": _attrs(derivation=Derivation.BASIC, input_contracts=(asked,)),
    }
    _source_asked_root_keys(attrs, environment)
    assert attrs["root"].output_concepts == ("local.tree_id", "local.genus")
    attrs["root"] = _attrs(
        derivation=Derivation.ROOT,
        output_concepts=("local.tree_id",),
        extent_spans=frozenset({"local.tree_id"}),
    )
    _source_asked_root_keys(attrs, environment)
    assert attrs["root"].output_concepts == ("local.tree_id",)
