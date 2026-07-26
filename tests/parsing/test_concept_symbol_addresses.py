"""A property declared `<parent_key_path>.<name>` takes its PARENT KEY's
namespace, so its address is not knowable until the parent's is. Concept
addresses are resolved parents-before-children at COLLECT_SYMBOLS, reading each
parent's real namespace rather than predicting one from the written path."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.environment import Environment


def addresses(source: str) -> set[str]:
    env = Environment()
    env.parse(source)
    return {k for k in env.concepts if "__preql_internal" not in k and "_env_" not in k}


def test_dotted_property_takes_parent_namespace():
    assert addresses("key sold_date.id int;\nproperty sold_date.id.year int;") == {
        "sold_date.id",
        "sold_date.year",
    }


def test_local_parent_keeps_file_namespace():
    assert addresses("key col_id int;\nproperty col_id.bytes_in int;") == {
        "local.col_id",
        "local.bytes_in",
    }


def test_parent_declared_after_the_property():
    """Source order must not decide the address — the parent resolves first
    regardless of where it is written."""
    assert addresses("property sold_date.id.year int;\nkey sold_date.id int;") == {
        "sold_date.id",
        "sold_date.year",
    }


def test_property_chained_onto_a_property_parent():
    assert addresses("key a.b int;\nproperty a.b.c int;\nproperty a.c.d int;") == {
        "a.b",
        "a.c",
        "a.d",
    }


def test_grain_tuple_form_stays_in_the_file_namespace():
    """The `<...>` form can hold keys from several namespaces, so it has no
    single parent to inherit from — it names into the declaring file instead.
    See concept_rules.concept_derivation."""
    assert addresses(
        "key sold_date.id int;\nkey item.id int;\n"
        "property <sold_date.id>.month int;\n"
        "property <sold_date.id, item.id>.price float;"
    ) == {"sold_date.id", "item.id", "local.month", "local.price"}


def test_missing_parent_raises():
    env = Environment()
    with pytest.raises(UndefinedConceptException, match="nope.missing"):
        env.parse("property nope.missing.thing int;")


def test_property_on_imported_key_lands_in_the_imported_namespace(tmp_path):
    (tmp_path / "dim.preql").write_text("key col_id int;\n", encoding="utf-8")
    env = Environment(working_path=tmp_path)
    env.parse("import dim as dim;\nproperty dim.col_id.bytes_in int;")
    assert env.concepts["dim.bytes_in"].keys == {"dim.col_id"}


def test_key_shorthand_sibling_property_still_resolves():
    """`property x.part_1 string` declares `local.part_1` keyed on `local.x`;
    `x.part_1` is a shorthand for that sibling, not a concept of its own."""
    env = Environment()
    env.parse(
        "key x int;\nproperty x.part_1 string;\nproperty x.part_2 string;\n"
        "auto joined <- concat(x.part_1, x.part_2);"
    )
    assert env.concepts["local.joined"].address == "local.joined"
    assert env.concepts["local.part_1"].keys == {"local.x"}


def test_derived_parent_symbol_matches_the_built_concept():
    """`order.date.month` only exists once hydration derives it off the date
    property, so its namespace is unknowable at symbol collection. The address
    is taken from the built concept rather than predicted — a predicted
    `order.date.tag` would be a symbol no concept occupies."""
    env = Environment()
    env.parse(
        "key order.id int;\nproperty order.id.date date;\n"
        "property order.date.month.tag string;"
    )
    assert "order.tag" in env.concepts
    assert "order.date.tag" not in env.concepts
