"""Symbol collection predicts each concept's address lexically, before the
parents it names exist; hydration derives the same address from resolved
parents. The two must agree — a symbol declared at an address no concept
occupies lets `_scoped_placeholder` manufacture a dangling concept for a name
that does not exist."""

from __future__ import annotations

import pytest

from trilogy.core.models.environment import Environment
from trilogy.parsing.v2 import symbols
from trilogy.parsing.v2.model import HydrationError

MODEL = """key sold_date.id int;
property sold_date.id.year int;
property <sold_date.id>.month int;
key order_id int;
property order_id.amount float;
property order_id.doubled <- amount * 2;
"""


@pytest.mark.parametrize(
    "path,file_namespace,expected",
    [
        ("col_id.year", "local", "local.year"),
        ("sold_date.id.year", "local", "sold_date.year"),
        ("a.b.c.d", "local", "a.b.d"),
        ("year", "local", "local.year"),
        ("col_id.year", "mod", "mod.year"),
    ],
)
def test_property_address(path, file_namespace, expected):
    env = Environment(namespace=file_namespace)
    assert symbols.property_address(path, env) == expected


def test_declared_symbols_match_hydrated_concepts():
    """The dotted form takes its parent key's namespace; the `<...>` grain form
    stays in the declaring file's namespace (see concept_rules.concept_derivation
    — pushing a `<...>` concept into an imported namespace orphans it across
    import boundaries). Both are enforced by verify_symbol_addresses."""
    env = Environment()
    env.parse(MODEL)
    for address in [
        "sold_date.year",
        "local.month",
        "local.amount",
        "local.doubled",
    ]:
        assert address in env.concepts
    assert "sold_date.month" not in env.concepts


def test_address_drift_raises_at_the_drift(monkeypatch):
    monkeypatch.setattr(
        symbols,
        "property_address",
        lambda path, environment: f"local.{path.rsplit('.', 1)[-1]}",
    )
    env = Environment()
    with pytest.raises(HydrationError, match="Concept address drift"):
        env.parse("key sold_date.id int;\nproperty sold_date.id.year int;\n")
