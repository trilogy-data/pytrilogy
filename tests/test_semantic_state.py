"""Coverage for parser-owned semantic-state facades (TypeLookup) and small
ConceptLookup edge cases."""

from __future__ import annotations

import pytest

from trilogy.core.models.author import CustomType
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.semantic_state import (
    ConceptLookup,
    SemanticState,
    TypeLookup,
)


def _state() -> SemanticState:
    return SemanticState(environment=Environment())


def test_type_lookup_get_returns_none_for_missing():
    assert TypeLookup(_state()).get("missing_type") is None


def test_type_lookup_contains_false_for_missing():
    assert "missing_type" not in TypeLookup(_state())


def test_type_lookup_require_raises_keyerror():
    with pytest.raises(KeyError):
        TypeLookup(_state()).require("missing_type")


def test_type_lookup_getitem_raises_keyerror():
    with pytest.raises(KeyError):
        TypeLookup(_state())["missing_type"]


def test_type_lookup_non_string_membership_returns_false():
    """__contains__ short-circuits to False for non-string keys."""
    assert (123 in TypeLookup(_state())) is False


def test_type_lookup_resolves_pending_type_from_state():
    state = _state()
    ct = CustomType(name="email", type=DataType.STRING)
    state.add_type(ct)
    lookup = TypeLookup(state)
    assert lookup.get("email") is ct
    assert "email" in lookup
    assert lookup.require("email") is ct


def test_type_lookup_falls_back_to_environment_data_types():
    env = Environment()
    ct = CustomType(name="zipcode", type=DataType.STRING)
    env.data_types["zipcode"] = ct
    lookup = TypeLookup(SemanticState(environment=env))
    assert lookup.get("zipcode") is ct
    assert "zipcode" in lookup


def test_concept_lookup_non_string_membership_returns_false():
    lookup = ConceptLookup(_state())
    assert (123 in lookup) is False


def test_concept_lookup_missing_get_returns_none_or_placeholder():
    """`get` returns a deferred placeholder (or None) for missing addresses."""
    lookup = ConceptLookup(_state())
    out = lookup.get("local.does_not_exist")
    # Either resolves to a placeholder or returns None — both are valid.
    if out is not None:
        assert hasattr(out, "address")
