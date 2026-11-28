from dataclasses import dataclass

import pytest

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.utility import append_new, assume_unique, unique


@dataclass
class Item:
    address: str
    tag: int = 0


class Bare:
    pass


class Raises:
    @property
    def address(self) -> str:
        raise ValueError("boom")


def test_unique_keeps_first_holder_in_order():
    items = [Item("a", 1), Item("b", 2), Item("a", 3), Item("c", 4), Item("b", 5)]
    assert [i.tag for i in unique(items, "address")] == [1, 2, 4]
    assert [i.tag for i in unique(tuple(items), "address")] == [1, 2, 4]  # type: ignore[arg-type]
    assert [i.tag for i in unique(items, lambda i: i.tag % 2)] == [1, 2]
    assert unique([], "address") == []


def test_unique_missing_attribute_shares_default_key():
    bare = Bare()
    items = [bare, Item(DEFAULT_NAMESPACE), Bare()]
    assert unique(items, "address") == [bare]
    assert unique(tuple(items), "address") == [bare]  # type: ignore[arg-type]


def test_unique_propagates_attribute_errors():
    with pytest.raises(ValueError, match="boom"):
        unique([Raises()], "address")


def test_append_new_skips_held_key():
    items = unique([Item("a", 1)], "address")
    assert append_new(items, Item("b", 2))
    assert not append_new(items, Item("a", 3))
    assert [i.tag for i in items] == [1, 2]


def test_unique_list_is_a_plain_list_at_runtime():
    items = [Item("a")]
    assert assume_unique(items) is items
    assert type(unique(items, "address")) is list
