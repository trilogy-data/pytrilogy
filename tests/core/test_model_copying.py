import copy
from dataclasses import dataclass

from trilogy import Environment
from trilogy.core.models.author import Concept
from trilogy.core.models.copying import (
    _copies_itself,
    _copy_value,
    dataclass_deepcopy,
)

MODEL = """
key id int;
property id.name string;
property id.tags list<string>;
auto name_length <- len(name);
datasource things (id: id, name: name, tags: tags) grain (id) address things;
"""


def _env() -> Environment:
    env = Environment()
    env.parse(MODEL)
    return env


def test_model_dataclasses_use_the_direct_copy():
    assert Concept.__deepcopy__ is dataclass_deepcopy


def test_copy_matches_the_generic_deepcopy():
    concept = _env().concepts["name_length"]
    copied = copy.deepcopy(concept)
    assert copied is not concept
    assert copied == concept
    assert copied.lineage is not concept.lineage
    assert copied.lineage == concept.lineage


def test_shared_structure_stays_shared_within_one_copy():
    concept = _env().concepts["name"]
    pair = copy.deepcopy([concept, concept])
    assert pair[0] is pair[1]


def test_duplicate_is_independent_of_its_source():
    env = _env()
    duplicate = env.duplicate()
    duplicate.concepts["name"].keys.add("local.other")
    duplicate.datasources["things"].columns.pop()
    assert "local.other" not in (env.concepts["name"].keys or set())
    assert len(env.datasources["things"].columns) == 3
    assert duplicate.datasources["things"].columns[0].concept.address == "local.id"


@dataclass
class _Pair:
    left: list
    right: list


def test_shared_lists_stay_shared():
    shared = ["a"]
    copied = dataclass_deepcopy(_Pair(left=shared, right=shared), {})
    assert copied.left is copied.right
    assert copied.left is not shared


@dataclass(slots=True)
class _Slotted:
    value: int


def test_slotted_dataclasses_keep_the_generic_copy():
    assert _copies_itself(_Slotted)


def test_enums_and_tuples_of_atoms_are_shared_not_copied():
    concept = _env().concepts["name"]
    copied = copy.deepcopy(concept)
    assert copied.purpose is concept.purpose
    atoms = ("a", 1, None, concept.purpose)
    assert _copy_value(atoms, {}) is atoms


def test_a_tuple_holding_a_model_object_is_copied():
    concept = _env().concepts["name"]
    held = (concept, "a")
    copied = _copy_value(held, {})
    assert copied is not held
    assert copied[0] is not concept
    assert copied[0] == concept


def test_one_object_reached_twice_is_copied_once():
    concept = _env().concepts["name"]
    memo: dict = {}
    assert _copy_value(concept, memo) is _copy_value(concept, memo)
