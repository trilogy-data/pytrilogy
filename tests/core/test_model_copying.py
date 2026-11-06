import copy

from trilogy import Environment
from trilogy.core.models.author import Concept
from trilogy.core.models.copying import dataclass_deepcopy

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
