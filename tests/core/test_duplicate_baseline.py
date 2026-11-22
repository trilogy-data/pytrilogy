import gc
import weakref

import pytest

from trilogy import Dialects, Environment
from trilogy.core import query_processor
from trilogy.core.models.build import Factory
from trilogy.core.models.datasource import Address
from trilogy.core.models.reference_index import dependents, strings_in

MODEL = """
key id int;
property id.name string;
property id.zone int;
constant active_zone <- 7;
auto in_zone <- case when zone = active_zone then 'in' else 'out' end;
auto name_length <- len(name);
datasource things (id: id, name: name, zone: zone) grain (id) address things;
"""
QUERY = "select in_zone, name_length, count(id) as n;"
REBINDING = "parameter active_zone int;" + QUERY


def _env() -> Environment:
    env = Environment()
    env.parse(MODEL)
    return env


def _sql(env: Environment, text: str, **parameters: int) -> str:
    env.set_parameters(**parameters)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        return executor.generate_sql(text)[-1]
    finally:
        executor.close()


def _alone(env: Environment) -> Environment:
    """A duplicate that builds everything itself."""
    duplicate = env.duplicate()
    duplicate._origin = None
    return duplicate


@pytest.fixture
def recorded(monkeypatch) -> list[Environment]:
    """Every environment materialized from nothing."""
    built: list[Environment] = []
    full = Factory.build_environment_recorded

    def record(self, base):
        built.append(base)
        return full(self, base)

    monkeypatch.setattr(Factory, "build_environment_recorded", record)
    return built


def test_duplicates_build_their_parent_once(recorded):
    env = _env()
    expected = _sql(_alone(env), QUERY)
    recorded.clear()
    assert [_sql(env.duplicate(), QUERY) for _ in range(3)] == [expected] * 3
    assert recorded == [env]


def test_a_rebound_concept_rebuilds_what_reads_it(recorded):
    env = _env()
    expected = _sql(_alone(env), REBINDING, active_zone=3)
    recorded.clear()
    duplicate = env.duplicate()
    assert _sql(duplicate, REBINDING, active_zone=3) == expected
    assert recorded == [env]
    assert duplicate.origin_changes() == (env, {"local.active_zone"})
    assert dependents(env, duplicate, {"local.active_zone"}, ()) == {
        "local.active_zone",
        "local.in_zone",
    }


def test_a_duplicate_reused_across_statements_keeps_answering(recorded):
    env = _env()
    alone = _alone(env)
    duplicate = env.duplicate()
    for zone in (3, 5):
        assert _sql(duplicate, REBINDING, active_zone=zone) == _sql(
            alone, REBINDING, active_zone=zone
        )


def test_a_concept_edited_in_place_counts_as_changed():
    env = _env()
    duplicate = env.duplicate()
    duplicate.concepts["name_length"].keys = {"local.name"}
    assert duplicate.origin_changes() == (env, {"local.name_length"})
    assert _sql(duplicate, QUERY) == _sql(_alone(duplicate), QUERY)


@pytest.mark.parametrize(
    "edit",
    [
        lambda parent, duplicate: parent.parse("key other int;"),
        lambda parent, duplicate: parent.concepts["name"].pseudonyms.add("local.x"),
        lambda parent, duplicate: setattr(
            duplicate.datasources["things"], "address", Address(location="moved")
        ),
        lambda parent, duplicate: setattr(
            duplicate.datasources["things"].columns[0], "modifiers", []
        ),
        lambda parent, duplicate: duplicate.parse(
            "datasource more (id: id) grain (id) address more;"
        ),
    ],
)
def test_a_moved_parent_or_datasource_is_built_in_full(edit, recorded):
    env = _env()
    duplicate = env.duplicate()
    edit(env, duplicate)
    assert duplicate.origin_changes() is None
    expected = _sql(_alone(duplicate), QUERY)
    recorded.clear()
    assert _sql(duplicate, QUERY) == expected
    assert recorded == [duplicate]


def test_an_inherited_bundle_leaves_the_parents_alone():
    env = _env()
    _sql(env.duplicate(), REBINDING, active_zone=3)
    held = query_processor._session_build_caches(env, None)
    assert "local.n" not in held.env_baselines[()].end_locals
    assert _sql(env, QUERY) == _sql(_alone(env), QUERY)


def test_dependents_follow_readers_and_join_partners():
    env = _env()
    assert dependents(env, env, {"local.zone"}, ()) == {
        "local.zone",
        "local.in_zone",
        "things",
    }
    joined = dependents(env, env, {"local.name"}, [("local.name", "local.zone", None)])
    assert {"local.zone", "local.in_zone", "local.name_length"} <= joined


def test_strings_in_reads_every_container():
    assert strings_in({"a": [("b", {"c"})], 1: frozenset({"d"})}) == {
        "a",
        "b",
        "c",
        "d",
    }


def test_a_planned_environment_is_freed_without_the_collector():
    """Nothing a statement leaves behind may hold its environment in a cycle:
    the session caches hang off it, and would wait for the collector too."""
    gc.collect()
    gc.disable()
    try:
        env = _env()
        _sql(env, "select in_zone, count(id) as n where name_length > 2 or zone = 1;")
        dead = weakref.ref(env)
        del env
        assert dead() is None
    finally:
        gc.enable()


class _Slotted:
    __slots__ = ("held", "unset")

    def __init__(self) -> None:
        self.held = ["inside"]


def test_strings_in_reads_slots():
    assert strings_in(_Slotted()) == {"inside"}


def test_a_mostly_dirty_duplicate_is_built_in_full(recorded, monkeypatch):
    env = _env()
    everything = set(env.concepts.data)
    monkeypatch.setattr(query_processor, "dependents", lambda *args: everything)
    duplicate = env.duplicate()
    expected = _sql(_alone(env), REBINDING, active_zone=3)
    recorded.clear()
    assert _sql(duplicate, REBINDING, active_zone=3) == expected
    assert recorded == [env, duplicate]


def test_a_parent_planning_on_another_thread_is_not_read(recorded, monkeypatch):
    def busy(self, dirty):
        raise RuntimeError("dictionary changed size during iteration")

    env = _env()
    monkeypatch.setattr(query_processor.BuildCaches, "fork", busy)
    duplicate = env.duplicate()
    assert _sql(duplicate, QUERY) == _sql(_alone(env), QUERY)
    assert duplicate in recorded


MERGED = MODEL + """
key other_id int;
datasource others (other_id: other_id) grain (other_id) address others;
merge other_id into id;
"""


def test_a_rebind_beside_a_merged_concept_answers_the_same(recorded):
    env = Environment()
    env.parse(MERGED)
    expected = _sql(_alone(env), REBINDING, active_zone=3)
    assert _sql(env.duplicate(), REBINDING, active_zone=3) == expected


def test_a_duplicate_of_a_duplicate_inherits_through_both(recorded):
    env = _env()
    expected = _sql(_alone(env), REBINDING, active_zone=3)
    recorded.clear()
    assert _sql(env.duplicate().duplicate(), REBINDING, active_zone=3) == expected


def test_a_rebind_beside_an_alias_origin_is_built_in_full(recorded):
    env = _env()
    env.alias_origin_lookup["local.name_length"] = env.concepts["name_length"]
    duplicate = env.duplicate()
    expected = _sql(_alone(env), REBINDING, active_zone=3)
    recorded.clear()
    assert _sql(duplicate, REBINDING, active_zone=3) == expected
    assert recorded == [duplicate]
