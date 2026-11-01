"""`formal/Trilogy/Binding.lean`: each datasource binding is a functional
dependency from the table's row determinants (its grain, plus a discriminator
it binds) to every other column it binds, and a grainless table determines
nothing (`Binding.grainless_determines_nothing`). The closure over every
table's bindings is sound when they all hold (`Binding.closure_sound`). Each
case builds a random model of keys, properties and tables, some of them
`complete where` arms or grainless, and holds the planner's FD readers to the
closure and `row_keys` computed here with the Lean definitions."""

import random
from dataclasses import dataclass, field

import pytest

from trilogy import Environment
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_closure,
    row_keys,
)

KEYS = [f"k{i}" for i in range(5)]
SEEDS = range(150)


@dataclass
class Table:
    name: str
    grain: list[str]
    bound: list[str]
    disc: list[str] = field(default_factory=list)

    @property
    def determinants(self) -> frozenset[str]:
        if not self.grain:
            return frozenset()
        return frozenset(self.grain) | (frozenset(self.disc) & frozenset(self.bound))

    def fds(self) -> list[tuple[frozenset[str], str]]:
        if not self.grain:
            return []
        det = self.determinants
        return [(det, c) for c in self.bound if c not in det]

    def render(self) -> str:
        columns = ", ".join(self.bound)
        grain = f" grain ({', '.join(self.grain)})" if self.grain else ""
        if self.disc:
            arm = " and ".join(f"{d} = 1" for d in self.disc)
            return (
                f"partial datasource {self.name} ({columns}){grain}"
                f" complete where {arm} address {self.name};"
            )
        return f"datasource {self.name} ({columns}){grain} address {self.name};"


@dataclass
class Model:
    properties: dict[str, str]
    tables: list[Table]

    def render(self) -> str:
        lines = [f"key {k} int;" for k in KEYS]
        lines += [f"property {key}.{p} int;" for p, key in self.properties.items()]
        lines += [t.render() for t in self.tables]
        return "\n".join(lines)

    def fds(self) -> list[tuple[frozenset[str], str]]:
        out = [fd for t in self.tables for fd in t.fds()]
        return out + [(frozenset({key}), p) for p, key in self.properties.items()]

    def closure(self, seed: set[str]) -> set[str]:
        closure = set(seed)
        changed = True
        while changed:
            changed = False
            for det, c in self.fds():
                if c not in closure and det <= closure:
                    closure.add(c)
                    changed = True
        return closure

    def bound_on(self, column: str) -> set[str]:
        return {d for t in self.tables for det, c in t.fds() if c == column for d in det}

    @property
    def addresses(self) -> list[str]:
        return KEYS + list(self.properties)


def _model(seed: int) -> Model:
    rng = random.Random(seed)
    properties = {f"p{i}": rng.choice(KEYS) for i in range(rng.randint(0, 3))}
    tables = []
    for i in range(rng.randint(1, 4)):
        grain = rng.sample(KEYS, rng.choice([0, 1, 1, 1, 2]))
        others = [k for k in KEYS if k not in grain]
        bound = grain + rng.sample(others, rng.randint(1, len(others)))
        bound += [p for p, key in properties.items() if key in bound and rng.random() < 0.5]
        candidates = [k for k in bound if k in KEYS and k not in grain]
        disc = [rng.choice(candidates)] if grain and candidates and rng.random() < 0.3 else []
        tables.append(Table(f"t{i}", grain, bound, disc))
    return Model(properties, tables)


def _build(model: Model) -> tuple[Environment, BuildEnvironment]:
    env = Environment()
    env.parse(model.render())
    return env, env.materialize_for_select()


def _local(addresses) -> set[str]:
    return {a.removeprefix("local.") for a in addresses if a.startswith("local.")} - {
        "_env_working_path"
    }


def _seeds(model: Model, seed: int) -> list[set[str]]:
    rng = random.Random(seed * 7919)
    out = [set(t.grain) for t in model.tables if t.grain]
    out += [set(rng.sample(KEYS, rng.randint(1, 2))) for _ in range(3)]
    return out


@pytest.mark.parametrize("seed", SEEDS)
def test_build_fd_closure_is_the_binding_closure(seed: int):
    model = _model(seed)
    env, build = _build(model)
    for start in _seeds(model, seed):
        addresses = {f"local.{a}" for a in start}
        expected = model.closure(start)
        for empty in (False, True):
            got = _local(build_fd_closure(build, addresses, include_empty_grain=empty))
            assert got == expected, (model.render(), start, empty)
        assert _local(env.binding_closure(addresses)) == expected, (
            model.render(),
            start,
        )


@pytest.mark.parametrize("seed", SEEDS)
def test_row_keys_are_the_binding_determinants(seed: int):
    model = _model(seed)
    env, build = _build(model)
    for key in KEYS:
        expected = model.bound_on(key)
        assert _local(row_keys(build.concepts[f"local.{key}"], build)) == expected, (
            model.render(),
            key,
        )
        assert _local(env.bound_on(f"local.{key}")) == expected, (model.render(), key)
    for prop, key in model.properties.items():
        assert _local(row_keys(build.concepts[f"local.{prop}"], build)) == {key}
