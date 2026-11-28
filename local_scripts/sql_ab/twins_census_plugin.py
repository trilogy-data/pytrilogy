"""pytest plugin: for every `UpgradeOuterFromKeySetEquivalence` equivalence
upgrade, the planner's twin proof (`join_resolution._twins`) per key at the
merge the joined CTEs came from, and the first test of it that failed.

    TWINS_CENSUS=/tmp/tw.jsonl PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p twins_census_plugin tests ...

Each merge's `_covered_keys` call is recorded with a verdict per (side, side,
key): `ok`, or the first test of `_one_read` that failed and then the first
of `_twins`' fallback, with `no_providers[...]` naming the joins that dropped
the last provider. A firing is matched to the latest merge whose sides are its
joining CTE's planned sources, each CTE to the side sharing most of its
columns; `witness` names the left an earlier INNER join equated with the key
(`_equated_reads`), whose verdict is reported. `merge: null` means no planned
merge holds the pair (the optimizer built or moved the join).
"""

from __future__ import annotations

import json
import os

import pytest

_current: list[str] = [""]
_merges: list[
    tuple[frozenset[str], dict[str, frozenset[str]], dict[tuple[str, str, str], str]]
] = []


def _one_read_verdict(key, a, b, sides, carries) -> str:
    from trilogy.core.processing import join_resolution as jr

    na, nb = carries.nodes[a], carries.nodes[b]
    if key in sides[a].rollup_padded | sides[b].rollup_padded:
        return "rollup_padded"
    if not jr._same_filters(na, nb):
        return "filters"
    pa, pb = carries.providers(a, key), carries.providers(b, key)
    if not pa or not pb:
        side = na if not pa else nb
        return "no_providers" + str(
            provider_blockers(side, carries.concepts[(a if not pa else b, key)].address)
        )
    twin = pa == pb and jr.same_rows(na, nb)
    va = jr._one_value_set(na, carries.concepts[(a, key)])
    vb = jr._one_value_set(nb, carries.concepts[(b, key)])
    if not twin and not va & vb:
        if not va or not vb:
            return "value_set_empty"
        return "value_sets_disjoint"
    nullable = [key in sides[s].nullables for s in (a, b)]
    if not any(nullable):
        return "ok"
    if not all(nullable):
        return "null_one_side"
    if sides[a].span_padding.get(key) != sides[b].span_padding.get(key):
        return "span_padding"
    if not jr.get_modifiers(
        carries.concepts[(a, key)], carries.concepts[(b, key)], na, nb
    ):
        return "null_modifiers"
    return "ok"


def _verdict(key, a, b, sides, carries, related) -> str:
    from trilogy.core.processing import join_resolution as jr

    if key in sides[b].partials:
        return "partial"
    one = _one_read_verdict(key, a, b, sides, carries)
    if one == "ok":
        return "ok"
    if not jr._null_alike(key, sides[a], sides[b]):
        return f"one_read:{one}|null_alike"
    if not (
        related
        or key not in sides[a].nullables | sides[b].nullables
        or jr.same_rows(carries.nodes[a], carries.nodes[b])
    ):
        return f"one_read:{one}|nullable"
    if not carries(a, key):
        return f"one_read:{one}|carries_a"
    if not carries(b, key):
        return f"one_read:{one}|carries_b"
    return "ok"


def _record(sides, ds_node_map, ds_concept_map, environment) -> None:
    from trilogy.core.processing import join_resolution as jr

    graph = environment.domain_graph
    carries = jr._Carries(ds_node_map, ds_concept_map, graph)
    declared = graph.declared_relation_keys()
    verdicts: dict[tuple[str, str, str], str] = {}
    for sub, sub_facts in sides.items():
        for key in sub_facts.columns:
            sub_concept = ds_concept_map[(sub, key)]
            for sup, sup_facts in sides.items():
                if sup == sub or key not in sup_facts.columns:
                    continue
                sup_concept = ds_concept_map[(sup, key)]
                if key in sub_facts.partials:
                    verdicts[(sub, sup, key)] = "partial"
                elif sub_concept.address != sup_concept.address:
                    verdicts[(sub, sup, key)] = "addresses_differ"
                else:
                    related = graph.canonical(sub_concept.address) in declared
                    verdicts[(sub, sup, key)] = _verdict(
                        key, sub, sup, sides, carries, related
                    )
    names = frozenset(n.removeprefix("ds~") for n in sides)
    outputs = {
        node: frozenset(c.address for c in ds.output_concepts)
        for node, ds in ds_node_map.items()
    }
    _merges.append((names, outputs, verdicts))


def _side_of(cte, outputs: dict[str, frozenset[str]]) -> str | None:
    """The merge side a CTE stands for: the one whose outputs it shares most
    of (the optimizer renames and collapses CTEs, never their columns)."""
    mine = frozenset(c.address for c in cte.output_columns)

    def score(node: str) -> float:
        theirs = outputs[node]
        return len(mine & theirs) / max(1, len(mine | theirs))

    best = max(outputs, key=score, default=None)
    return best if best is not None and score(best) > 0 else None


def _match(cte, left, right):
    ids = {sub.identifier for sub in cte.source.datasources}
    for names, outputs, verdicts in reversed(_merges):
        if names != ids:
            continue
        a, b = _side_of(left, outputs), _side_of(right, outputs)
        if a and b and a != b:
            return a, b, verdicts
        return None
    return None


def pytest_configure(config) -> None:
    out = os.environ.get("TWINS_CENSUS")
    if not out:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    path = f"{out}.{worker}.jsonl" if worker else out
    from trilogy.core.optimizations import value_set_join_upgrade as m
    from trilogy.core.processing import join_resolution as jr

    covered = jr._covered_keys

    def covered_wrapper(sides, ds_node_map, ds_concept_map, environment):
        try:
            _record(sides, ds_node_map, ds_concept_map, environment)
        except Exception as exc:  # the census must not change outcomes
            _merges.append((frozenset(), {}, {("error", "", ""): repr(exc)}))
        return covered(sides, ds_node_map, ds_concept_map, environment)

    jr._covered_keys = covered_wrapper
    original = m.UpgradeOuterFromKeySetEquivalence._upgrade_to_inner

    def upgrade(self, cte, join, right_cte):
        before = join.join_type
        witnesses = [
            next(
                (
                    read.node.name
                    for read in m._equated_reads(cte, join, pair)
                    if self._pair_values_equal(read, right_cte)
                ),
                None,
            )
            for pair in join.pairs
        ]
        result = original(self, cte, join, right_cte)
        if not result:
            return result
        rows = []
        for pair, witness in zip(join.pairs, witnesses):
            entry: dict = {"left": pair.node.name, "key": pair.right.address}
            if witness != pair.node.name:
                entry["witness"] = witness
            reader = pair.node
            if witness is not None and witness != pair.node.name:
                reader = next(
                    (c for c in cte.parent_ctes if c.name == witness), pair.node
                )
            found = _match(cte, reader, right_cte)
            if found is None:
                entry["merge"] = None
            else:
                a, b, verdicts = found
                entry["merge"] = True
                entry["verdicts"] = sorted(
                    {
                        f"{'' if (x, y) == (a, b) else 'rev:'}{k}={v}"
                        for (x, y, k), v in verdicts.items()
                        if {x, y} == {a, b} and pair.right.address in k
                    }
                )
            rows.append(entry)
        with open(path, "a") as fh:
            fh.write(
                json.dumps(
                    {
                        "test": _current[0],
                        "from": before.value,
                        "cte": cte.name,
                        "right": right_cte.name,
                        "pairs": rows,
                        "position": cte.joins.index(join),
                        "joins": len(cte.joins),
                    }
                )
                + "\n"
            )
        return result

    m.UpgradeOuterFromKeySetEquivalence._upgrade_to_inner = upgrade


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item) -> None:
    _current[0] = item.nodeid
    _merges.clear()


def provider_blockers(ds, address: str) -> list[str]:
    """Why `key_providers(ds, address)` is empty: each join that dropped the
    last provider, named by its shape."""
    from trilogy.core.models.execute import Join, QueryDatasource
    from trilogy.core.processing import join_resolution as jr

    if not isinstance(ds, QueryDatasource):
        return []
    if ds.limit is not None:
        return ["limit"]
    if any(c.address == address for c in ds.partial_concepts):
        return ["stamp"]
    preserved = jr.preserved_sources(ds.datasources, ds.joins)
    binding = [
        sub
        for sub in ds.datasources
        if jr.key_providers(sub, address) and sub.identifier not in preserved
    ]
    out: list[str] = []
    for sub in binding:
        for join in ds.joins:
            if not isinstance(join, Join):
                out.append("unnest")
                continue
            keys = sorted({p.right.address for p in join.pairs})
            one_row = all(
                c.granularity.value == "single_row" for c in join.right.output_concepts
            )
            shape = (
                "keyless_one_row"
                if not keys and one_row
                else (
                    "keyless"
                    if not keys
                    else "on_key" if address in keys else "off_key"
                )
            )
            out.append(f"{join.join_type.value}:{shape}")
    if out:
        return sorted(set(out))
    for sub in ds.datasources:
        if not jr.key_providers(sub, address):
            found = provider_blockers(sub, address)
            if found:
                return found
    return ["none_bound"]
