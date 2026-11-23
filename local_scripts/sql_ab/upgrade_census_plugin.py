"""pytest plugin: record every `UpgradeOuterFromKeySetEquivalence` firing with
the proofs that held while it was decided.

    UPGRADE_CENSUS=/tmp/up.jsonl PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p upgrade_census_plugin tests ...

Each line: test, method (`inner` / `narrow` / `narrow_veto`), from -> to, the
join's lefts, keys and place in its CTE (a multi-left or a later join is one
the planner's whole-ON proof skips), and the proof helpers that returned True
during the decision (`_complete_values` reports the branch that answered at
the top of its recursion).
"""

from __future__ import annotations

import json
import os

import pytest

_current: list[str] = [""]
_proofs: list[list[str]] = []

_TRACKED = (
    "_pair_key_sets_equivalent",
    "_proven_subset_of",
    "_genuine_partial_stamp",
    "_rowset_definition_boundary",
    "_on_complete_grain",
    "_authoritative_scan",
    "_same_filter_chain",
)


def _tracking(module, name: str):
    original = getattr(module, name)

    def wrapper(*args, **kwargs):
        result = original(*args, **kwargs)
        if result and _proofs:
            _proofs[-1].append(name)
        return result

    setattr(module, name, wrapper)


def pytest_configure(config) -> None:
    out = os.environ.get("UPGRADE_CENSUS")
    if not out:
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    path = f"{out}.{worker}.jsonl" if worker else out
    from trilogy.core.optimizations import value_set_join_upgrade as m

    for name in _TRACKED:
        _tracking(m, name)
    rule = m.UpgradeOuterFromKeySetEquivalence

    def recorded(method_name: str, label):
        original = getattr(rule, method_name)

        def wrapper(self, cte, join, right_cte, *args, **kwargs):
            before = join.join_type
            _proofs.append([])
            try:
                result = original(self, cte, join, right_cte, *args, **kwargs)
            finally:
                proofs = _proofs.pop()
            if result:
                with open(path, "a") as fh:
                    fh.write(
                        json.dumps(
                            {
                                "test": _current[0],
                                "method": label(args, kwargs),
                                "from": before.value,
                                "to": join.join_type.value,
                                "proofs": sorted(set(proofs)),
                                "left": cte.name,
                                "right": right_cte.name,
                                "lefts": sorted({p.node.name for p in join.pairs}),
                                "keys": sorted({p.left.address for p in join.pairs}),
                                "position": cte.joins.index(join),
                                "joins": len(cte.joins),
                            }
                        )
                        + "\n"
                    )
            return result

        setattr(rule, method_name, wrapper)

    recorded("_upgrade_to_inner", lambda a, k: "inner")
    recorded(
        "_narrow_directionally",
        lambda a, k: "narrow_veto" if k.get("graph_proof_only") else "narrow",
    )


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item) -> None:
    _current[0] = item.nodeid
