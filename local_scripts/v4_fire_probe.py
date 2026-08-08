"""pytest plugin: record every firing of the d1-scope condition-placement rule.

Usage: pytest -p local_scripts.v4_fire_probe <targets>
Writes one line per firing to local_scripts/v4_size/fire.log, so "this change is
inert outside the shapes it targets" is a measurement rather than a claim.
"""

from __future__ import annotations

from pathlib import Path

from trilogy.core.processing.v4_helper import condition_placement as cp

LOG = Path("local_scripts/v4_size/fire.log")
_real = cp._nested_scope_swallows_atom


def _patched(atom, row_inputs, candidates, nested_ids, buckets, environment):
    out = _real(atom, row_inputs, candidates, nested_ids, buckets, environment)
    if out:
        with LOG.open("a", encoding="utf-8") as handle:
            handle.write(f"{str(atom)[:120]}\t{sorted(row_inputs)}\n")
    return out


cp._nested_scope_swallows_atom = _patched
