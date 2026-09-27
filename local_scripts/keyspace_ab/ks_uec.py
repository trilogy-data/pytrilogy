"""pytest plugin for sweeping `_uncovered_exposing_output_contributor`.

`KS_UEC=<file>`: append a JSONL line per firing, tagged with the current test.
`KS_UEC_OFF=1`: the rule answers False (the FINAL restatement is never added).
Both may be set; the firing is logged before it is suppressed."""

import json
import os

from trilogy.core.processing.v4_helper import condition_placement

_PATH = os.environ.get("KS_UEC")
_OFF = os.environ.get("KS_UEC_OFF") == "1"
_original = condition_placement._uncovered_exposing_output_contributor


def _swept(chosen_groups, row_inputs, *args, **kwargs):
    fired = _original(chosen_groups, row_inputs, *args, **kwargs)
    if fired and _PATH:
        test = os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0]
        with open(_PATH, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(
                json.dumps(
                    {
                        "test": test,
                        "hosts": sorted(chosen_groups),
                        "inputs": sorted(row_inputs),
                    }
                )
                + "\n"
            )
    return fired and not _OFF


if _PATH or _OFF:
    condition_placement._uncovered_exposing_output_contributor = _swept
