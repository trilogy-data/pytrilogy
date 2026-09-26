"""pytest plugin: append a JSONL line to $KS_PFB whenever
_preserved_final_branch returns True or region_domains logs "keeps its padded
plan", tagged with the current test."""

import json
import logging
import os

from trilogy.core.processing.v4_helper import condition_placement, region_domains

_PATH = os.environ.get("KS_PFB")


def _record(kind: str, detail: str) -> None:
    test = os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0]
    with open(_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"test": test, "kind": kind, "detail": detail}) + "\n")


_orig = condition_placement._preserved_final_branch


def _wrapped(chosen_groups, row_inputs, buckets, group_graph, mandatory_addrs):
    result = _orig(chosen_groups, row_inputs, buckets, group_graph, mandatory_addrs)
    if result:
        _record("pfb", f"{sorted(row_inputs)} on {list(chosen_groups)}")
    return result


class _Catch(logging.Handler):
    def emit(self, record):
        msg = record.getMessage()
        if "keeps its padded plan" in msg:
            _record("emit", msg)


if _PATH:
    condition_placement._preserved_final_branch = _wrapped
    region_domains.logger.addHandler(_Catch())
    region_domains.logger.setLevel(logging.INFO)
