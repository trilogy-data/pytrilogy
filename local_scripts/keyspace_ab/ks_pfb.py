"""pytest plugin: append a JSONL line to $KS_PFB whenever region_domains logs
"keeps its padded plan", tagged with the current test. (It also traced
`_preserved_final_branch` until that rule was retired; git history has the
wrapper, and `ks_nopfb.py` beside it, for sweeping another rule the same way.)"""

import json
import logging
import os

from trilogy.core.processing.v4_helper import region_domains

_PATH = os.environ.get("KS_PFB")


def _record(kind: str, detail: str) -> None:
    test = os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0]
    with open(_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"test": test, "kind": kind, "detail": detail}) + "\n")


class _Catch(logging.Handler):
    def emit(self, record):
        msg = record.getMessage()
        if "keeps its padded plan" in msg:
            _record("emit", msg)


if _PATH:
    region_domains.logger.addHandler(_Catch())
    region_domains.logger.setLevel(logging.INFO)
