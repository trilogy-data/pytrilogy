"""pytest plugin: append every compiled statement's SQL to $KS_SQLDUMP as JSONL."""

import json
import os

from trilogy.dialect.base import BaseDialect

_PATH = os.environ.get("KS_SQLDUMP")
_original = BaseDialect.compile_statement


def _dumping(self, *args, **kwargs):
    sql = _original(self, *args, **kwargs)
    test = os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0]
    with open(_PATH, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"test": test, "sql": sql}) + "\n")
    return sql


if _PATH:
    BaseDialect.compile_statement = _dumping
