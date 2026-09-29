"""Compile every SELECT of a corpus of .preql files; write {file#index: sql}.

    python local_scripts/sql_ab/corpus_sql.py <out.json> "tests/modeling/**/*.preql" ...

Run from the root of the tree under test: cwd goes first on sys.path, so a
detached worktree compiles with its own planner. See README.md.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.getcwd())

from trilogy import Environment
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import (
    MultiSelectStatement,
    SelectStatement,
)
from trilogy.dialect.enums import Dialects


def compile_file(path: Path) -> dict[str, str]:
    renderer = Dialects.DUCK_DB.default_renderer()
    key = path.as_posix()
    try:
        env, statements = Environment(working_path=path.parent).parse(
            path.read_text(encoding="utf-8")
        )
    except Exception as e:
        return {key: f"PARSE ERROR {type(e).__name__}: {str(e)[:200]}"}
    out: dict[str, str] = {}
    selects = [
        s for s in statements if isinstance(s, (SelectStatement, MultiSelectStatement))
    ]
    for idx, statement in enumerate(selects):
        try:
            processed = process_query(
                env,
                statement,
                having_alias=renderer.SUPPORTS_ALIAS_IN_HAVING,
                supports_full_join=renderer.SUPPORTS_FULL_JOIN,
            )
            out[f"{key}#{idx}"] = renderer.compile_statement(processed)
        except Exception as e:
            # no traceback: it holds the tree's path, and two trees would differ
            out[f"{key}#{idx}"] = f"ERROR {type(e).__name__}: {str(e)[:300]}"
    return out


def main() -> None:
    out = Path(sys.argv[1])
    files = sorted({f for g in sys.argv[2:] for f in glob.glob(g, recursive=True)})
    start = time.time()
    result: dict[str, str] = {}
    for f in files:
        result.update(compile_file(Path(f)))
    out.write_text(json.dumps(result, indent=1), encoding="utf-8")
    errors = sum(1 for v in result.values() if v.startswith(("ERROR", "PARSE ERROR")))
    print(
        f"{len(result)} statements from {len(files)} files,"
        f" {errors} errors, {time.time() - start:.1f}s -> {out}"
    )


if __name__ == "__main__":
    main()
