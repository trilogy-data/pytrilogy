"""Canonical-vs-agent query pairs, and their rendered SQL.

Transpiling preql to SQL costs ~0.1-0.4s per query plus a one-off ~2s engine
boot, so this is the page's most expensive read by far. It is fetched for one
question at a time, only when the compare panel is open.
"""

from __future__ import annotations

import json
from pathlib import Path

from common.spec import BenchmarkSpec

# Which candidate/canonical extension each eval leg authors.
_CATEGORY_EXT = {
    "enriched": "preql",
    "ingest": "preql",
    "sql_bare": "sql",
    "sql_schema": "sql",
}


def category_ext(category: str | None) -> str:
    """Trilogy legs author .preql; the SQL baselines author .sql. Warehouse
    variants inherit from the leg their key starts with."""
    if not category:
        return "preql"
    if category in _CATEGORY_EXT:
        return _CATEGORY_EXT[category]
    return "sql" if category.startswith("sql") else "preql"


def ref_dir(spec: BenchmarkSpec) -> Path | None:
    """The hand-authored canonical query dir for this benchmark."""
    for cand in (spec.references_dir, spec.default_enriched_dir):
        if cand is not None and cand.is_dir():
            return cand
    return None


def read_query(base: Path | None, qid: int, ext: str) -> dict | None:
    if base is None:
        return None
    for name in (f"query{qid:02d}.{ext}", f"query{qid}.{ext}"):
        p = base / name
        if p.exists():
            return {
                "name": name,
                "lang": ext,
                "src": p.read_text(encoding="utf-8"),
                "_path": p,
                "_base": base,
            }
    return None


# --- preql to SQL transpilation (best-effort; the viewer stays usable without it) ---
_TRANSPILE_ENGINE: object | None = None
_ENGINE_FAILED = False
_SQL_CACHE: dict[tuple, tuple] = {}  # (path, mtime, working_path) -> (sql, is_error)
_PARAMS_CACHE: dict[Path, dict[int, dict]] = {}  # prompts_file -> qid -> params


def _transpile_engine():
    """A DB-less in-memory DuckDB executor reused across renders - generate_sql is
    pure transpilation, so we never open the (large) workspace database."""
    global _TRANSPILE_ENGINE, _ENGINE_FAILED
    if _TRANSPILE_ENGINE is None and not _ENGINE_FAILED:
        try:
            from trilogy import Dialects
            from trilogy.core.models.environment import Environment
            from trilogy.dialect.config import DuckDBConfig

            _TRANSPILE_ENGINE = Dialects.DUCK_DB.default_executor(
                environment=Environment(), conf=DuckDBConfig()
            )
        except Exception:
            _ENGINE_FAILED = True
    return _TRANSPILE_ENGINE


def load_params(spec: BenchmarkSpec) -> dict[int, dict]:
    pf = spec.prompts_file
    if pf not in _PARAMS_CACHE:
        params: dict[int, dict] = {}
        if pf.exists():
            data = json.loads(pf.read_text(encoding="utf-8"))
            qs = data.get("queries", []) if isinstance(data, dict) else data
            params = {q["id"]: q["params"] for q in qs if q.get("params")}
        _PARAMS_CACHE[pf] = params
    return _PARAMS_CACHE[pf]


def _render_sql(text: str, working_path: Path, params: dict | None) -> tuple[str, bool]:
    engine = _transpile_engine()
    if engine is None:
        return "", True
    from trilogy.core.models.environment import Environment

    try:
        engine.environment = Environment(working_path=working_path)
        if params:
            engine.environment.set_parameters(
                **{name: spec.get("value") for name, spec in params.items()}
            )
        statements = engine.generate_sql(text)
        return (statements[-1] if statements else "-- (no statement)"), False
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}", True


def augment_sql(q: dict | None, params: dict | None) -> dict | None:
    """Attach rendered SQL (cached by file mtime) and strip internal path fields."""
    if q is None:
        return None
    base, path = q.pop("_base"), q.pop("_path")
    if q["lang"] != "preql":
        q["sql"], q["sqlError"] = q["src"], False  # SQL legs: candidate is already SQL
        return q
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    key = (str(path), mtime, str(base))
    if key not in _SQL_CACHE:
        _SQL_CACHE[key] = _render_sql(q["src"], base, params)
    q["sql"], q["sqlError"] = _SQL_CACHE[key]
    return q


def query_pair(
    run_dir: Path, spec: BenchmarkSpec, qid: int, rep: int, category: str | None
) -> dict:
    """The canonical and agent-written queries for one question, with SQL."""
    workspace = run_dir / "workspace"
    ext = category_ext(category)
    params = load_params(spec).get(qid)
    # Candidate at workspace root (single-leg runs), else the rep's worker dir.
    candidate = read_query(workspace, qid, ext) or read_query(
        workspace / f"_worker_{rep}", qid, ext
    )
    return {
        "candidate": augment_sql(candidate, params),
        "canonical": augment_sql(read_query(ref_dir(spec), qid, ext), params),
    }
