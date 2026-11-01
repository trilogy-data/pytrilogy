"""Compiles and runs the Urban Tree Reporting dashboard queries the way its resolver does.

The resolver (trilogy-studio-core `pyserver`) parses the whole model bundle
through a `DictImportResolver` keyed by alias, imports the page's models, adds
each query's dashboard filters as one more WHERE stage (`filters_to_conditional`)
and generates DuckDB SQL. `script` datasources never exist there, so their
datasources drop out of planning; the browser only reads parquet.
"""

import copy
import json
import re
from functools import cache
from pathlib import Path
from typing import Any

import duckdb

from trilogy import Dialects, Environment, Executor
from trilogy.authoring import SelectStatement, WhereClause
from trilogy.core.models.environment import DictImportResolver, EnvironmentConfig
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parser import parse_text

ROOT = Path(__file__).parent
MODELS = ROOT / "models"
FIXTURES = MODELS / "fixtures"
QUERIES = json.loads((ROOT / "queries.json").read_text(encoding="utf-8"))

# Count summaries: what the oracle environment loses. `tree_rollup_city` and
# `species_native_range` are property lookups with no tree-row equivalent.
COUNT_SUMMARIES = (
    "tree_rollup_species",
    "tree_rollup_year",
    "tree_rollup_dbh",
    "tree_rollup_cell",
    "tree_rollup_coarse_cell",
)
TREE_FILE = re.compile(r"[/\\](full_tree_info|[a-z]{5}_tree_info)_v\d+\.parquet")
SUMMARY_FILE = re.compile(r"[/\\](" + "|".join(COUNT_SUMMARIES) + r")_v\d+\.parquet")
NAMED_PARAMETER = re.compile(r"(?<!:):([a-zA-Z_]\w*)")
DIALECT = DuckDBDialect()


class StudioEnvironmentConfig(EnvironmentConfig):
    # pyserver's config: every import resolves from the bundle root.
    def copy_for_root(self, root: str | None) -> "StudioEnvironmentConfig":
        return copy.deepcopy(self)


def local_text(text: str) -> str:
    return text.replace("`./fixtures/", f"`{FIXTURES.as_posix()}/")


@cache
def bundle_sources() -> dict[str, str]:
    sources = {}
    for path in MODELS.rglob("*.preql"):
        alias = path.relative_to(MODELS).with_suffix("").as_posix().replace("/", ".")
        text = local_text(path.read_text(encoding="utf-8"))
        # pyserver's normalize_relative_imports, for the one depth the bundle uses.
        sources[alias] = text.replace("import ..", "import ") if "." in alias else text
    return sources


def context(city: str) -> dict[str, Any]:
    return QUERIES["contexts"][city]


@cache
def dashboard_environment(city: str, summaries: bool) -> Environment:
    ctx = context(city)["source"]
    content = {**bundle_sources(), ctx["alias"]: ctx["contents"]}
    env = Environment(
        working_path=MODELS,
        config=StudioEnvironmentConfig(
            import_resolver=DictImportResolver(content=content)
        ),
    )
    imports = QUERIES["imports"]
    parse_text("\n".join(f"import {name};" for name in imports), env)
    if not summaries:
        for name in COUNT_SUMMARIES:
            assert env.delete_datasource(name), name
    return env


def param_type(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def filter_clause(
    filters: list[str], parameters: dict[str, Any], env: Environment, scope: str
) -> WhereClause | None:
    """pyserver's `filters_to_conditional`: every bind becomes a declared parameter."""
    cleaned = [f for f in filters if f.strip()]
    if not cleaned:
        return None
    declarations = ""
    values = {}
    for key, value in parameters.items():
        name = key.lstrip(":")
        declarations += f"\nparameter {name} {param_type(value)};"
        values[name] = value
        cleaned = [f.replace(key, name) for f in cleaned]
    env.set_parameters(**values)
    conditions = " AND ".join(f"({f})" for f in cleaned)
    _, parsed = parse_text(
        f"{declarations}\nWHERE {conditions} SELECT 1 as __ftest_{scope}_{len(cleaned) - 1};",
        env,
    )
    final = parsed[-1]
    return final.where_clause if isinstance(final, SelectStatement) else None


def generate_sql(
    case: dict[str, Any],
    summaries: bool = True,
    env: Environment | None = None,
    scope: str = "q0",
) -> str:
    """One case, in a fresh copy of its city's environment unless `env` is
    given: pyserver's `/generate_queries` compiles a page's queries in one
    environment, sweeping each query's derived concepts after it."""
    env = env or dashboard_environment(case["city"], summaries).duplicate()
    previous = {k: v for k, v in env.concepts.items() if v.name in case["query"]}
    _, parsed = parse_text(case["query"], env)
    final = parsed[-1]
    assert isinstance(final, SelectStatement)
    parameters = {**context(case["city"])["parameters"], **case["parameters"]}
    conditional = filter_clause(case["filters"], parameters, env, scope)
    if conditional:
        final.where_clauses = [*final.where_clauses, conditional]
    processed = DIALECT.generate_queries(env, [final])[-1]
    for address in final.locally_derived:
        env.remove_concept(address)
        if address in previous:
            env.add_concept(previous[address], force=True)
    return bind(
        DIALECT.compile_statement(processed),
        {k.lstrip(":"): v for k, v in parameters.items()},
    )


def sql_literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def bind(sql: str, parameters: dict[str, Any]) -> str:
    """The app's `applySqlParameters`: binds are inlined client-side before DuckDB runs."""
    return NAMED_PARAMETER.sub(
        lambda m: (
            sql_literal(parameters[m.group(1)])
            if m.group(1) in parameters
            else m.group(0)
        ),
        sql,
    )


def served_from(sql: str) -> str:
    summary, trees = bool(SUMMARY_FILE.search(sql)), bool(TREE_FILE.search(sql))
    return {
        (True, False): "summary",
        (False, True): "trees",
        (True, True): "mixed",
    }.get((summary, trees), "neither")


def execute(sql: str) -> list[tuple]:
    with duckdb.connect() as con:
        return [tuple(row) for row in con.execute(sql).fetchall()]


def execute_records(sql: str) -> list[dict[str, Any]]:
    with duckdb.connect() as con:
        cursor = con.execute(sql)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


# --- Builds ------------------------------------------------------------------
#
# A build runs where its scripts exist, so here each `file` script whose
# output is staged in build_fixtures/ reads that parquet instead.
BUILD_FIXTURES = ROOT / "build_fixtures"
SCRIPT = re.compile(r"`(?:\.\.?/)+(\w+)\.py`")
CITY_BUILDS = {
    "USSTO": ("ussto.stoughton_tree_info", "stoughton_tree_info"),
    "USSFO": ("ussfo.sf_tree_info", "sf_tree_info"),
    "USBOS": ("usbos.boston_tree_info", "boston_tree_info"),
}


def staged_script(match: re.Match) -> str:
    staged = BUILD_FIXTURES / f"{match.group(1)}.parquet"
    return f"`{staged.as_posix()}`" if staged.exists() else match.group(0)


def build_executor(entrypoint: str, output: Path, targets: list[str]) -> Executor:
    """The `trilogy refresh <entrypoint>` scope, with each target's file moved under `output`."""
    content = {}
    for alias, text in bundle_sources().items():
        text = SCRIPT.sub(staged_script, text)
        for target in targets:
            text = text.replace(
                f"{FIXTURES.as_posix()}/{target}_v", f"{output.as_posix()}/{target}_v"
            )
        content[alias] = text
    env = Environment(
        working_path=MODELS,
        config=StudioEnvironmentConfig(
            import_resolver=DictImportResolver(content=content)
        ),
    )
    parse_text(content[entrypoint], env)
    return Dialects.DUCK_DB.default_executor(environment=env)
