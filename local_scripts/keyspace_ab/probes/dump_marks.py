"""Walk the resolved QueryDatasource tree of argv queries on
TWO_PROP_GUEST_ALLDESC_MODEL (or UNSOLD with --unsold) and print each node's
nullable / partial / extent-free marks and its joins."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment
from trilogy.core.models.build import BuildDatasource
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.join_resolution import (
    extension_padded_addresses,
    extent_null_addresses,
)

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = t.TWO_PROP_MODEL if "--unsold" in sys.argv else t.TWO_PROP_GUEST_ALLDESC_MODEL


def _s(addr: str) -> str:
    return addr.replace("local.", "")


def dump(ds, depth: int = 0, seen: set[int] | None = None) -> None:
    seen = seen if seen is not None else set()
    pad = "  " * depth
    if isinstance(ds, BuildDatasource):
        print(f"{pad}LEAF {ds.identifier}")
        return
    if not isinstance(ds, QueryDatasource):
        print(f"{pad}?? {type(ds).__name__}")
        return
    ident = ds.identifier
    short = ident if len(ident) < 60 else ident[:28] + "…" + ident[-28:]
    print(
        f"{pad}QDS {short}\n"
        f"{pad}    out={[_s(c.address) for c in ds.output_concepts]}\n"
        f"{pad}    nullable={[_s(c.address) for c in ds.nullable_concepts]}"
        f" partial={[_s(c.address) for c in ds.partial_concepts]}\n"
        f"{pad}    region_spans={sorted(_s(a) for a in ds.region_spans)}"
        f" extent_free={sorted(_s(a) for a in ds.extent_free_spans)}"
        f" carried={sorted(_s(a) for a in ds.extent_free_carried)}\n"
        f"{pad}    extent_null={sorted(_s(a) for a in extent_null_addresses(ds))}"
        f" ext_padded[extent_free]={sorted(_s(a) for a in extension_padded_addresses(ds, ds.extent_free_spans))}"
    )
    for j in ds.joins:
        if hasattr(j, "join_type"):
            pairs = [
                f"{_s(p.left.address)}={_s(p.right.address)}"
                for p in (j.concept_pairs or [])
            ]
            print(
                f"{pad}    JOIN {j.join_type.name} -> {j.right_datasource.identifier[:40]} on {pairs}"
            )
    if id(ds) in seen:
        print(f"{pad}    (seen)")
        return
    seen.add(id(ds))
    for child in ds.datasources:
        dump(child, depth + 1, seen)


env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [x for x in sys.argv[1:] if not x.startswith("--")]:
    print(f"\n=== {q}")
    from trilogy.core.query_processor import get_query_datasources
    from trilogy.core.statements.author import SelectStatement

    _, stmts = env.parse(q)
    for stmt in stmts:
        if isinstance(stmt, SelectStatement):
            dump(get_query_datasources(environment=env, statement=stmt))
    print("  rows:", ex.execute_query(q).fetchall())
