"""Reproduce row multiplication from group(...) under a multi-level rollup."""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from evals.common.scoring import make_scoring_engine
from trilogy.core.models.environment import Environment

DEFAULT_WORKSPACE = Path("evals/tpcds_agent/results/20260808-151955_enriched/workspace")

PRELUDE = """\
import raw.catalog_sales as cs;

auto row_dependent_count <- group(cs.pos_customer_demographic.dependent_count)
    by cs.order_number, cs.item.sk;

where
  cs.sale_date.year = 1998
  and cs.pos_customer_demographic.gender = 'F'
  and cs.pos_customer_demographic.education_status = 'Unknown'
"""

SINGLE_ROLLUP = PRELUDE + """\
select
  cs.item.id as item_code,
  avg(row_dependent_count) as avg_dependent_count
by rollup (cs.item.id);
"""

MULTI_ROLLUP = PRELUDE + """\
select
  cs.item.id as item_code,
  cs.billing_customer.current_address.country as country,
  cs.billing_customer.current_address.state as state,
  cs.billing_customer.current_address.county as county,
  grouping(cs.item.id) as g_item,
  grouping(cs.billing_customer.current_address.country) as g_country,
  grouping(cs.billing_customer.current_address.state) as g_state,
  grouping(cs.billing_customer.current_address.county) as g_county,
  avg(row_dependent_count) as avg_dependent_count
by rollup (
  cs.item.id,
  cs.billing_customer.current_address.country,
  cs.billing_customer.current_address.state,
  cs.billing_customer.current_address.county
);
"""


def execute(engine, workspace: Path, query: str) -> list[tuple]:
    engine.environment = Environment(working_path=workspace)
    sql = engine.generate_sql(query)[-1]
    return list(engine.execute_raw_sql(sql).fetchall())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = args.workspace.resolve()
    engine = make_scoring_engine(workspace / "tpcds.duckdb", workspace, "tpcds")
    single = execute(engine, workspace, SINGLE_ROLLUP)
    multi = execute(engine, workspace, MULTI_ROLLUP)
    keys = [row[:8] for row in multi]
    counts = Counter(keys)
    duplicate_rows = len(keys) - len(counts)
    max_multiplicity = max(counts.values(), default=0)
    print(f"single-level rollup rows: {len(single)}")
    print(f"four-level rollup rows: {len(multi)}")
    print(f"four-level distinct dimension+grouping tuples: {len(counts)}")
    print(f"duplicate rows at the full rollup grain: {duplicate_rows}")
    print(f"maximum tuple multiplicity: {max_multiplicity}")
    if duplicate_rows:
        print("REPRODUCED: rollup emitted repeated rows at the same output grain")
        return 0
    print("NOT REPRODUCED: every rollup dimension tuple is unique")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
