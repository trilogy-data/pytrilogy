"""An aggregate is only an aggregation when its CTE groups.

`BaseDialect.render_expr` reaches for `FUNCTION_MAP` only under
`group_to_grain`; otherwise the single-row forms apply (`sum(x) -> x`). Two
guards read the lineage instead of the render and treated a grain-matched or a
precomputed aggregate as local aggregation, blocking sound folds
(docs/handoff_grain_matched_projection_collapse.md).
"""

import re
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

THELOOK = Path(__file__).parent.parent / "modeling" / "thelook_duckdb"
TPCDS = Path(__file__).parent.parent / "modeling" / "tpc_ds_duckdb"

_CTE = re.compile(
    r"^(\w+) as \(\nSELECT\n(.*?)\nFROM\n(.*?)\)(?=,\n|\nSELECT)", re.DOTALL | re.MULTILINE
)
_RENAME = re.compile(r'^\s*"\w+"\."\w+" as "\w+",?$')


def _sql(working_path: Path, query: str) -> str:
    env = Environment(working_path=working_path)
    return Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]


def _cte_names(sql: str) -> list[str]:
    return re.findall(r"^(\w+) as \(", sql, re.MULTILINE)


def _rename_only_ctes(sql: str) -> list[str]:
    """CTEs that only re-alias one parent's columns: nothing but bare
    `"parent"."col" as "alias"` over a single source, no WHERE/JOIN/GROUP BY."""
    out = []
    for name, selects, source in _CTE.findall(sql):
        if any(word in source for word in ("JOIN", "WHERE", "GROUP BY")):
            continue
        if all(_RENAME.match(line) for line in selects.splitlines()):
            out.append(name)
    return out


def test_grain_matched_aggregate_cte_is_not_a_projection_barrier():
    """thelook adhoc04: the line-level aggregates sit at their input's grain, so
    the group node resolves to a non-grouping CTE whose columns are all bare
    renames of its single parent's."""
    sql = _sql(THELOOK, (THELOOK / "adhoc04.preql").read_text(encoding="utf-8"))
    assert not _rename_only_ctes(sql), sql
    assert len(_cte_names(sql)) == 3, sql


def test_precomputed_aggregate_does_not_block_an_aggregate_fold():
    """tpc-ds q08: `zip_p_count > 10 ? zip` is rendered from lineage, but the
    count it reads comes out of the parent's source_map, so folding an
    aggregate child into it nests nothing."""
    sql = _sql(TPCDS, (TPCDS / "query08.preql").read_text(encoding="utf-8"))
    assert not _rename_only_ctes(sql), sql
    assert len(_cte_names(sql)) == 8, sql
