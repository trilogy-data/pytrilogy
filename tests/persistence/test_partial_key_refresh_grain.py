"""Lock (#791): a `~` key outside the target's grain never adds extension rows.

`select id, k` over a fact binding `~k` licenses one extension row per `k`
member the fact never cites, NULL at `id`. A target declared `grain (id)` is
built from its grain's rows, so the refresh must write only the fact's rows.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeys, UpdateKeyType

DIRECT = """
key k string;
key id int;
root datasource dim_raw (k: k) grain (k) address dim_raw;
root datasource fact_raw (id: id, k: ~k) grain (id) address fact_raw;
datasource fact (id, ~k) grain (id) address fact_out;
"""

MERGED = """
key k string;
key id int;
key k_cited string;
root datasource dim_raw (k: k) grain (k) address dim_raw;
root datasource fact_raw (id: id, k: k_cited) grain (id) address fact_raw;
merge k_cited into ~k;
datasource fact (id, ~k) grain (id) address fact_out;
"""


def _refresh(model: str, keys: UpdateKeys | None = None) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "create table dim_raw as select * from (values ('a'), ('b'), ('c')) t(k)"
    )
    executor.execute_raw_sql(
        "create table fact_raw as select * from (values (1, 'a'), (2, 'a')) t(id, k)"
    )
    executor.parse_text(model)
    executor.update_datasource(executor.environment.datasources["fact"], keys)
    return executor.execute_raw_sql(
        "select id, k from fact_out order by id nulls last"
    ).fetchall()


@pytest.mark.parametrize("model", [DIRECT, MERGED], ids=["direct", "merged"])
def test_partial_key_refresh_writes_only_grain_rows(model: str) -> None:
    assert _refresh(model) == [(1, "a"), (2, "a")]


def test_partial_key_incremental_refresh_keeps_watermark() -> None:
    keys = UpdateKeys(
        {"local.id": UpdateKey("local.id", UpdateKeyType.INCREMENTAL_KEY, 1)}
    )
    assert _refresh(DIRECT, keys) == [(2, "a")]
