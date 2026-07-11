"""Authored joins on a dimension PROPERTY needing an FK hop per side.

The q17/q25 shape: two fact sources each bind a local surrogate FK to their own
dimension scan; the authored join relates the dimensions' shared BUSINESS key
(`union join a.customer.id = b.customer.id`), which no fact binds. The facts
ALSO share a directly-bound key (`sku` — q25's item), giving discovery a
cheaper path that historically won the Steiner walk and silently dropped the
customer equality (global/per-sku sums instead of per-customer).

Surrogate keys deliberately DISAGREE between the sides (C1 is sk 1 on side a,
sk 10 on side b), so any accidental sk-based pairing changes every row.
Side-a customer ids are a subset of side-b ids (honest SUBSET data); values
are power-of-two vs power-of-hundred so fan-out or key drops move sums
detectably.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import make_engine, sort_rows

# (sk, id) per side; same business ids, different surrogates
A_DIM = [(1, "C1"), (2, "C2"), (3, "C3")]
B_DIM = [(10, "C1"), (20, "C2"), (30, "C3"), (40, "C4")]
# (order, cust_sk, sku, amount)
A_FACTS = [(1, 1, 100, 1), (2, 1, 200, 2), (3, 2, 100, 4), (4, 3, 200, 8)]
B_FACTS = [
    (1, 10, 100, 100),
    (2, 20, 200, 200),
    (3, 20, 100, 400),
    (4, 40, 200, 800),
]

A_SK_TO_ID = dict(A_DIM)
B_SK_TO_ID = dict(B_DIM)


def _dim_sql(rows: list[tuple[int, str]]) -> str:
    return " union all ".join(f"select {sk} sk, '{cid}' cid" for sk, cid in rows)


def _fact_sql(rows: list[tuple[int, int, int, int]]) -> str:
    return " union all ".join(
        f"select {o} o, {sk} cust, {sku} sku, {amt} amt" for o, sk, sku, amt in rows
    )


MODEL = f"""
key sku int;

key a_cust_sk int;
property a_cust_sk.a_cust_id string;
datasource a_customers (
    sk: a_cust_sk,
    cid: a_cust_id,
) grain (a_cust_sk) query '''{_dim_sql(A_DIM)}''';

key b_cust_sk int;
property b_cust_sk.b_cust_id string;
datasource b_customers (
    sk: b_cust_sk,
    cid: b_cust_id,
) grain (b_cust_sk) query '''{_dim_sql(B_DIM)}''';

key a_order int;
property a_order.a_amount int;
datasource a_facts (
    o: a_order,
    cust: a_cust_sk,
    sku: sku,
    amt: a_amount,
) grain (a_order) query '''{_fact_sql(A_FACTS)}''';

key b_order int;
property b_order.b_amount int;
datasource b_facts (
    o: b_order,
    cust: b_cust_sk,
    sku: sku,
    amt: b_amount,
) grain (b_order) query '''{_fact_sql(B_FACTS)}''';
"""


def _write_model(tmp_path: Path) -> Path:
    (tmp_path / "hop.preql").write_text(MODEL)
    return tmp_path


def _run(tmp_path: Path, query: str) -> tuple[str, list[tuple]]:
    engine = make_engine(_write_model(tmp_path))
    statements = engine.parse_text("import hop;\n" + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
    return sql, sort_rows(rows)


def _totals_by_id(facts, sk_to_id) -> dict[str, int]:
    out: dict[str, int] = {}
    for _, sk, _, amt in facts:
        cid = sk_to_id[sk]
        out[cid] = out.get(cid, 0) + amt
    return out


A_BY_ID = _totals_by_id(A_FACTS, A_SK_TO_ID)
B_BY_ID = _totals_by_id(B_FACTS, B_SK_TO_ID)


def _b_rows_matching(cid: str, sku: int) -> list[tuple[int, int, int, int]]:
    return [row for row in B_FACTS if B_SK_TO_ID[row[1]] == cid and row[2] == sku]


@pytest.mark.parametrize("join", ["union join", "subset join"])
def test_member_projected_two_side_rollup(tmp_path: Path, join: str):
    # member-grain rollup: both sides preserved (side-a ids ⊆ side-b domain,
    # so subset and union agree cell-for-cell here); an unmatched b row still
    # belongs to its own id's group via the projection coalesce
    _, rows = _run(
        tmp_path,
        f"""select
    a_cust_id,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
{join} a_cust_id = b_cust_id
;""",
    )
    keys = set(A_BY_ID) | set(B_BY_ID)
    expected = sort_rows([(k, A_BY_ID.get(k), B_BY_ID.get(k)) for k in keys])
    assert rows == expected, rows


@pytest.mark.parametrize("join", ["union join", "subset join"])
def test_member_unprojected_cross_side_grain(tmp_path: Path, join: str):
    # THE q25 shape: the authored key appears nowhere in the output, and the
    # shared `sku` gives discovery a cheaper fact-to-fact path. Each a_order
    # row's b_total must still be its (customer, sku)-matched total — the
    # historical bug paired on sku alone (500/1000 per row here).
    sql, rows = _run(
        tmp_path,
        f"""select
    a_order,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
{join} a_cust_id = b_cust_id
;""",
    )
    matched_b_orders = set()
    expected_matched = []
    for order, sk, sku, amt in A_FACTS:
        matching = _b_rows_matching(A_SK_TO_ID[sk], sku)
        matched_b_orders.update(row[0] for row in matching)
        b_total = sum(row[3] for row in matching) if matching else None
        expected_matched.append((order, amt, b_total))
    unmatched_total = sum(row[3] for row in B_FACTS if row[0] not in matched_b_orders)
    expected = sort_rows(expected_matched + [(None, None, unmatched_total)])
    assert rows == expected, f"{rows}\n{sql}"


def test_member_unprojected_sql_pairs_dimension_property(tmp_path: Path):
    # shape pin: both dimension scans appear and the pairing predicate relates
    # the two id columns — negative-assert the dropped-equality shape
    sql, _ = _run(
        tmp_path,
        """select
    a_order,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
union join a_cust_id = b_cust_id
;""",
    )
    assert '"a_customers"' in sql, sql
    assert '"b_customers"' in sql, sql
    assert '"cid"' in sql, sql


def test_parity_join_clause_vs_global_merge(tmp_path: Path):
    # scope-blind: the query-scoped subset join and the model-level partial
    # merge are the same declaration and must return identical rows
    query = """select
    a_order,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
subset join a_cust_id = b_cust_id
;"""
    _, join_rows = _run(tmp_path, query)

    merged_dir = tmp_path / "merged"
    merged_dir.mkdir()
    (merged_dir / "hop.preql").write_text(
        MODEL + "\nmerge a_cust_id into ~b_cust_id;\n"
    )
    engine = make_engine(merged_dir)
    statements = engine.parse_text("""import hop;
select
    a_order,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
;""")
    sql = engine.generate_sql(statements[-1])[-1]
    merge_rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    assert join_rows == merge_rows, f"{join_rows}\n{merge_rows}"
