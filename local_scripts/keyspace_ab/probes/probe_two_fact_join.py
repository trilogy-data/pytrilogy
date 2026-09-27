"""Rows-first probe for docs/keyspace_phase_plan.md open item 2: a region only
a JOIN of two facts witnesses (TPC-DS q64's returned sale line). `sales`
carries {item, ticket, sale_date}; `returns`, at the same grain, carries
{~item, ~ticket, reason}. No single source carries all four entities, so the
keyspace's base region stands in. Derived `reason_class` (a function of the
reason, ABSENT on an unreturned line) against the same concept materialized
at its grain in `reasons` (the oracle), plus hand rows.

`--sql <n>` prints query n's SQL on the derived model; `--ks` prints every
keyspace as it is built."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from trilogy import Dialects

BASE = """
key item_sk int;
property item_sk.item_desc string;
key ticket int;
key sale_date int;
property sale_date.sale_year int;
property <item_sk, ticket>.qty int;
property <item_sk, ticket>.ret_qty int?;
key reason_sk int;
property reason_sk.reason_desc string;

datasource items (sk: item_sk, d: item_desc)
grain (item_sk)
query '''select 10 as sk, 'alpha' as d union all select 20, 'beta' union all select 30, 'gamma' ''';

datasource dates (d: sale_date, y: sale_year)
grain (sale_date)
query '''select 1 as d, 1999 as y union all select 2, 2000''';
"""
_SALES = """select 10 as sk, 1 as t, 1 as d, 5 as q
union all select 10, 2, 2, 7
union all select 20, 3, 1, 9
union all select 30, 4, 2, 2"""
_RETURNS = """select 10 as sk, 1 as t, 1 as rq, 1 as r
union all select 20, 3, 9, 2"""
_REASONS = """select 1 as sk, 'broken' as d
union all select 2, 'late'
union all select 3, 'wrong' """

DERIVED = BASE + f"""
datasource sales (sk: item_sk, t: ticket, d: sale_date, q: qty)
grain (item_sk, ticket)
query '''{_SALES}''';

datasource returns (sk: ~item_sk, t: ~ticket, rq: ret_qty, r: reason_sk)
grain (item_sk, ticket)
query '''{_RETURNS}''';

datasource reasons (sk: reason_sk, d: reason_desc)
grain (reason_sk)
query '''{_REASONS}''';

auto reason_class <- case when reason_desc = 'broken' then 'defect' else 'other' end;
auto is_returned <- ret_qty is not null;
"""

MATERIALIZED = BASE + f"""
property reason_sk.reason_class string;
property <item_sk, ticket>.is_returned bool;

datasource sales (sk: item_sk, t: ticket, d: sale_date, q: qty, ir: is_returned)
grain (item_sk, ticket)
query '''select s.*, s.t in (1, 3) as ir from ({_SALES}) s''';

datasource returns (sk: ~item_sk, t: ~ticket, rq: ret_qty, r: reason_sk)
grain (item_sk, ticket)
query '''{_RETURNS}''';

datasource reasons (sk: reason_sk, d: reason_desc, c: reason_class)
grain (reason_sk)
query '''select r.*, case when r.d = 'broken' then 'defect' else 'other' end as c from ({_REASONS}) r''';
"""

# lines: L1=(10,t1,1999,q5; returned rq1 broken) L2=(10,t2,2000,q7)
#        L3=(20,t3,1999,q9; returned rq9 late)   L4=(30,t4,2000,q2)
CASES: list[tuple[str, list[tuple] | None]] = [
    (
        "select item_sk, ticket, reason_class",
        [(10, 1, "defect"), (10, 2, None), (20, 3, "other"), (30, 4, None)],
    ),
    (
        "select sale_year, reason_class",
        [(1999, "defect"), (1999, "other"), (2000, None)],
    ),
    (
        "select sale_year, reason_class, count(ticket) as n",
        [(1999, "defect", 1), (1999, "other", 1), (2000, None, 2)],
    ),
    (
        "select sale_year, reason_class, sum(qty) as q",
        [(1999, "defect", 5), (1999, "other", 9), (2000, None, 9)],
    ),
    (
        "select item_desc, reason_class, is_returned",
        [
            ("alpha", "defect", True),
            ("alpha", None, False),
            ("beta", "other", True),
            ("gamma", None, False),
        ],
    ),
    (
        "select reason_class, count(ticket) as n",
        [("defect", 1), ("other", 1), (None, 2)],
    ),
    (
        "select reason_class, count(ticket) as n where is_returned",
        [("defect", 1), ("other", 1)],
    ),
    ("select item_sk, ticket where reason_class is null", [(10, 2), (30, 4)]),
    (
        "select sale_year, count(ticket) as n where reason_class = 'other' or reason_class is null",
        [(1999, 1), (2000, 2)],
    ),
    (
        "select sale_year, reason_desc, count(ticket) as n",
        [(1999, "broken", 1), (1999, "late", 1), (2000, None, 2)],
    ),
    (
        "select item_desc, count(ticket) as n, count(reason_sk) as r",
        [("alpha", 2, 1), ("beta", 1, 1), ("gamma", 1, 0)],
    ),
    ("select sale_year, count(reason_sk) as r", [(1999, 2), (2000, 0)]),
    (
        "select reason_class, sum(ret_qty) as rq",
        [("defect", 1), ("other", 9), (None, None)],
    ),
    (
        "select item_desc, reason_class where sale_year = 2000",
        [("alpha", None), ("gamma", None)],
    ),
    (
        "select sale_year, is_returned, count(ticket) as n",
        [(1999, True, 2), (2000, False, 2)],
    ),
    # ticket is not requested: the item/reason relation is the returns rows,
    # and item 10 is referenced by a return, so only item 30 is reason-less
    (
        "select reason_class, count(item_sk) as items",
        [("defect", 1), ("other", 1), (None, 1)],
    ),
    (
        "select sale_year, reason_class, count(item_sk) as i where qty > 4",
        [(1999, "defect", 1), (1999, "other", 1), (2000, None, 1)],
    ),
    ("select reason_desc, sum(qty) as q", [("broken", 5), ("late", 9), (None, 9)]),
    (
        "select item_desc, sale_year, reason_class",
        [
            ("alpha", 1999, "defect"),
            ("alpha", 2000, None),
            ("beta", 1999, "other"),
            ("gamma", 2000, None),
        ],
    ),
    ("select sale_year, count(ticket) as n where reason_class is null", [(2000, 2)]),
    ("select sale_year, count(ticket) as n where reason_class = 'defect'", [(1999, 1)]),
    ("select sale_year, reason_class, is_returned", None),
    (
        "select item_desc, count(ticket) as n where reason_class is null",
        [("alpha", 1), ("gamma", 1)],
    ),
    (
        "select reason_class, count(sale_date) as d",
        [("defect", 1), ("other", 1), (None, 1)],
    ),
    (
        "select sale_year, count(ticket) as n, count(reason_class) as c",
        [(1999, 2, 2), (2000, 2, 0)],
    ),
]


def _ex(model: str):
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(model)
    return ex


def _key(r):
    return tuple((v is None, str(v)) for v in r)


def _rows(ex, q):
    try:
        return sorted(
            (tuple(r) for r in ex.execute_text(q + ";")[-1].fetchall()), key=_key
        )
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:200]}"


def _show_keyspaces():
    import trilogy.core.processing.v4_node_generators.rowset_witness as rw

    original = rw.statement_keyspace

    def showing(concept_attrs, mandatory_list, environment, conditions, *a, **kw):
        ks = original(concept_attrs, mandatory_list, environment, conditions, *a, **kw)
        print(
            "  KEYSPACE outputs=",
            [c.address for c in mandatory_list][:6],
            "demanded=",
            sorted(ks.output_demanded_spans),
            "regions=",
            ks.describe(),
        )
        return ks

    rw.statement_keyspace = showing


if __name__ == "__main__":
    if "--ks" in sys.argv:
        _show_keyspaces()
    if "--sql" in sys.argv:
        n = int(sys.argv[sys.argv.index("--sql") + 1])
        q, _ = CASES[n]
        ex = _ex(DERIVED)
        print(q)
        print(_rows(ex, q))
        print(ex.generate_sql(q + ";")[-1])
        sys.exit()
    if "--q" in sys.argv:
        # ad-hoc queries on both models; `--raise` re-raises the derived side
        d, m = _ex(DERIVED), _ex(MATERIALIZED)
        for q in sys.argv[sys.argv.index("--q") + 1 :]:
            if q.startswith("--"):
                break
            if "--raise" in sys.argv:
                d.execute_text(q + ";")
            rd, rm = _rows(d, q), _rows(m, q)
            print(
                f"{'SAME' if rd == rm else 'DIFF'} {q}\n      derived:      {rd}\n      materialized: {rm}"
            )
        sys.exit()
    d, m = _ex(DERIVED), _ex(MATERIALIZED)
    bad = 0
    for i, (q, want) in enumerate(CASES):
        rd, rm = _rows(d, q), _rows(m, q)
        want_sorted = sorted(want, key=_key) if want is not None else None
        oracle = "SAME" if rd == rm else "DIFF"
        hand = (
            "" if want is None else (" hand=OK " if rd == want_sorted else " hand=BAD")
        )
        print(f"[{i}] oracle={oracle}{hand} {q}")
        if rd != rm or (want is not None and rd != want_sorted):
            bad += 1
            print(f"      derived:      {rd}")
            print(f"      materialized: {rm}")
            if want is not None:
                print(f"      want:         {want_sorted}")
    print(f"\n{bad} of {len(CASES)} differ")
