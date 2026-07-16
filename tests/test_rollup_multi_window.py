"""Windows over ROLLUP/CUBE selects must not duplicate or drop grouping rows
(q86).

Under a nonstandard grouping spec the visible dims are not a row identity:
the grand-total, a NULL-key subtotal, and a genuine NULL-key detail row all
display the same key tuple. Any plan that computes sibling windows in
separate branches and joins them back on the dims fans those rows out (or,
with a NULL-unsafe join, deletes them). Sibling windows over the same
grouping pass must be computed together.

Data is chosen so a genuine NULL detail row collides with the NULL-padded
subtotal/total rows: (NULL, NULL, 3) detail vs (NULL, NULL, 3) category
subtotal vs (NULL, NULL, 38) grand total.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key sale_id int;
property sale_id.category string;
property sale_id.cls string;
property sale_id.amount int;

datasource sales (id: sale_id, cat: category, cl: cls, amt: amount)
grain (sale_id)
query '''select * from (values
    (1,'A','x',10),(2,'A','y',20),(3,'B','x',5),(4,null,null,3)
) as t(id,cat,cl,amt)''';
"""

DERIVATIONS = """auto total <- sum(amount);
auto lvl <- grouping(category) + grouping(cls);
auto parent <- case when grouping(cls) = 0 then category else null end;
auto rank_a <- rank(category, cls) over (partition by lvl, parent order by total desc);
auto rank_b <- rank(category) over (partition by lvl order by total desc);
"""

ORDERING = "order by lvl asc, category asc, cls asc"

ROLLUP_TWO_WINDOWS_EXPECTED = [
    ("A", "x", 10, 0, 2, 2),
    ("A", "y", 20, 0, 1, 1),
    ("B", "x", 5, 0, 1, 3),
    (None, None, 3, 0, 1, 4),
    ("A", None, 30, 1, 1, 1),
    ("B", None, 5, 1, 2, 2),
    (None, None, 3, 1, 3, 3),
    (None, None, 38, 2, 1, 1),
]


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_rollup_two_windows_different_partitions():
    # The q86 trigger: two ranks whose partition specs differ. 8 rollup rows
    # in, 8 rows out — no 3x3 fan-out of the (NULL, NULL) rows.
    rows = _rows(
        DERIVATIONS + "select category, cls, total, lvl, rank_a, rank_b\n"
        f"by rollup (category, cls)\n{ORDERING};"
    )
    assert rows == ROLLUP_TWO_WINDOWS_EXPECTED


def test_rollup_no_window_baseline():
    rows = _rows(
        DERIVATIONS
        + f"select category, cls, total, lvl\nby rollup (category, cls)\n{ORDERING};"
    )
    assert len(rows) == 8
    assert rows == [(c, k, t, lv) for c, k, t, lv, _, _ in ROLLUP_TWO_WINDOWS_EXPECTED]


def test_rollup_single_window_each_partition():
    for rank_col, idx in (("rank_a", 4), ("rank_b", 5)):
        rows = _rows(
            DERIVATIONS + f"select category, cls, total, lvl, {rank_col}\n"
            f"by rollup (category, cls)\n{ORDERING};"
        )
        assert rows == [
            (c, k, t, lv, (ra, rb)[idx - 4])
            for c, k, t, lv, ra, rb in ROLLUP_TWO_WINDOWS_EXPECTED
        ], rank_col


def test_rollup_two_windows_same_partition():
    rows = _rows(
        DERIVATIONS
        + "auto rank_b2 <- rank(category, cls) over (partition by lvl, parent order by total asc);\n"
        "select category, cls, total, lvl, rank_a, rank_b2\n"
        f"by rollup (category, cls)\n{ORDERING};"
    )
    assert len(rows) == 8


def test_no_rollup_two_windows_different_partitions():
    # Without a grouping spec the dims are a genuine identity; the diamond
    # plan (if any) is sound. 4 grouped rows in, 4 out.
    rows = _rows(
        "auto total <- sum(amount);\n"
        "auto rank_a <- rank(category, cls) over (partition by category order by total desc);\n"
        "auto rank_b <- rank(category) over (order by total desc);\n"
        "select category, cls, total, rank_a, rank_b\n"
        "order by category asc, cls asc;"
    )
    assert len(rows) == 4


def test_cube_two_windows_different_partitions():
    # CUBE has two distinct lvl=1 rows that display (NULL, NULL, 3): the
    # NULL-category subtotal and the NULL-class subtotal. Both must appear
    # exactly once each — 11 rows total (4 detail + 3 + 3 subtotal + 1 total).
    rows = _rows(
        DERIVATIONS + "select category, cls, total, lvl, rank_a, rank_b\n"
        f"by cube (category, cls)\n{ORDERING};"
    )
    assert len(rows) == 11
    assert rows.count((None, None, 3, 1, 5, 5)) == 2
    assert rows.count((None, None, 38, 2, 1, 1)) == 1


def test_rowset_rollup_two_windows_different_partitions():
    # Same trigger through a rowset boundary: the rollup rows' identity must
    # survive the rowset so the outer windows neither fan out nor drop rows.
    rows = _rows("""with agg as
select category, cls, sum(amount) as total,
    grouping(category) + grouping(cls) as lvl,
    case when grouping(cls) = 0 then category else null end as parent
by rollup (category, cls);
select agg.category, agg.cls, agg.total, agg.lvl,
    rank(agg.category, agg.cls) over (partition by agg.lvl, agg.parent order by agg.total desc) as rank_a,
    rank(agg.category) over (partition by agg.lvl order by agg.total desc) as rank_b
order by agg.lvl asc, agg.category asc, agg.cls asc;""")
    assert rows == ROLLUP_TWO_WINDOWS_EXPECTED
