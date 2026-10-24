"""Null-rejection laws (formal/Trilogy/NullReject.lean), checked on random WHEREs.

A column the walk proves non-null forces a padded side of a join present, so
the planner may read a partial source alone or upgrade an outer join. Each
case requires:

- `proofs_sound`: every row a WHERE keeps has each column
  `gather_non_null_proofs` names non-null
- `upgrade_by_proof`: a model whose `returns` columns pad through a `~` key
  answers every WHERE as the same model over one pre-joined table does
"""

import random

import pytest

from trilogy import Dialects, Environment
from trilogy.core.models.build import Factory
from trilogy.core.processing.condition_utility import gather_non_null_proofs

TABLES = """
CREATE TABLE lines AS
SELECT 1 AS o, 10 AS i, 5 AS q, 'x' AS note UNION ALL
SELECT 1, 11, 3, 'y' UNION ALL
SELECT 2, 10, 7, 'x' UNION ALL
SELECT 3, 12, 2, 'z';
CREATE TABLE returns AS
SELECT 1 AS o, 10 AS i, 1 AS ro, 'dmg' AS reason UNION ALL
SELECT 3, 12, 3, NULL;
CREATE TABLE joined AS
SELECT l.o, l.i, l.q, l.note, r.ro, r.reason
FROM lines l LEFT JOIN returns r ON l.o = r.o AND l.i = r.i;
"""

HEAD = """
key order_id int;
key item_id int;
properties <order_id, item_id> (
    qty int,
    note string,
    ret int?,
    reason string?,
);
auto is_returned <- ret is not null;
auto ret_label <- concat('r', ret::string);
auto ret_plus <- ret + 1;
"""

SPLIT = HEAD + """
datasource lines (o: order_id, i: item_id, q: qty, note: note)
grain (order_id, item_id) address lines;
datasource returns (o: ~order_id, i: ~item_id, ro: ret, reason: reason)
grain (order_id, item_id) address returns;
"""

FLAT = HEAD + """
datasource joined (o: order_id, i: item_id, q: qty, note: note, ro: ret, reason: reason)
grain (order_id, item_id) address joined;
"""

ROWSET = "with rs as where qty < 4 select order_id, item_id, ret, qty;"

SHAPES = [
    "select order_id, item_id;",
    "select order_id, count(item_id) -> n;",
    "select note, sum(qty) -> s;",
]


def _int_expr(rng: random.Random, depth: int) -> str:
    base = rng.choice(["ret", "ret", "qty"])
    if depth <= 0:
        return base
    inner = _int_expr(rng, depth - 1)
    return rng.choice(
        [
            base,
            f"coalesce({inner}, {rng.randint(0, 3)})",
            f"({inner} + 1)",
            f"nullif({inner}, {rng.randint(0, 3)})",
            f"greatest({inner}, {rng.randint(0, 3)})",
            f"len({_str_expr(rng, depth - 1)})",
            f"case when {inner} > 1 then 1 else 0 end",
            f"({inner} * 2)",
        ]
    )


def _str_expr(rng: random.Random, depth: int) -> str:
    base = rng.choice(["reason", "ret::string", "note"])
    if depth <= 0:
        return base
    inner = _str_expr(rng, depth - 1)
    return rng.choice(
        [
            base,
            f"concat({inner}, 'z')",
            f"concat_ws(',', {inner}, 'z')",
            f"coalesce({inner}, 'dmg')",
            f"upper({inner})",
            f"({inner} || 'z')",
        ]
    )


def _atom(rng: random.Random, depth: int) -> str:
    cmp = rng.choice(["=", "!=", ">", "<"])
    return rng.choice(
        [
            f"{_int_expr(rng, depth)} {cmp} {rng.randint(0, 3)}",
            (
                f"{_str_expr(rng, depth)} {rng.choice(['=', '!='])} "
                f"'{rng.choice(['z', 'dmg', '1z', 'x', ',z'])}'"
            ),
            (
                f"{rng.choice([_int_expr(rng, depth), _str_expr(rng, depth)])} is "
                f"{rng.choice(['', 'not '])}null"
            ),
            (
                f"{_int_expr(rng, depth)} {rng.choice(['in', 'not in'])} "
                f"{rng.choice(['(1, 3)', '(1, null)'])}"
            ),
            f"is_returned = {rng.choice(['true', 'false'])}",
            f"{_int_expr(rng, depth)} between 1 and 3",
            f"ret_label {rng.choice(['=', '!='])} '{rng.choice(['r', 'r1', 'x'])}'",
            f"ret_plus {rng.choice(['>', '<', '!='])} 2",
            (
                f"{rng.choice(['ret', 'qty'])} {rng.choice(['in', 'not in'])} "
                f"rs.{rng.choice(['ret', 'qty'])}"
            ),
            (
                f"{rng.choice(['sum', 'max', 'count'])}(ret) by order_id {cmp} "
                f"{rng.randint(0, 3)}"
            ),
            (
                f"{rng.choice(['sum', 'avg'])}(ret) by order_id is "
                f"{rng.choice(['', 'not '])}null"
            ),
        ]
    )


def _condition(rng: random.Random, depth: int) -> str:
    roll = rng.random()
    if depth <= 0 or roll < 0.5:
        return _atom(rng, depth)
    if roll < 0.6:
        return f"not ({_condition(rng, depth - 1)})"
    return (
        f"({_condition(rng, depth - 1)}) {rng.choice(['and', 'or'])} "
        f"({_condition(rng, depth - 1)})"
    )


def _rows(model: str, text: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(TABLES)
    executor.parse_text(model)
    rows = executor.execute_text(text)[-1].fetchall()
    return sorted((tuple(r) for r in rows), key=repr)


def _assert_twins_agree(text: str) -> None:
    assert _rows(SPLIT, text) == _rows(FLAT, text), text


REGRESSIONS = [
    # concat skips a NULL argument
    "where concat(ret::string, 'z') = 'z' select order_id, item_id;",
    "where ret_label = 'r' select order_id, item_id;",
    # membership matches by identity
    "where ret not in (1, 3) select order_id, item_id;",
    "where ret in (1, null) select note, sum(qty) -> s;",
    f"{ROWSET} where ret in rs.ret select order_id, count(item_id) -> n;",
    # a flag computed after the join is not NULL on the padded row
    "where is_returned = false select order_id, item_id;",
    "where (ret = 2) or (is_returned = false) select order_id, item_id;",
    # a padded COUNT is 0
    "where count(ret) by order_id = 0 select order_id, item_id;",
    "where count(ret) by order_id < 1 select order_id;",
    # a row derivation beside a grouped sibling keeps its own grain
    "where (ret_plus > 2) or (sum(qty) by order_id > 7) select order_id, item_id;",
    (
        "where (ret_label != 'x') or (avg(ret) by order_id is null) "
        "select order_id, item_id;"
    ),
]


@pytest.mark.parametrize("text", REGRESSIONS)
def test_padded_model_matches_prejoined_model(text: str) -> None:
    _assert_twins_agree(text)


@pytest.mark.parametrize("seed", range(40))
def test_random_where_padded_matches_prejoined(seed: int) -> None:
    rng = random.Random(seed)
    text = f"{ROWSET} where {_condition(rng, rng.randint(0, 2))} {rng.choice(SHAPES)}"
    _assert_twins_agree(text)


@pytest.mark.parametrize("seed", range(40))
def test_proved_columns_are_non_null_on_kept_rows(seed: int) -> None:
    rng = random.Random(1000 + seed)
    condition = _condition(rng, rng.randint(0, 2))
    if "rs." in condition or " by order_id" in condition:
        return
    env = Environment()
    env.parse(FLAT)
    _, statements = env.parse(f"where {condition} select order_id;")
    built = Factory(environment=env).build(statements[-1].where_clause).conditional
    proved = sorted(
        address.split(".", 1)[1]
        for address in gather_non_null_proofs(built)
        if address.startswith("local.")
    )
    if not proved:
        return
    rows = _rows(
        FLAT, f"where {condition} select order_id, item_id, {', '.join(proved)};"
    )
    for row in rows:
        assert all(value is not None for value in row[2:]), (condition, proved, row)
