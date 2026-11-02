"""Optimizer rule soundness (formal/Trilogy/Rewrite.lean), checked on random queries.

The model reads customers, orders, a `~` returns table and a `~` notes table, so
plans carry outer joins, padded columns, null-sensitive derivations and the
WHEREs the rules rewrite. Each case requires:

- every rule keeps the rows: the query with one rule disabled returns what it
  returns with every rule on (`strip_sound`, `left_to_inner`, `full_to_left`,
  `push_inner`, `push_padded`, `push_group_key`, ...)
- the padded model answers as the same model over one pre-joined table, whether
  its tables are inlinable (`address`) or not (`query`): a scan never hands a
  padded row the NULL of a value the join computes (`pad_null_sensitive`)
- `redundant_not_null` drops an atom only from a CTE whose rows hold no NULL
  in its column before the WHERE runs (`strip_sound`'s premise)
"""

import random
import re
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core.models.execute import CTE
from trilogy.core.optimizations.atom_contract import and_atoms
from trilogy.core.optimizations.base_optimization import OptimizationRule
from trilogy.core.optimizations.existence_having_fold import FoldExistenceIntoAggregate
from trilogy.core.optimizations.identity_group import DropIdentityGroup
from trilogy.core.optimizations.semi_join_pushdown import PushSemiJoinIntoAggregate
from trilogy.core.optimizations.strip_redundant_not_null import redundant_not_null
from trilogy.core.optimizations.value_set_join_upgrade import (
    UpgradeOuterFromKeySetEquivalence,
)
from trilogy.core.processing.condition_utility import _not_null_concept
from trilogy.core.query_processor import process_query
from trilogy.core.statements.execute import ProcessedQuery

TABLES = """
CREATE TABLE customers AS SELECT * FROM (VALUES
 (1, 'ann', 'n'), (2, NULL, 's'), (3, 'cat', NULL), (4, 'dan', 'n')) t(c, name, region);
CREATE TABLE orders AS SELECT * FROM (VALUES
 (10, 1, 5), (11, 1, NULL), (12, 2, 7), (13, 3, 1), (14, 4, 2), (15, 4, 3)) t(o, c, amt);
CREATE TABLE returns AS SELECT * FROM (VALUES
 (10, 'dmg', 1), (12, NULL, 2), (13, 'late', NULL)) t(o, reason, qty);
CREATE TABLE notes AS SELECT * FROM (VALUES (1, 'vip'), (3, NULL), (4, 'x')) t(c, note);
CREATE TABLE flat AS
SELECT o.o, o.c, o.amt, c.name, c.region, r.reason, r.qty, n.note
FROM orders o JOIN customers c ON o.c = c.c
LEFT JOIN returns r ON o.o = r.o LEFT JOIN notes n ON o.c = n.c;
"""

HEAD = """
key cust_id int;
key order_id int;
property cust_id.name string?;
property cust_id.region string?;
property cust_id.note string?;
property order_id.amount int?;
property order_id.reason string?;
property order_id.rqty int?;
auto returned <- rqty is not null;
auto label <- coalesce(reason, 'none');
auto small <- rqty not in (2, 9);
auto bucket <- case when rqty > 1 then 'big' else 'small' end;
"""

SPLIT = HEAD + """
datasource customers (c: cust_id, name: name, region: region)
grain (cust_id) address customers;
datasource orders (o: order_id, c: cust_id, amt: amount)
grain (order_id) address orders;
datasource returns (o: ~order_id, reason: reason, qty: rqty)
grain (order_id) address returns;
datasource notes (c: ~cust_id, note: note)
grain (cust_id) address notes;
"""

# a query address is never inlined, so its scan computes what it is asked for
SPLIT_QUERY = re.sub(
    r"address (\w+);", lambda m: f"query '''select * from {m.group(1)}''';", SPLIT
)

FLAT = HEAD + """
datasource flat (o: order_id, c: cust_id, amt: amount, name: name, region: region,
    reason: reason, qty: rqty, note: note)
grain (order_id) address flat;
"""

COLS = ["cust_id", "order_id", "name", "region", "note", "amount", "reason", "rqty"]
INTS = ["cust_id", "order_id", "amount", "rqty"]
STRS = ["name", "region", "note", "reason", "label", "bucket"]
FLAGS = ["returned", "small"]

RULES = [
    "strip_redundant_not_null",
    "upgrade_condition_joins",
    "upgrade_outer_key_set_equivalence",
    "predicate_pushdown",
    "datasource_inlining",
    "merge_aggregate",
    "drop_identity_group",
    "fold_existence_into_aggregate",
    "push_semi_join_into_aggregate",
    "merge_irrelevant_group_by",
]

# A shape each rule fires on, with the addressing it fires under, so its
# ablation compares a plan it changed (`Rewrite.identity_group_exact`,
# `existence_fold_exact`, `semi_join_exact`, `matched_left_is_inner`).
FIRES = {
    DropIdentityGroup: (
        SPLIT,
        "where (amount > 3) or (name is null) select rqty, note, order_id;",
    ),
    FoldExistenceIntoAggregate: (
        SPLIT,
        "where (amount > 0) and (sum(amount) by cust_id > 4) select name;",
    ),
    PushSemiJoinIntoAggregate: (
        SPLIT,
        (
            "with rs as where (rqty > 11) and ((reason is not null) or "
            "(count(order_id) by cust_id = 2)) select cust_id, order_id, reason;\n"
            "select rs.cust_id, count(rs.order_id) -> n;"
        ),
    ),
    UpgradeOuterFromKeySetEquivalence: (
        SPLIT,
        "where region is null select reason, rqty, note;",
    ),
}


def _atom(rng: random.Random) -> str:
    choices = [
        lambda: f"{rng.choice(COLS)} is not null",
        lambda: f"{rng.choice(COLS)} is not null",
        lambda: f"{rng.choice(COLS + FLAGS)} is null",
        lambda: f"{rng.choice(INTS)} > {rng.randint(0, 11)}",
        lambda: f"{rng.choice(INTS)} != {rng.randint(0, 11)}",
        lambda: (
            f"{rng.choice(STRS)} = "
            f"'{rng.choice(['ann', 'n', 'dmg', 'none', 'vip', 'big', 'small'])}'"
        ),
        lambda: f"coalesce({rng.choice(INTS)}, 0) = 0",
        lambda: f"{rng.choice(FLAGS)} = {rng.choice(['true', 'false'])}",
        lambda: f"count(order_id) by cust_id {rng.choice(['>', '='])} {rng.randint(0, 2)}",
        lambda: f"sum(amount) by cust_id > {rng.randint(0, 8)}",
        lambda: f"{rng.choice(INTS)} in ({rng.randint(0, 12)}, {rng.randint(0, 12)})",
    ]
    return rng.choice(choices)()


def _condition(rng: random.Random, depth: int) -> str:
    roll = rng.random()
    if depth <= 0 or roll < 0.45:
        return _atom(rng)
    if roll < 0.55:
        return f"not ({_condition(rng, depth - 1)})"
    op = "and" if roll < 0.85 else "or"
    return f"({_condition(rng, depth - 1)}) {op} ({_condition(rng, depth - 1)})"


def _query(rng: random.Random) -> str:
    cols = rng.sample(COLS, rng.randint(1, 3))
    where = _condition(rng, rng.randint(0, 2))
    shape = rng.random()
    if shape < 0.4:
        return f"where {where} select {', '.join(cols)};"
    if shape < 0.65:
        group = rng.choice(["cust_id", "region", "name"])
        agg = rng.choice(
            ["count(order_id)", "sum(amount)", "max(rqty)", "count(reason)"]
        )
        having = (
            f" having n {rng.choice(['>', '<'])} {rng.randint(0, 6)}"
            if rng.random() < 0.3
            else ""
        )
        return f"where {where} select {group}, {agg} -> n{having};"
    if shape < 0.85:
        body = ", ".join(sorted({"order_id", "cust_id", *cols}))
        tail = rng.choice(
            [
                "select rs.order_id;",
                "select rs.cust_id, count(rs.order_id) -> n;",
                f"where rs.order_id > {rng.randint(9, 15)} select rs.order_id, rs.cust_id;",
            ]
        )
        return f"with rs as where {where} select {body};\n{tail}"
    return f"where {where} select {', '.join(cols)}, count(order_id) by cust_id -> n;"


def _rows(model: str, text: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    try:
        executor.execute_raw_sql(TABLES)
        executor.parse_text(model)
        rows = executor.execute_text(text)[-1].fetchall()
    finally:
        executor.close()
    return sorted((tuple(r) for r in rows), key=repr)


@contextmanager
def _disabled(rule: str) -> Iterator[None]:
    setattr(CONFIG.optimizations, rule, False)
    try:
        yield
    finally:
        setattr(CONFIG.optimizations, rule, True)


REGRESSIONS = [
    # a null-sensitive flag over the `~` returns table: the order no return
    # lists reads false, computed after the join, not NULL from the scan
    "where returned = false select order_id;",
    "where returned is null select order_id;",
    "where small = true select order_id, amount;",
    "where returned = false or amount > 100 select order_id;",
    # the FINAL WHERE reads its filter-only arg off the deduplicated row side
    "where returned = true select reason, count(order_id) by cust_id -> n;",
    # an OR whose disjuncts each prove a different returns column
    "where rqty > 1 or reason = 'dmg' select order_id, amount;",
    "where (reason = 'late' or rqty = 2) and note is null select order_id;",
    # a disjunct reading only orders keeps the order no return lists
    "where rqty > 1 or amount = 3 select order_id;",
    # `is null` on the padded side is not pushed into the returns scan
    "where reason is null select order_id, cust_id;",
    "where note is null and region = 'n' select cust_id, count(order_id) -> n;",
    # an atom true on an empty group is not decided by the groups it kept
    "where count(order_id) by cust_id = 0 select reason, count(order_id) by cust_id -> n;",
    "where count(order_id) by cust_id != 2 select reason, cust_id, count(order_id) by cust_id -> n;",
    "where count(order_id) by cust_id is null select reason, count(order_id) by cust_id -> n;",
    # a gated dimension peel pairs back on the key it hid or never scanned
    "where count(order_id) by cust_id > 1 select amount, name;",
    "where count(order_id) by cust_id > 1 select order_id, reason, name;",
    "where (region = 'n' and count(order_id) by cust_id = 1) or amount = 3 select reason, region;",
    # a WHERE on an order column keeps the customer the `~` notes table lacks
    "where returned = true select cust_id, note;",
    "where order_id in (10, 12) select cust_id, note;",
    "where returned = true select region, note;",
    "with rs as where count(order_id) by cust_id = 1 select amount, cust_id, note, order_id, rqty;\nselect rs.order_id;",
    # a `~` lookup beside its complete dimension keeps the dimension's members
    "where sum(amount) by cust_id > 1 select region, reason, note;",
    "where ((sum(amount) by cust_id > 0) or (reason is not null)) and (cust_id != 10) select note, amount;",
    "where (name is not null) and ((bucket = 'big') or (sum(amount) by cust_id > 4)) select cust_id, note, order_id;",
    # an inlined table and a CTE collapsed into it no longer share one name
    "where returned = false select rqty, region, count(order_id) by cust_id -> n;",
    # a row-level argument beside a gate pairs on the gate's grain, not ON 1=1
    "where (rqty > 1) or (sum(amount) by cust_id > 8) select name;",
    "where (rqty > 1) or (count(order_id) by cust_id > 1) select region;",
    "where (reason = 'dmg') or (sum(amount) by cust_id > 8) select region;",
    "where (rqty > 1) or (sum(amount) by cust_id > 8) select name, note;",
    "where (reason = 'dmg') or (count(order_id) by cust_id > 1) select region, note, name;",
    # a proof on the key forces no match on a `~` lookup keyed by it
    "where (order_id > 8) and (cust_id != 8) select name, order_id, note;",
    # a FULL to the `~` notes pads only notes, so the filtered aggregate is the
    # population even under an atom true on the all-NULL row
    "where not (order_id != 10) select note, region, count(order_id) by cust_id -> n;",
    "where amount is null select note, name, sum(amount) by cust_id -> n;",
    # `note is null` runs after the notes padding it accepts, beside another
    # `~` table's span
    "where note is null and rqty is null select note, order_id, count(order_id) by cust_id -> n;",
    "where note is null and small = true select note, cust_id, count(order_id) by cust_id -> n;",
    # two gates on the customer beside a row atom: the pre-joined model plans
    # (no keyless join), and a gate hosted on the `~` notes peel keeps the
    # customer with no note
    "where ((count(order_id) by cust_id = 0) and (reason is not null)) and (sum(amount) by cust_id > 8) select note;",
    "where ((count(order_id) by cust_id > 1) or (rqty > 1)) and (sum(amount) by cust_id > 8) select note;",
    "where ((sum(amount) by cust_id > 8) or (reason is null)) and (sum(amount) by cust_id > 4) select region;",
    "where (amount > 0) and (sum(amount) by cust_id > 4) select note;",
    # a filter fed into the bare `~` notes scan keeps the customer it lacks
    "where (returned = true) or (sum(amount) by cust_id > 4) select note;",
    # a selected customer column an order-level atom also reads stays on the
    # order rows the atom filters
    "where (amount = 4) or (note is not null) select note, count(order_id) by cust_id -> n;",
    "where (rqty in (4, 4)) or ((amount is not null) or (note is not null)) select note, count(order_id) by cust_id -> n;",
    "where (not (count(order_id) by cust_id > 1)) and ((order_id > 9) or (name is not null)) select name, note, cust_id, count(order_id) by cust_id -> n;",
    "where (name is null or name = 'ann') and (not (sum(amount) by cust_id > 6)) select name, region, count(order_id) by region -> r;",
    # a gate true on a customer with no rows is re-applied beside the order
    # rows that carry the customer's columns
    "where ((rqty > 1) or (name is null)) and (not (sum(amount) by cust_id > 6)) select name, cust_id, count(order_id) -> n;",
    "where ((reason is null) or (region = 'n')) and (not (sum(amount) by cust_id > 6)) select region, sum(amount) by cust_id -> n;",
]

# An atom over an order column and a customer column, beside selected customer
# columns, a per-customer aggregate and optionally a gate.
MIXED_ATOMS = [
    f"where (({order}) {join} ({customer})){gate} select {columns};"
    for order in ["amount = 4", "reason is null"]
    for customer in ["note is not null", "name is null", "region = 'n'"]
    for join in ["or", "and"]
    for gate in ["", " and (not (sum(amount) by cust_id > 6))"]
    for columns in [
        "name, note, count(order_id) by cust_id -> n",
        "region, sum(amount) by cust_id -> n",
    ]
]


@pytest.mark.parametrize("model", [SPLIT, SPLIT_QUERY], ids=["table", "query"])
@pytest.mark.parametrize("text", REGRESSIONS)
def test_padded_model_matches_prejoined(model: str, text: str) -> None:
    assert _rows(model, text) == _rows(FLAT, text), text


@pytest.mark.parametrize("model", [SPLIT, SPLIT_QUERY], ids=["table", "query"])
@pytest.mark.parametrize("text", MIXED_ATOMS)
def test_mixed_atom_matches_prejoined(model: str, text: str) -> None:
    assert _rows(model, text) == _rows(FLAT, text), text


@pytest.mark.parametrize("model", [SPLIT, SPLIT_QUERY], ids=["table", "query"])
@pytest.mark.parametrize("seed", range(30))
def test_random_padded_model_matches_prejoined(model: str, seed: int) -> None:
    text = _query(random.Random(seed))
    assert _rows(model, text) == _rows(FLAT, text), text


@pytest.mark.parametrize("rule", RULES)
@pytest.mark.parametrize("text", REGRESSIONS)
def test_rule_keeps_rows(rule: str, text: str) -> None:
    expected = _rows(SPLIT_QUERY, text)
    with _disabled(rule):
        assert _rows(SPLIT_QUERY, text) == expected, (rule, text)


@pytest.mark.parametrize("rule", list(FIRES), ids=lambda r: r.__name__)
def test_rule_fires_and_keeps_rows(
    rule: type[OptimizationRule], monkeypatch: pytest.MonkeyPatch
) -> None:
    model, text = FIRES[rule]
    fired: list[bool] = []
    optimize = rule.optimize

    def counted(self, cte, inverse_map):  # type: ignore[no-untyped-def]
        result = optimize(self, cte, inverse_map)
        fired.append(result[0])
        return result

    monkeypatch.setattr(rule, "optimize", counted)
    assert _rows(model, text) == _rows(FLAT, text), text
    assert any(fired), text


@pytest.mark.parametrize("seed", range(30))
def test_random_rule_keeps_rows(seed: int) -> None:
    text = _query(random.Random(500 + seed))
    expected = _rows(SPLIT, text)
    for rule in RULES:
        with _disabled(rule):
            assert _rows(SPLIT, text) == expected, (rule, text)


def _null_probe(processed: ProcessedQuery, cte: CTE, address: str) -> str:
    """`cte`'s rows before its WHERE, counted where `address` is NULL."""
    executor = Dialects.DUCK_DB.default_executor()
    dialect = executor.generator
    concept = next(c for c in cte.output_columns if c.address == address)
    ctes = dialect.generate_ctes(processed)
    body = ",\n".join(f'"{c.name}" as ({c.statement})' for c in ctes)
    executor.close()
    return (
        f'WITH {body}\nSELECT count(*) FROM "{cte.name}" '
        f'WHERE "{cte.name}"."{concept.safe_address}" IS NULL'
    )


def _strip_candidates(processed: ProcessedQuery) -> list[tuple[CTE, str]]:
    out = []
    for cte in processed.ctes:
        if not isinstance(cte, CTE) or cte.condition is None:
            continue
        for atom in and_atoms(cte.condition):
            concept = _not_null_concept(atom)
            if concept is None or not redundant_not_null(cte, atom):
                continue
            if any(c.address == concept.address for c in cte.output_columns):
                out.append((cte, concept.address))
    return out


@pytest.mark.parametrize("model", [SPLIT, SPLIT_QUERY], ids=["table", "query"])
@pytest.mark.parametrize("seed", range(30))
def test_stripped_column_never_null_before_where(model: str, seed: int) -> None:
    rng = random.Random(900 + seed)
    column = rng.choice(COLS)
    others = rng.sample([c for c in COLS if c != column], 2)
    text = (
        f"where {column} is not null and {_condition(rng, 1)} "
        f"select {', '.join(others)}, {column};"
    )
    env = Environment()
    env.parse(model)
    _, statements = env.parse(text)
    with _disabled("strip_redundant_not_null"):
        processed = process_query(env, statements[-1])
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(TABLES)
    try:
        for cte, address in _strip_candidates(processed):
            kept, hidden = cte.condition, cte.hidden_concepts
            cte.condition, cte.hidden_concepts = None, hidden - {address}
            try:
                probe = _null_probe(processed, cte, address)
            finally:
                cte.condition, cte.hidden_concepts = kept, hidden
            assert executor.execute_raw_sql(probe).fetchone()[0] == 0, (
                text,
                cte.name,
                address,
            )
    finally:
        executor.close()


@pytest.mark.parametrize("model", [SPLIT, FLAT], ids=["split", "flat"])
def test_filtered_value_collapses_to_its_key(model: str) -> None:
    """`Rewrite.grouped_filter_fans_out`: a filtered value a parent computed per
    order row is grouped at its customer key with MAX, not as a group key, or
    customer 1 (one order over 4, one not) counts twice (TPC-DS q97)."""
    text = (
        "auto big <- cust_id ? amount > 4;\n"
        "select sum(case when big is not null then 1 else 0 end) -> n, "
        "count(cust_id) -> m;"
    )
    with _disabled("merge_aggregate"):
        assert _rows(model, text) == [(2, 4)]
