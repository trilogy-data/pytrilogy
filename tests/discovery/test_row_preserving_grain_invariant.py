"""Gate: a row-preserving node declares the rows it reads.

A node that only projects or filters emits its parents' rows, so it must not
declare a grain its parents don't already satisfy. Deriving one from its own
outputs states the grain the projection *selects*, which is only the row grain
if something deduped to it; nothing did, so consumers read the node as already
deduped and skip the GROUP BY that would make it true.

That is how `select dim as alias` with a membership predicate lost its group:
the alias stacked a rename over an existence wrapper that claimed the narrowed
projection's grain. The census below was 149 before `inherits_parent_grain`.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes.base_node import StrategyNode

TPCH = Path(__file__).parent.parent / "modeling" / "tpc_h"

MODEL = """
key item_sk int;
property item_sk.item_id string;
property item_sk.price int;
datasource items (s: item_sk, i: item_id, p: price) grain (item_sk)
query '''
select 1 s, 'I1' i, 70 p union all select 2 s, 'I2' i, 80 p
''';

key inv_id int;
property inv_id.inv_item int;
property inv_id.qoh int;
datasource inventory (d: inv_id, s: inv_item, q: qoh) grain (inv_id)
query '''
select 1 d, 1 s, 200 q union all select 2 d, 1 s, 300 q
union all select 3 d, 2 s, 250 q
''';
merge inv_item into item_sk;

auto cheap_items <- item_sk ? price between 68 and 98;
"""

SHAPES = [
    "where qoh >= 100 and item_sk in cheap_items select item_id as item_code;",
    "where qoh >= 100 and item_sk not in cheap_items select item_id as item_code;",
    "where qoh >= 100 and item_sk in cheap_items select item_id as item_code, price;",
    "where qoh >= 100 select item_id as item_code having count(inv_id) > 0;",
    "select item_id as item_code, sum(qoh) as total;",
]


@pytest.fixture
def violations(monkeypatch) -> list[str]:
    """Collect row-preserving nodes whose declared grain their parents don't satisfy."""
    found: list[str] = []
    original = StrategyNode._resolve

    def audited(self: StrategyNode):
        resolved = original(self)
        if not self.inherits_parent_grain or self.force_group or not self.parents:
            return resolved
        if resolved.group_required:
            return resolved
        parent_grain = BuildGrain()
        for parent in self.parents:
            parent_grain += parent.resolve().grain
        if parent_grain.components and not parent_grain.issubset(resolved.grain):
            found.append(
                f"{type(self).__name__} declares {resolved.grain} over {parent_grain}"
            )
        return resolved

    monkeypatch.setattr(StrategyNode, "_resolve", audited)
    return found


def _render(working_path: Path, text: str) -> None:
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=working_path)
    )
    engine.generate_sql(text)


def test_tpch_corpus_holds_row_grain_invariant(violations: list[str]):
    for query in sorted(TPCH.glob("query*.preql")):
        _render(TPCH, query.read_text())
    assert violations == []


@pytest.mark.parametrize("shape", SHAPES)
def test_membership_and_alias_shapes_hold_row_grain_invariant(
    tmp_path: Path, shape: str, violations: list[str]
):
    _render(tmp_path, MODEL + shape)
    assert violations == []
