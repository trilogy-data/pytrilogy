"""Two row-stream outputs derived over roots at related grains (`upper(name)`
at product grain beside `quantity * 2` at order grain) recombine at FINAL as
one row stream. The axis pairing them is the fact's FK, a datasource fact no
lineage edge shows, so the roots must share a scan: sourced apart, the merge
had nothing to join on and rendered a cross product. The direct spelling
(`name, quantity * 2`) never split, since a leaf output has no reach.
"""

from tests.engine.test_filter_concept_is_a_value import _MODEL, _rows
from trilogy import Dialects

_DERIVED = _MODEL + "auto upper_name <- upper(name);\nauto double_qty <- quantity * 2;"


def test_derived_outputs_pair_on_the_fact_key():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_DERIVED)
    assert _rows(executor, "select upper_name, double_qty") == [
        ("APPLE", 10),
        ("APPLE", 40),
        ("BEAN", 16),
    ]
    assert _rows(executor, "select name, double_qty") == [
        ("apple", 10),
        ("apple", 40),
        ("bean", 16),
    ]
    sql = executor.generate_sql("select upper_name, double_qty;")[-1]
    assert "1=1" not in sql, sql
