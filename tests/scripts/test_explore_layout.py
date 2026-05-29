"""Snapshot test for `trilogy explore` groups view. Locks in the nested
layout: properties grouped under the key they're defined against, compound
grains in their own ``props @ <…>`` block, metrics flat below.

The fixture ``explore_fixture.preql`` lives next to this file and is
intentionally exhaustive — touch it with intent."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.explore import explore

FIXTURE = Path(__file__).parent / "explore_fixture.preql"

EXPECTED = """\
Available Concepts (1 namespaces, 11 concepts)

# (root)
customer_id : integer
    KEY
    Surrogate key identifying a customer.
name : string
    PROP @customer_id
    Customer's full legal name.
signup_date : date
    PROP @customer_id
    Day the customer joined.
tier : string
    PROP @customer_id
    Loyalty tier ('gold' | 'silver' | 'bronze').
order_id : integer
    KEY
    Surrogate key identifying an order.
placed_at : datetime
    PROP @order_id
    Time the order was created.
status : string
    PROP @order_id
    Lifecycle stage ('open' | 'shipped' | 'cancelled').
quantity : integer
    PROP @<customer_id, order_id>
    Units ordered on this line.
unit_price : float
    PROP @<customer_id, order_id>
    Per-unit price charged.
avg_unit_price : float
    METRIC
    Mean unit price across all lines.
total_revenue : float
    METRIC
    Sum of line revenue.
"""


def _run_explore(*extra_args: str) -> str:
    runner = CliRunner()
    result = runner.invoke(explore, [str(FIXTURE), *extra_args])
    assert result.exit_code == 0, result.output
    # Strip rich/click coloring + a leading blank from `print_info`.
    return result.output.lstrip("\n")


def test_groups_view_nests_props_under_key():
    assert _run_explore() == EXPECTED


def test_concepts_view_includes_description_column():
    """`--show concepts` keeps the flat table form but now carries the
    description column populated from Metadata."""
    out = _run_explore("--show", "concepts")
    assert "description" in out
    assert "Customer's full legal name." in out
    assert "Surrogate key identifying a customer." in out
