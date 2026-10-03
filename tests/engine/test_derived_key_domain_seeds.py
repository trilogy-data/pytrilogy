"""Planning must not depend on the interpreter hash seed.

`select customer_id, name` beside an order-grain WHERE builds a row stream for
the order atoms and a customer dim peeled off it, both exposing `customer_id`;
the FINAL cover must keep the order stream whichever is built first. A second
fact binding `~customer_id` must not make the key a demanded grain key.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

SEEDS = ("0", "2", "3", "5")

_RETURNS = """
key return_id int;
property return_id.ret_amount int;
root datasource returns (return_id: return_id, customer_id: ~customer_id, ret_amount: ret_amount)
grain (return_id)
query '''select 1 as return_id, 2 as customer_id, 5 as ret_amount union all select 2, 3, 6''';
"""

QUERIES = {
    "select customer_id, name where count(order_id) by customer_id = 1 and status = 'in-transit'": [],
    "select name, customer_id where count(order_id) by customer_id = 1 and status = 'in-transit'": [],
    "select customer_id, name where count(order_id) by customer_id = 2 and status = 'in-transit'": [
        [1, "ann"]
    ],
    "select customer_id, name where count(order_id) by customer_id = 1 and status = 'delivered'": [
        [2, "bob"]
    ],
}


def _run() -> dict[str, list]:
    from tests.engine.test_derived_key_domain import _MATERIALIZED, _executor, _rows

    executor = _executor(_MATERIALIZED + _RETURNS)
    return {query: [list(r) for r in _rows(executor, query)] for query in QUERIES}


def test_rows_hold_under_every_hash_seed():
    procs = {
        seed: subprocess.Popen(
            [sys.executable, __file__],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONIOENCODING": "utf-8"},
        )
        for seed in SEEDS
    }
    for seed, proc in procs.items():
        stdout, stderr = proc.communicate(timeout=300)
        assert proc.returncode == 0, (seed, stderr)
        assert json.loads(stdout.splitlines()[-1]) == QUERIES, seed


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2]))
    print(json.dumps(_run()))
