from preql.core.models import Environment
from preql.core.enums import Purpose
from preql import parse, Executor
import pytest


def test_ambiguous_resolution(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    store_id, 
    product_id,
;"""

    _, statements = parse(test_select, test_environment)
    statement = statements[-1]

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert 1 == 0

