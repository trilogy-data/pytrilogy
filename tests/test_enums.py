from trilogy.core.enums import BooleanOperator, Boolean


def test_boolean_operator():
    assert BooleanOperator("AND") == BooleanOperator.AND
    assert BooleanOperator("and") == BooleanOperator.AND


def test_boolean():
    assert Boolean("TRUE") == Boolean.TRUE
    assert Boolean("true") == Boolean.TRUE
    assert Boolean(True) == Boolean.TRUE
