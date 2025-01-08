from trilogy.core.enums import Purpose
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.author import Concept
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import EnvironmentConceptDict
from trilogy.parser import parse


def test_undefined_concept_query(test_environment):
    q = "SELECT orid LIMIT 10;"
    try:
        parse(q, test_environment)
    except UndefinedConceptException as e:
        assert e.suggestions == ["order_id"]

    q = "SELECT order_ct LIMIT 10;"
    try:
        parse(q, test_environment)
    except UndefinedConceptException as e:
        assert len(e.suggestions) == 3


def test_undefined_concept_dict():
    env = EnvironmentConceptDict()
    env["order_id"] = Concept(
        name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    try:
        env["zzz"]
    except UndefinedConceptException as e:
        assert e.suggestions == []
        assert "suggestions" not in e.message.lower()

    try:
        env["orid"]
    except UndefinedConceptException as e:
        assert e.suggestions == ["order_id"]
        assert "suggestions" in e.message.lower()
        assert "order_id" in e.message.lower()
