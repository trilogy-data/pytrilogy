from preql.core.models import Concept
from preql.core.enums import DataType, Purpose

INTERNAL_NAMESPACE = "__preql_internal"


DEFAULT_CONCEPTS = {
    "concept_name": Concept(
        name="concept_name",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
    ),
    "datasource": Concept(
        name="datasource",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
    ),
    "query_text": Concept(
        name="query_text",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
    ),
}
