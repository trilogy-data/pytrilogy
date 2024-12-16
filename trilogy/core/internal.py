from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import FunctionType, Purpose
from trilogy.core.models import Concept, DataType, Function, Grain

DEFAULT_CONCEPTS = {
    ALL_ROWS_CONCEPT: Concept(
        name=ALL_ROWS_CONCEPT,
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.INTEGER,
        purpose=Purpose.CONSTANT,
        grain=Grain(),
        lineage=Function(
            operator=FunctionType.CONSTANT,
            arguments=[1],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.CONSTANT,
        ),
    ),
    "concept_name": Concept(
        name="concept_name",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
    ),
    "datasource": Concept(
        name="datasource",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
    ),
    "query_text": Concept(
        name="query_text",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
    ),
}
