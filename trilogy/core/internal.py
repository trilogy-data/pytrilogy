from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import FunctionType, Purpose, Derivation
from trilogy.core.author_models import Concept, DataType, Function, Grain, Granularity

DEFAULT_CONCEPTS = {
    ALL_ROWS_CONCEPT: Concept(
        name=ALL_ROWS_CONCEPT,
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.INTEGER,
        purpose=Purpose.CONSTANT,
        derivation = Derivation.CONSTANT,
        grain=Grain(),
        lineage=Function(
            operator=FunctionType.CONSTANT,
            arguments=[1],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.CONSTANT,
            output_grain = Grain()
        ),
        granularity = Granularity.ALL_ROWS
    ),
    "concept_name": Concept(
        name="concept_name",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        derivation = Derivation.CONSTANT,
        grain=Grain(),
                granularity = Granularity.MULTI_ROW
    ),
    "datasource": Concept(
        name="datasource",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        derivation = Derivation.CONSTANT,
        grain=Grain(),
        granularity = Granularity.MULTI_ROW
    ),
    "query_text": Concept(
        name="query_text",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        derivation = Derivation.CONSTANT,
        grain=Grain(),
        granularity= Granularity.SINGLE_ROW
    ),
}
