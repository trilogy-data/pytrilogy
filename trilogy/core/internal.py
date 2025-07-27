from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import Derivation, FunctionType, Granularity, Purpose
from trilogy.core.models.author import Concept, Function, Grain
from trilogy.core.models.core import DataType

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
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
    "concept_address": Concept(
        name="concept_address",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
    "concept_datatype": Concept(
        name="concept_datatype",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
    "concept_description": Concept(
        name="concept_description",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
    "datasource": Concept(
        name="datasource",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
    "query_text": Concept(
        name="query_text",
        namespace=INTERNAL_NAMESPACE,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        grain=Grain(),
        granularity=Granularity.SINGLE_ROW,
        derivation=Derivation.CONSTANT,
    ),
}
