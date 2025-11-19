from trilogy import Environment
from trilogy.ai.constants import AGGREGATE_FUNCTIONS, FUNCTIONS, RULE_PROMPT
from trilogy.authoring import (
    ArrayType,
    Concept,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.models.core import DataTyped, StructComponent

TRILOGY_LEAD_IN = f'''You are a world-class expert in Trilogy, a SQL inspired language with similar syntax and a built in semantic layer. Use the following syntax description to help answer whatever questions they have. Often, they will be asking you to generate a query for them.

Key Trilogy Syntax Rules:
{RULE_PROMPT}

Aggregate Functions:
{AGGREGATE_FUNCTIONS}

Functions:  
{FUNCTIONS}

Valid types:  
{[x.value for x in DataType]}

Some types may have additional metadata, which will help you understand them. For example, 'latitude', 'longitude' and 'currency' are all of type 'float', but have additional meaning.

For any response to the user, use this format -> put your actual response within triple double quotes with thinking and justification before it, in this format (replace placeholders with relevant content): Reasoning: {{reasoning}} """{{response}}"""
'''


def datatype_to_field_prompt(
    datatype: (
        DataType
        | TraitDataType
        | ArrayType
        | StructType
        | MapType
        | NumericType
        | DataTyped
        | StructComponent
        | int
        | float
        | str
    ),
) -> str:
    if isinstance(datatype, TraitDataType):
        return f"{datatype_to_field_prompt(datatype.type)}({','.join(datatype.traits)})"
    if isinstance(datatype, ArrayType):
        return f"ARRAY<{datatype_to_field_prompt(datatype.type)}>"
    if isinstance(datatype, StructType):
        instantiated = []
        for name, field_type in datatype.field_types.items():
            if isinstance(field_type, StructComponent):
                instantiated.append(f"{datatype_to_field_prompt(field_type.type)}")
            else:
                instantiated.append(f"{name}: {datatype_to_field_prompt(field_type)}")
        fields_str = ", ".join(instantiated)
        return f"STRUCT<{fields_str}>"
    if isinstance(datatype, MapType):
        return f"MAP<{datatype_to_field_prompt(datatype.key_type)}, {datatype_to_field_prompt(datatype.value_type)}>"
    if isinstance(datatype, NumericType):
        return f"NUMERIC({datatype.precision}, {datatype.scale})>"
    if isinstance(datatype, DataTyped):
        return datatype_to_field_prompt(datatype.output_datatype)
    if isinstance(datatype, StructComponent):
        return f"{datatype.name}: {datatype_to_field_prompt(datatype.type)}"
    if isinstance(datatype, (int, float, str)):
        return f"{datatype}"
    return f"{datatype.value}"


def concepts_to_fields_prompt(concepts: list[Concept]) -> str:
    return ", ".join(
        [
            f"[name: {c.address} | type: {datatype_to_field_prompt(c.datatype)}]"
            for c in concepts
        ]
    )


def create_query_prompt(query: str, environment: Environment) -> str:
    fields = concepts_to_fields_prompt(list(environment.concepts.values()))
    return f'''
Using these base and aliased calculations, derivations thereof created with valid Trilogy, and any extra context you have: {fields}, create the best valid Trilogy query to answer the following user input: "{query}" Return the query within triple double quotes with your thinking and justification before it, so of this form as a jinja template: Reasoning: {{reasoning_placeholder}} """{{trilogy}}""". Example: Because the user asked for sales by year, and revenue is the best sales related field available, we can aggregate revenue by year: """SELECT order.year, sum(revenue) as year_revenue order by order.year asc;"""'''
