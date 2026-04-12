from trilogy import Environment
from trilogy.ai.constants import AGGREGATE_FUNCTIONS, FUNCTIONS, RULE_PROMPT
from trilogy.ai.models import LLMRequestOptions, LLMToolDefinition
from trilogy.authoring import (
    ArrayType,
    Concept,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.models.core import DataTyped, EnumType, StructComponent


def get_trilogy_syntax_reference() -> str:
    return f"""Key Trilogy Syntax Rules:
{RULE_PROMPT}

Aggregate Functions:
{AGGREGATE_FUNCTIONS}

Functions:
{FUNCTIONS}

Valid types:
{[x.value for x in DataType]}

Some types may have additional metadata, which will help you understand them. For example, 'latitude', 'longitude' and 'currency' are all of type 'float', but have additional meaning."""


def get_trilogy_prompt(intro: str | None = None, outro: str | None = None) -> str:
    parts = []
    if intro:
        parts.append(intro)
    parts.append(get_trilogy_syntax_reference())
    if outro:
        parts.append(outro)
    return "\n\n".join(parts)


TRILOGY_CREATE_QUERY_TOOL = LLMToolDefinition(
    name="create_query",
    description="Validate a draft Trilogy query against the current environment.",
    input_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A draft Trilogy query to validate.",
            }
        },
        "required": ["query"],
        "additionalProperties": False,
    },
)


TRILOGY_QUERY_TOOL = LLMToolDefinition(
    name="submit_query",
    description="Return the final Trilogy query that answers the user's request.",
    input_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A complete valid Trilogy query ending with a semicolon.",
            },
            "reasoning": {
                "type": "string",
                "description": "Brief explanation for how the query answers the request.",
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    },
)


def create_query_request_options() -> LLMRequestOptions:
    return LLMRequestOptions(
        tools=[TRILOGY_CREATE_QUERY_TOOL, TRILOGY_QUERY_TOOL],
        require_tool=True,
    )


TRILOGY_LEAD_IN = get_trilogy_prompt(
    intro="You are a world-class expert in Trilogy, a SQL inspired language with similar syntax and a built in semantic layer. Use the following syntax description to help answer whatever questions they have. Often, they will be asking you to generate a query for them.",
    outro=(
        f"When generating a Trilogy query, use the {TRILOGY_CREATE_QUERY_TOOL.name} tool to validate draft queries. "
        f"That tool returns parser and query-processing validation feedback only; it does not execute the query. "
        f"When you are done, you must call the {TRILOGY_QUERY_TOOL.name} tool with the final query and optional reasoning. "
        "Do not wrap the query in markdown fences or triple quotes."
    ),
)


def datatype_to_field_prompt(
    datatype: (
        DataType
        | TraitDataType
        | ArrayType
        | StructType
        | MapType
        | NumericType
        | EnumType
        | DataTyped
        | StructComponent
        | int
        | float
        | str
    ),
) -> str:
    if isinstance(datatype, EnumType):
        return f"enum<{', '.join(repr(v) for v in datatype.values)}>"
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
    return f"""
Using these base and aliased calculations, derivations thereof created with valid Trilogy, and any extra context you have: {fields}, create the best valid Trilogy query to answer the following user input: "{query}". Use the {TRILOGY_CREATE_QUERY_TOOL.name} tool to validate draft queries if needed. You must finish by calling the {TRILOGY_QUERY_TOOL.name} tool with a complete Trilogy query in the query field. The final query must end with a semicolon."""
