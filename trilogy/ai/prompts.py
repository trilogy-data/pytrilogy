from trilogy import Environment
from trilogy.ai.constants import AGGREGATE_FUNCTIONS, FUNCTIONS, RULE_PROMPT
from trilogy.authoring import Concept, DataType

TRILOGY_LEAD_IN = f'''You are a world-class expert in Trilogy, a SQL inspired language with similar syntax and a built in semantic layer. Use the following syntax description to help answer whatever questions they have. Often, they will be asking you to generate a query for them.

Key Trilogy Syntax Rules:
{RULE_PROMPT}

Aggregate Functions:
{AGGREGATE_FUNCTIONS}

Functions:  
{FUNCTIONS}

Valid types:  
{[x.value for x in DataType]}

For any response to the user, use this format -> put your actual response within triple double quotes with thinking and justification before it, in this format (replace placeholders with relevant content): Reasoning: {{reasoning}} """{{response}}"""
'''


def concepts_to_fields_prompt(concepts: list[Concept]) -> str:
    return ", ".join([f"[name: {c.address} | type: {c.datatype}" for c in concepts])


def create_query_prompt(query: str, environment: Environment) -> str:
    fields = concepts_to_fields_prompt(list(environment.concepts.values()))
    return f'''
Using these base and aliased calculations, derivations thereof created with valid Trilogy, and any extra context you have: {fields}, create the best valid Trilogy query to answer the following user input: "{query}" Return the query within triple double quotes with your thinking and justification before it, so of this form as a jinja template: Reasoning: {{reasoning_placeholder}} """{{trilogy}}""". Example: Because the user asked for sales by year, and revenue is the best sales related field available, we can aggregate revenue by year: """SELECT order.year, sum(revenue) as year_revenue order by order.year asc;"""'''
