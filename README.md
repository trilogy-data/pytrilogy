# Trilogy
**SQL with superpowers for analytics**

[![Website](https://img.shields.io/badge/INTRO-WEB-orange?)](https://trilogydata.dev/)
[![Discord](https://img.shields.io/badge/DISCORD-CHAT-red?logo=discord)](https://discord.gg/Z4QSSuqGEd)
[![PyPI version](https://badge.fury.io/py/pytrilogy.svg)](https://badge.fury.io/py/pytrilogy)

The Trilogy language is an experiment in better SQL for analytics - a streamlined SQL that replaces tables/joins with a lightweight semantic binding layer and provides easy reuse and composability. It compiles to SQL - making it easy to debug or integrate into existing workflows - and can be run against any supported SQL backend.

[pytrilogy](https://github.com/trilogy-data/pytrilogy) is the reference implementation, written in Python.

## What Trilogy Gives You

- **Speed** - write faster, with concise, powerful syntax
- **Efficiency** - write less SQL, and reuse what you do
- **Fearless refactoring** - change models without breaking queries
- **Testability** - built-in testing patterns with query fixtures
- **Easy to use** - for humans and LLMs alike

Trilogy is especially powerful for data consumption, providing a rich metadata layer that makes creating, interpreting, and visualizing queries easy and expressive.


We recommend starting with the studio to explore Trilogy. For integration, `pytrilogy` can be run locally to parse and execute trilogy model [.preql] files using the `trilogy` CLI tool, or can be run in python by importing the `trilogy` package.


## Quick Start

> [!TIP]
> **Try it now:** [Open-source studio](https://trilogydata.dev/trilogy-studio-core/) | [Interactive demo](https://trilogydata.dev/demo/) | [Documentation](https://trilogydata.dev/)

**Install**
```bash
pip install pytrilogy
```

**Save in hello.preql**
```sql
const prime <- unnest([2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);

def cube_plus_one(x) -> (x * x * x + 1);

WHERE 
    prime_cubed_plus_one % 7 = 0
SELECT
    prime,
    @cube_plus_one(prime) as prime_cubed_plus_one
ORDER BY
    prime asc
LIMIT 10;
```

**Run it in DuckDB**
```bash
trilogy run hello.preql duckdb
```

## Trilogy is Easy to Write
For humans *and* AI. Enjoy flexible, one-shot query generation without any DB access or security risks. 

(full code in the python API section.)

```python
query = text_to_query(
    executor.environment,
    "number of flights by month in 2005",
    Provider.OPENAI,
    "gpt-5-chat-latest",
    api_key,
)

# get a ready to run query
print(query)
# typical output
'''where local.dep_time.year = 2020  
select
    local.dep_time.month,
    count(local.id2) as number_of_flights
order by
    local.dep_time.month asc;'''
```

## Goals

Versus SQL, Trilogy aims to: 

**Keep:**
- Correctness
- Accessibility

**Improve:**
- Simplicity
- Refactoring/maintainability
- Reusability/composability
- Expressivness

**Maintain:**
- Acceptable performance

## Backend Support

| Backend | Status | Notes |
|---------|--------|-------|
| **BigQuery** | Core | Full support |
| **DuckDB** | Core | Full support |
| **Snowflake** | Core | Full support |
| **SQL Server** | Experimental | Limited testing |
| **Presto** | Experimental | Limited testing |

## Examples

### Hello World

Save the following code in a file named `hello.preql`

```python
# semantic model is abstract from data

type word string; # types can be used to provide expressive metadata tags that propagate through dataflow

key sentence_id int;
property sentence_id.word_one string::word; # comments after a definition 
property sentence_id.word_two string::word; # are syntactic sugar for adding
property sentence_id.word_three string::word; # a description to it

# comments in other places are just comments

# define our datasource to bind the model to data
# for most work, you can import something already defined
# testing using query fixtures is a common pattern
datasource word_one(
    sentence: sentence_id,
    word:word_one
)
grain(sentence_id)
query '''
select 1 as sentence, 'Hello' as word
union all
select 2, 'Bonjour'
''';

datasource word_two(
    sentence: sentence_id,
    word:word_two
)
grain(sentence_id)
query '''
select 1 as sentence, 'World' as word
union all
select 2 as sentence, 'World'
''';

datasource word_three(
    sentence: sentence_id,
    word:word_three
)
grain(sentence_id)
query '''
select 1 as sentence, '!' as word
union all
select 2 as sentence, '!'
''';

def concat_with_space(x,y) -> x || ' ' || y;

# an actual select statement
# joins are automatically resolved between the 3 sources
with sentences as
select sentence_id, @concat_with_space(word_one, word_two) || word_three as text;

WHERE 
    sentences.sentence_id in (1,2)
SELECT
    sentences.text
;
```

**Run it:**
```bash
trilogy run hello.preql duckdb
```

![UI Preview](hello-world.png)

### Python SDK Usage

Trilogy can be run directly in python through the core SDK. Trilogy code can be defined and parsed inline or parsed out of files.

A BigQuery example, similar to the [BigQuery quickstart](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console):

```python
from trilogy import Dialects, Environment

environment = Environment()

environment.parse('''
key name string;
key gender string;
key state string;
key year int;
key yearly_name_count int; int;

datasource usa_names(
    name:name,
    number:yearly_name_count,
    year:year,
    gender:gender,
    state:state
)
address `bigquery-public-data.usa_names.usa_1910_2013`;
''')

executor = Dialects.BIGQUERY.default_executor(environment=environment)

results = executor.execute_text('''
WHERE
    name = 'Elvis'
SELECT
    name,
    sum(yearly_name_count) -> name_count 
ORDER BY
    name_count desc
LIMIT 10;
''')

# multiple queries can result from one text batch
for row in results:
    # get results for first query
    answers = row.fetchall()
    for x in answers:
        print(x)
```

### LLM Usage

Connect to your favorite provider and generate queries with confidence and high accuracy.

```python
from trilogy import Environment, Dialects
from trilogy.ai import Provider, text_to_query
import os

executor = Dialects.DUCK_DB.default_executor(
    environment=Environment(working_path=Path(__file__).parent)
)

api_key = os.environ.get(OPENAI_API_KEY)
if not api_key:
    raise ValueError("OPENAI_API_KEY required for gpt generation")
# load a model
executor.parse_file("flight.preql")
# create tables in the DB if needed
executor.execute_file("setup.sql")
# generate a query
query = text_to_query(
    executor.environment,
    "number of flights by month in 2005",
    Provider.OPENAI,
    "gpt-5-chat-latest",
    api_key,
)

# print the generated trilogy query
print(query)
# run it
results = executor.execute_text(query)[-1].fetchall()
assert len(results) == 12

for row in results:
    # all monthly flights are between 5000 and 7000
    assert row[1] > 5000 and row[1] < 7000, row

```

### CLI Usage

Trilogy can be run through a CLI tool, also named 'trilogy'.

**Basic syntax:**
```bash
trilogy run <cmd or path to trilogy file> <dialect>
```

**With backend options:**
```bash
trilogy run "key x int; datasource test_source(i:x) grain(x) address test; select x;" duckdb --path <path/to/database>
```

**Format code:**
```bash
trilogy fmt <path to trilogy file>
```

#### Backend Configuration

**BigQuery:**
- Uses applicationdefault authentication (TODO: support arbitrary credential paths)
- In Python, you can pass a custom client

**DuckDB:**
- `--path` - Optional database file path

**Postgres:**
- `--host` - Database host
- `--port` - Database port  
- `--username` - Username
- `--password` - Password
- `--database` - Database name

**Snowflake:**
- `--account` - Snowflake account
- `--username` - Username
- `--password` - Password

## More Resources

- [Interactive demo](https://trilogydata.dev/demo/)
- [Public model repository](https://github.com/trilogydata/trilogy-public-models) - Great place for modeling examples
- [Full documentation](https://trilogydata.dev/)

## Python API Integration

### Root Imports

Are stable and should be sufficient for executing code from Trilogy as text.

```python
from pytrilogy import Executor, Dialect
```

### Authoring Imports

Are also stable, and should be used for cases which programatically generate Trilogy statements without text inputs
or need to process/transform parsed code in more complicated ways.

```python
from pytrilogy.authoring import Concept, Function, ...
```

### Other Imports

Are likely to be unstable. Open an issue if you need to take dependencies on other modules outside those two paths. 

## MCP/Server

Trilogy is straightforward to run as a server/MCP server; the former to generate SQL on demand and integrate into other tools, and MCP
for full interactive query loops.

This makes it easy to integrate Trilogy into existing tools or workflows.

You can see examples of both use cases in the trilogy-studio codebase [here](https://github.com/trilogy-data/trilogy-studio-core)
and install and run an MCP server directly with that codebase.

If you're interested in a more fleshed out standalone server or MCP server, please open an issue and we'll prioritize it!

## Trilogy Syntax Reference 

Not exhaustive - see [documentation](https://trilogydata.dev/) for more details.

### Import
```sql
import [path] as [alias];
```

### Concepts

**Types:**
`string | int | float | bool | date | datetime | time | numeric(scale, precision) | timestamp | interval | array<[type]> | map<[type], [type]> | struct<name:[type], name:[type]>`

**Key:**
```sql
key [name] [type];
```

**Property:**
```sql
property [key].[name] [type];
property x.y int;

# or multi-key
property <[key],[key]>.[name] [type];
property <x,y>.z int;
```

**Transformation:**
```sql
auto [name] <- [expression];
auto x <- y + 1;
```

### Datasource
```sql
datasource <name>(
    <column_and_concept_with_same_name>,
    # or a mapping from column to concept
    <column>:<concept>,
    <column>:<concept>,
)
grain(<concept>, <concept>)
address <table>;

datasource orders(
    order_id,
    order_date,
    total_rev: point_of_sale_rev,
    customomer_id: customer.id
)
grain orders
address orders;

```

### Queries

**Basic SELECT:**
```sql
WHERE
    <concept> = <value>
SELECT
    <concept>,
    <concept>+1 -> <alias>,
    ...
HAVING
    <alias> = <value2>
ORDER BY
    <concept> asc|desc
;
```

**CTEs/Rowsets:**
```sql
with <alias> as
WHERE
    <concept> = <value>
select
    <concept>,
    <concept>+1 -> <alias>,
    ...

select <alias>.<concept>;
```

### Data Operations

**Persist to table:**
```sql
persist <alias> as <table_name> from
<select>;
```

**Export to file:**
```sql
COPY INTO <TARGET_TYPE> '<target_path>' FROM SELECT
    <concept>, ...
ORDER BY
    <concept>, ...
;
```

**Show generated SQL:**
```sql
show <select>;
```

**Validate Model**
```sql
validate all
validate concepts abc,def...
validate datasources abc,def...
```


## Contributing

Clone repository and install requirements.txt and requirements-test.txt.

Please open an issue first to discuss what you would like to change, and then create a PR against that issue.

## Similar Projects

Trilogy combines two aspects: a semantic layer and a query language. Examples of both are linked below:

**Semantic layers** - tools for defining a metadata layer above SQL/warehouse to enable higher level abstractions:
- [MetricFlow](https://github.com/dbt-labs/metricflow)
- [Cube](https://github.com/cube-js/cube)  
- [Zillion](https://github.com/totalhack/zillion)

**Better SQL** has been a popular space. We believe Trilogy takes a different approach than the following, but all are worth checking out. Please open PRs/comment for anything missed!
- [Malloy](https://github.com/malloydata/malloy)
- [Preql](https://github.com/erezsh/Preql)
- [PRQL](https://github.com/PRQL/prql)