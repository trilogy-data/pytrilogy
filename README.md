## Trilogy
[![Website](https://img.shields.io/badge/INTRO-WEB-orange?)](https://trilogydata.dev/)
[![Discord](https://img.shields.io/badge/DISCORD-CHAT-red?logo=discord)](https://discord.gg/Z4QSSuqGEd)

The Trilogy language is an experiment in better SQL for analytics - a streamlined SQL that replaces tables/joins with a lightweight semantic binding layer and provides easy reuse and composability. It compiles to SQL - making it easy to debug or integrate into existing workflows - and can be run against any supported SQL backend.  

[pytrilogy](https://github.com/trilogy-data/pytrilogy) is the reference implementation, written in Python.

### What Trilogy Gives You

- Speed - write faster, with concise, powerful syntax
- Efficiency - write less SQL, and reuse what you do
- Fearless refactoring
- Testability
- Easy to use for humans and LLMs

Trilogy is epsecially targeted at data consumption, providing a rich metadata layer that makes visualizing Trilogy easy and expressive. 

> [!TIP]
> You can try Trilogy in a [open-source studio](https://trilogydata.dev/trilogy-studio-core/). More details on the language can be found on the [documentation](https://trilogydata.dev/).

Start in the studio to explore Trilogy. For deeper work and integration, `pytrilogy` can be run locally to parse and execute trilogy model [.preql] files using the `trilogy` CLI tool, or can be run in python by importing the `trilogy` package.

Installation: `pip install pytrilogy`

### Trilogy Looks Like SQL

```sql
import names;

const top_names <- ['Elvis', 'Elvira', 'Elrond', 'Sam'];

def initcap(word) -> upper(substring(word, 1, 1)) || substring(word, 2, len(word));

WHERE 
    @initcap(name) in top_names
SELECT
    name,
    sum(births) as name_count
ORDER BY
    name_count desc
LIMIT 10;
```
## Goals
Versus SQL, Trilogy aims to: 

Keep:
- Correctness
- Accessibility

Improve:
- Simplicity
- Refactoring/mantainability
- Reusability

Maintain:
- Acceptable performance

Remove:
- Lower-level procedural features
- Transactional optimizations/non-analytics features

## Hello World

Save the following code in a file named `hello.preql`

```python
# semantic model is abstract from data
key sentence_id int;
property sentence_id.word_one string; # comments after a definition 
property sentence_id.word_two string; # are syntactic sugar for adding
property sentence_id.word_three string; # a description to it

# comments in other places are just comments

# define our datasource to bind the model to data
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
Run the following from the directory the file is in.

```bash
trilogy run hello.trilogy duckdb
```

![UI Preview](hello-world.png)

## Backends

The current Trilogy implementation supports these backends:

### Core
- Bigquery
- DuckDB
- Snowflake

### Experimental
- SQL Server
- Presto

## Basic Example - Python

Trilogy can be run directly in python through the core SDK. Trilogy code can be defined and parsed inline or parsed out of files.

A bigquery example, similar to bigquery [the quickstart](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console).

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

'''
)
executor = Dialects.BIGQUERY.default_executor(environment=environment)

results = executor.execute_text(
'''
WHERE
    name = 'Elvis'
SELECT
    name,
    sum(yearly_name_count) -> name_count 
ORDER BY
    name_count desc
LIMIT 10;
'''

)
# multiple queries can result from one text batch
for row in results:
    # get results for first query
    answers = row.fetchall()
    for x in answers:
        print(x)
```


## Basic Example - CLI

Trilogy can be run through a CLI tool, also named 'trilogy'.

After installing trilogy, you can run the trilogy CLI with two required positional arguments; the first the path to a file or a direct command,
and second the dialect to run.

`trilogy run <cmd or path to trilogy file> <dialect>`

To pass arguments to a backend, append additional --<option> flags after specifying the dialect.

Example:
`trilogy run "key x int; datasource test_source ( i:x) grain(in) address test; select x;" duckdb --path <path/to/database>`

### Bigquery Args
N/A, only supports default auth. In python you can pass in a custom client.
<TODO> support arbitrary cred paths. 

### DuckDB Args
- path <optional>

### Postgres Args
- host
- port
- username
- password
- database

### Snowflake Args
- account
- username
- password


> [!TIP]
> The CLI can also be used for formatting. Trilogy has a default formatting style that should always be adhered to. `trilogy fmt <path to trilogy file>`


## More Examples

[Interactive demo](https://trilogydata.dev/demo/). 

Additional examples can be found in the [public model repository](https://github.com/trilogydata/trilogy-public-models).

This is a good place to look for modeling examples.

## Developing

Clone repository and install requirements.txt and requirements-test.txt.

## Contributing

Please open an issue first to discuss what you would like to change, and then create a PR against that issue.

## Similar in space
Trilogy combines two aspects; a semantic layer and a query language. Examples of both are linked below:

"semantic layers" are tools for defining an metadata layer above a SQL/warehouse base to enable higher level abstractions.

- [metricsflow](https://github.com/dbt-labs/metricflow)
- [cube](https://github.com/cube-js/cube)
- [zillion](https://github.com/totalhack/zillion)

"Better SQL" has been a popular space. We believe Trilogy takes a different approach then the following,
but all are worth checking out. Please open PRs/comment for anything missed!

- [malloy](https://github.com/malloydata/malloy)
- [preql](https://github.com/erezsh/Preql)
- [PREQL](https://github.com/PRQL/prql)

## Minimal Syntax Reference

#### IMPORT

`import [path] as [alias];`

#### CONCEPT

Types: `string | int | float | bool | date | datetime | time | numeric(scale, precision) | timestamp | interval | list<[type]> | map<[type], [type]> | struct<name:[type], name:[type]>`;

Key:
`key [name] [type];`

Property:
`property [key>].[name] [type];`
`property x.y int;`
or 
`property <[key](,[key])?>.<name> [type];`
`property <x,y>.z int;`


Transformation:
`auto [name] <- [expression];`
`auto x <- y + 1;`

#### DATASOURCE
```sql
datasource <name>(
    <column>:<concept>,
    <column>:<concept>,
)
grain(<concept>, <concept>)
address <table>;
```

#### SELECT

Primary acces

```sql
WHERE
    <concept> = <value>
select
    <concept>,
    <concept>+1 -> <alias>,
    ...
HAVING
    <alias> = <value2>
ORDER BY
    <concept> asc|desc
;
```

#### CTE/ROWSET

Reusable virtual set of rows. Useful for windows, filtering. 

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


#### PERSIST

Store output of a query in a warehouse table

```sql
persist <alias> as <table_name> from
<select>;
```

#### COPY

Currently supported target types are <CSV>, though backend support may vary.

```sql
COPY INTO <TARGET_TYPE> '<target_path>' FROM SELECT
    <concept>, ...
ORDER BY
    <concept>, ...
;
```

#### SHOW

Return generated SQL without executing.

```sql
show <select>;
```
