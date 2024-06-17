## PreQL/Trilogy

pypreql is an experimental implementation of the [PreQL/Trilogy] (prequel trilogy) language, a extension of SQL that replaces tables/joins with a lightweight semantic binding layer.

PreQL/Trilogy looks like SQL, but simpler. It's a modern SQL refresh targeted at SQL lovers who want reusability and simplicity with the power and iteratability of SQL. It compiles to SQL, making it easy to debug, and can be run againstany supported SQL backend.  

PypreQL can be run locally to parse and execute preql [.preql] models using the `trilogy` CLI tool, or can be run in python using the `preql` package.

You can read more about the project [here](https://preqldata.dev/) and try out an interactive demo on the page an interactive demo [here](https://preqldata.dev/demo). 

PreQL looks like like SQL:
```sql
SELECT
    name,
    count(name) as name_count
WHERE 
    name='Elvis'
ORDER BY
    name_count desc
LIMIT 10;
```
## Goals
vs SQL, the goals are:

Preserve:
- Correctness
- Accessibility

Enhance:
- Simplicity
- Understandability
- Refactoring/mantainability
- Reusability

Maintain:
- Acceptable performance

## Hello World

Save the following code in a file named `hello.preql`

```python
key sentence_id int;
property sentence_id.word_one string; # comments after a definition 
property sentence_id.word_two string; # are syntactic sugar for adding
property sentence_id.word_three string; # a description to it

# comments in other places are just comments

# define our datasources as queries in duckdb
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

# an actual select statement
SELECT
    --sentence_id,
    word_one || ' ' || word_two ||  word_three as hello_world, # outputs must be named, trailing commas are okah
WHERE 
    sentence_id = 1
;
# semicolon termination for all statements

```

Run the following from the directory the file is in.

```bash
trilogy run hello.preql duckdb
```


## Backends

The current PreQL implementation supports these backends:

- Bigquery
- SQL Server
- DuckDB
- Snowflake

## Basic Example - Python

Preql can be run directly in python.

A bigquery example, similar to bigquery [the quickstart](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console)

```python


from preql import Dialects, Environment

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
address bigquery-public-data.usa_names.usa_1910_2013;

'''
)
executor = Dialects.BIGQUERY.default_executor(environment=environment)

results = executor.execute_text(
'''SELECT
    name,
    sum(yearly_name_count) -> name_count 
WHERE
    name = 'Elvis'
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

Preql can be run through a CLI tool, 'trilogy'.

After installing preql, you can run the trilogy CLI with two required positional arguments; the first the path to a file or a direct command,
and second the dialect to run.

`trilogy run <cmd or path to preql file> <dialect>`

> [!TIP]
> This will only work for basic backends, such as Bigquery with local default credentials; if the backend requires more configuration, the CLI will require additional config arguments.

The CLI can also be used for formatting. PreQL has a default formatting style that should always be adhered to.

`trilogy fmt <path to preql file>`


## More Examples

Examples can be found in the [public model repository](https://github.com/preqldata/trilogy-public-models).
This is a good place to start for more complex examples.


## Developing

Clone repository and install requirements.txt and requirements-test.txt.

## Contributing

Please open an issue first to discuss what you would like to change, and then create a PR against that issue.

## Similar in space

"Better SQL" has been a popular space. We believe Trilogy/PreQL takes a different approach then the following,
but all are worth checking out. Please open PRs/comment for anything missed!


- [malloy](https://github.com/malloydata/malloy)
- [preql](https://github.com/erezsh/Preql)
- [PREQL](https://github.com/PRQL/prql)