## PreQL/Trilogy

pypreql is an experimental implementation of the [PreQL/Trilogy](https://github.com/preqldata) (prequel trilogy) language, a variant on SQL intended to embrace the best features of SQL while fixing common pain points.

Preql looks like SQL, but doesn't require table references, group by, or joins. It's crafted by data professionals to be more human-readable and less error-prone than SQL, and is perfect for a modern data company that just can't quit SQL but wants less pain, including compatability with common tools such as DBT through a rich extension ecosystem.

The Preql language spec itself will be linked from the above repo. 

Pypreql can be run locally to parse and execute preql [.preql] models.  

You can try out an interactive demo [here](https://preqldata.dev/).


Preql looks like this:
```sql
SELECT
    name,
    name_count.sum
WHERE 
    name like '%elvis%'
ORDER BY
    name_count.sum desc
LIMIT 10;
```

## More Examples

Examples can be found in the [public model repository](https://github.com/preqldata/trilogy-public-models). 
This is a good place to start for a basic understanding of the language. 

## Backends

The current Preql implementation supports compiling to 3 backend SQL flavors:

- Bigquery
- SQL Server
- DuckDB



## Basic Example

Preql can be run directly in python.

A bigquery example, similar to [the quickstart](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console)

```python


from preql import Dialects, Environment

environment = Environment()

environment.parse('''

key name string;
key gender string;
key state string;
key year int;
key name_count int;
auto name_count.sum <- sum(name_count);

datasource usa_names(
    name:name,
    number:name_count,
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
    name_count.sum
ORDER BY
    name_count.sum desc
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

## Developing

Clone repository and install requirements

## Contributing

Please open an issue first to discuss what you would like to change, and then create a PR against that issue.


## Similar in space

- singleorigin
- malloy