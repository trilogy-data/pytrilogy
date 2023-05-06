## PreQL

pypreql is an experimental implementation of the [PreQL](https://github.com/preqldata).

The preql language spec itself will be linked from the above repo. 

Pypreql can be run locally to parse and execute preql [.preql] models.  

## Examples

Examples can be found in the [public model repository](https://github.com/preqldata/trilogy-public-models). 
This is a good place to start for a basic understanding of the language. 

## Dialects

The POC supports alpha syntax for

- Bigquery
- SQL Server
- DuckDB


## Setting Up Your Environment

Recommend that you work in a virtual environment with requirements from both requirements.txt and requirements-test.txt installed. The latter is necessary to run
tests (surprise). 

Pypreql is python 3.10+

## Running Tests

The tests are implemented primarily in pytest. To run all tests you are strongly suggested to have docker installed, though you can manually configured the required
data warehouse in an express edition of SQL server if docker is not possible. Guidance for the non-docker path is not provided. Docker is
STRONGLY RECOMMENDED.

A portion of the tests are dependent on having access to an AdventureWOrks2019DW example database
in Microsoft SQL Server that can be downloaded via this [link]https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2019.bak.

The tests will treat this as database server a pytest fixture, starting a docker image if the tests detect a sql server is not already running. Before
you run tests you must build this docker image. From the root of this repository run the following to fetch the database data and build a docker image
containing it

```bash
/bin/bash ./docker/build_image.sh
```

If you are using windows download the AdventureWorks2019DW database backup from the link above and place it in the ./docker path.
From the root of the repo run
```bash
docker build --no-cache ./docker/ -t pyreql-test-sqlserver
```

To run the test suite, from the root of the repository run

```python
python -m pytest ./tests
```

## Basic Example

Bigquery, similar to [the quickstart](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console)

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

```sql
import concepts.internet_sales as internet_sales;
import concepts.customer as customer;
import concepts.dates as dates;
import concepts.sales_territory as sales_territory;

select
    customer.first_name,
    customer.last_name,
    internet_sales.total_order_quantity,
    internet_sales.total_sales_amount
order by
    internet_sales.total_sales_amount desc
limit 100;


select
    dates.order_date,
    customer.first_name,
    internet_sales.total_order_quantity,
    internet_sales.total_sales_amount
order by
    internet_sales.total_sales_amount desc
limit 100;


select
    internet_sales.order_number,
    internet_sales.order_line_number,
    internet_sales.total_sales_amount,
    sales_territory.region,
    customer.first_name
order by
    internet_sales.order_number desc
limit 5;


```

```sql
import concepts.internet_sales as internet_sales;
import concepts.customer as customer;
import concepts.dates as dates;
import concepts.sales_territory as sales_territory;

select
    customer.first_name,
    customer.last_name,
    internet_sales.total_order_quantity,
    internet_sales.total_sales_amount
order by
    internet_sales.total_sales_amount desc
limit 100;


select
    dates.order_date,
    customer.first_name,
    internet_sales.total_order_quantity,
    internet_sales.total_sales_amount
order by
    internet_sales.total_sales_amount desc
limit 100;


select
    internet_sales.order_number,
    internet_sales.order_line_number,
    internet_sales.total_sales_amount,
    sales_territory.region,
    customer.first_name
order by
    internet_sales.order_number desc
limit 5;


```


## Contributing


## Similar in space

- singleorigin