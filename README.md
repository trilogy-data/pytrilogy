## PreQL

pypreql is a experimental implementation of the preql language [https://github.com/preqldata].

The preql language spec itself will be linked from the above repo. 

Pypreql can be run locally to parse and execute preql [.preql] models. 

## Dialects

The POC supports alpha syntax for

- SQL Server
- Bigquery

## Setting Up Your Environment

Recommend that you work in a virtual environment with requirements from both requirements.txt and requirements-test.txt installed. The latter is necessary to run
tests (surprise). To run all tests you must also have docker installed.

## Running Tests

The tests are implemented primarily in pytest. A portion of the tests are dependent on having access to an AdventureWOrks2019DW example database
in Microsoft SQL Server that can be downloaded via this [link]https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2019.bak.

The tests will treat this as database server a pytest fixture, starting a docker image if the tests detect a sql server is not already running. Before
you run tests you must build this docker image. From the root of this repository run the following to fetch the database data and build a docker image
containg it

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


## Contributing