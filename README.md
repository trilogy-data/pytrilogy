## PreQL

pypreql is a experimental implementation of the preql language [https://github.com/preqldata].

The preql language spec itself will be linked from the above repo. 

Pypreql can be run locally to parse and execute preql [.preql] models. 

## Dialects

The POC supports alpha syntax for

- SQL Server
- Bigquery

## Developing

Clone repo, install requirements.txt.

Run tests to confirm proper setup.

To get adventureworks tests to pass, install [Adventureworks datawarehouse](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms)
locally in an express instance of SQL server.

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