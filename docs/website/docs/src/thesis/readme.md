## Why does this exist?

SQL is fantastic.

SQL has been the de-facto language for working with data for decades. Data professionals can use a common, declarative syntax to interact with anything from local file based databases to global distributed compute clusters.

But SQL solves the wrong problem for the modern data stack.

SQL is a declarative language for reading and manipulating data in tables in SQL databases.

This is a perfect fit for an application interacting with a datastore.

But in data warehouses, a table is an leaky abstraction. Users don't care about tables; tables are a means to an end. They want the data, and the table is a detail. Tables will be replicated; aggregated; cached - and the user spends all their time on the container, not the product.

## PreQL puts the answers first

A natural query language for the data should be oriented around the outputs, not where it happens to be.

Seeing revenue by product line is a goal; the table that contains the products and the table that contains revenue are implementation details.

This is what an analyst wants:
```sql
select
    product_line,
    revenue
```

This is what they write
```sql
select
    product_line,
    revenue
from fact_revenue_latest
```

This mismatch between what is being declared leads to a sprawling mass of SQL that is duplicative,
hard to follow, brittle - and yet critical to the business. Fortune 500 companies spend millions of dollars trying to reverse engineer the original intent of SQL to document dataflow or lineage, or to refactor business logic when moving to a new database.

## How PreQL solves this

PreQL separates declared conceptual manipulation - [Profit] = [Revenue] - [Cost] from the database that implements those concepts and executes queries. These concepts are strongly typed and validated through static analysis, which can be explicitly tested against the declared concrete datasources to produce empirical proof of correctness for a given expression of business logic. These two definitions can then be independently evolved and validated over time.

A new analyst in a company can be productive in PreQL in minutes; a new analyst with SQL might take days to understand which tables are relevant.

#### SQL
```sql
USE AdventureWorks;

SELECT 
    t.Name, 
    SUM(s.SubTotal) AS [Sub Total],
    STR(Sum([TaxAmt])) AS [Total Taxes],
    STR(Sum([TotalDue])) AS [Total Sales]
FROM Sales.SalesOrderHeader AS s
    INNER JOIN Sales.SalesTerritory as t ON s.TerritoryID = t.TerritoryID
GROUP BY 
    t.Name
ORDER BY 
    t.Name
```

#### PreQL
```sql
import concepts.sales as sales;

select
    sales.territory_name,
    sales.sub_total,
    sales.total_taxes,
    sales.total_sales,
order by
    sales.territory_name desc;
```

## What's the catch?

The example above cheats a little, with the import `import concepts.sales as sales;`.

As a *Semantic Query Langage, PreQL requires some up front modeling before the first query can be run. This is a tradeoff for the benefits of a strongly typed language. The cost to model the data is incurred infrequenetly, and then the savings are amortized over every single user and query.

However, PreQL is designed to be easy to learn and use, and to be able to be incrementally adopted - this modeling can even be done in-line in a script as part of an incremental or adhoc query.
