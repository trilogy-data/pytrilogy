## PreQL

Preql [Pre-quel] is a declarative, typed language that compiles to SQL [sequel].


## Why?

SQL is fantastic.

SQL has been the de-facto language for working with data for decades. Data professionals 
can use a common, declarative syntax to interact with anything from local file based databases
to global distributed compute clusters.

SQL solves the wrong problem.

SQL is a declarative language for reading and manipulating data in tables in SQL databases.

This is a perfect fit for an application interacting with a datastore. 

But in data warehouses, a _table_ is an leaky abstraction. Users actually want to declare
_conceptual_ queries. Seeing revenue by product line is a goal; the table that contains
the products and the table that contains revenue are implementation details.

This mismatch between what is being declared
leads to a sprawling mass of SQL that is duplicative an hard to follow and is critical 
to business function. Fortune 500 companies spend millions of dollars trying to reverse 
engineer the original intent of SQL to document dataflwo or lineage, or to refactor
business logic when moving to a new database.

## How PreQL solves this

PreQL separates declared conceptual manipulation - [Profit] = [Revenue] - [Cost] from the
database that implements those concepts and executes queries. This allows conceptual 
manipulation to be strongly typed and validated through static analysis, which can then be 
explicitly tested against the declared concrete datasources to produce empirical
proof of correctness. Additionally, the two definitions can be independently evolved
over time. 

A new analyst in a company can be productive in PreQL in minutes; a new analyst with SQL might
take a month to understand which tables are relevant. 

#### SQL
```sql
USE AdventureWorks;

SELECT t.Name, SUM(s.SubTotal) AS [Sub Total],
STR(Sum([TaxAmt])) AS [Total Taxes],
STR(Sum([TotalDue])) AS [Total Sales]

FROM Sales.SalesOrderHeader AS s

INNER JOIN Sales.SalesTerritory as t ON

s.TerritoryID = t.TerritoryID

GROUP BY t.Name
ORDER BY t.Name
```

### PreQL
```sql
SELECT 
    territory_name,
    sub_total,
    total_taxes,
    total_sales
order by territory_name desc
```
