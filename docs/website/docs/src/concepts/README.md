## PreQL is SQL, simplified
#### SELECT without the FROM, JOIN, or GROUP BY

PreQL/Trilogy is a statically typed language for declarative data access that compiles to SQL against common databases. Users define and query against a semantic layer, not tables.
        Just as a database knows how to compile a query referencing columns to the best execution path, PreQL knows how to map a query referencing concepts to the right tables and aggregation.  
        It combines the simplicity of SQL with the best characteristics of a typed language. It can be used directly or compiled to SQL embedded in other tools.

```sql
SELECT
    product_line,
    revenue,
    revenue_from_top_customers,
    revenue_from_top_customers / revenue -> revenue_from_top_customers_pct
```

## For the Analyst
#### Never query the wrong table again

A business runs on data. But getting to the right table, figuring out how to join it, and dealing with the inevitable deprecation takes up too much time. By writing scripts a level above the raw tables,
          PreQL guarantees you're using the right tables and makes migrations a breeze. You won't have to update a script after you get it perfect ever again. And you don't have to wait for 
          some central team to update the model; you have access to the full expressiveness of SQL to add in the logic you need.

```sql

SELECT
    customer.name,
    customer.address,
    order.id,
    order.placed.date,
    CASE WHEN len(order.products)>1 then 'multiple' else order.products[0].name END -> product_name,
WHERE
    order.placed.date>='2020-01-01'

```
        

## For the Engineer
#### Modern, expressive language features

Compile time checks and type enforcement stops bugs before they get to prod and ensures that your database upgrade doesn't
          break key reports. Preql can fit seamlessly into a modern CI solution to provide confidence in your data. Compiling to SQL means PreQL can be adopted incrementally and embedded into standard toolchains like DBT, Tableau, and Looker. Lineage of reports is automatically visible all the way to source tables,
          and you can dynamically build aggregates that are transparently referenced by queries.
        

## For the Data Engineer
### Scripts that are faster, more fun to write, and easier to mantain

PreQL makes your scripts shorter, more reusable, more expressive, simpler, and database agnostic. Functions and concepts are clearly defined, composable, and testable. You can
          transparently optimize your underlying tables for performance and accessibility without breaking user scripts. And trailing commas are okay!
        

## Anatomy of a Query

A basic PreQL statement is one or more lines ending a semicolon. 

The most common statement is a select, which will start with select and then have one or more concept or constant refrences to output. Each item in the select must be mapped to an output name via the arrow assignment -> operator. This example is equivalent to "select 1 as constant_one, '2' as constant_string_two" in most databases. 

```sql
select 
    1 -> constant_one,
    '2'-> constant_string_two
;
```
        

## Concepts and Constants - the building blocks

PreQL queries can reference constants - such as integers or strings, shown in the first query - or Concepts, the core semantic building block in PreQL. PreQL concepts are either keys/properties - values such as a name, ID, or price -
        or metrics, which are aggregatable to different levels. The cost of a banana is a property; the total cost of groceries is a metric.
        

## Grains and Aggregation

A query is automatically grouped - or aggregated - up to the grain of all keys in the output selection. Metrics will be calculated at this level using appropriate logic. A property is a special key that is related to a parent key; 
        for example, a order ID might have a time it was placed, a location, a source. Only the parent key is included in the grain if a property and the base key are both in the select.
        

## Models

Typical queries will import an existing semantic model to reference concepts. This is done via imports.

```sql 
import bigquery.github.repo as repo;
```
        

## Querying

Once imported, a model can be queried using typical SQL syntax. Note that no tables need to be referenced.

```sql
select 
    repo.repo,
    repo.license,
order by 
    repo.repo asc
limit 10;
```
        

## Querying
A model can be extended at any point. This is an assignment operation, where a function is being used to derive a new concept. 

Functions have defind output data types, so we don't need to specify a type here. The auto type will let the compiler infer the type [property, key, or metric].

```sql
auto license_count <- count(repo.repo) by repo.license;

select
  repo.license,
  license_count
order by 
  license_count desc
limit 10;
```
        

## Querying

A query can also be used to extend a model. 

This query shows a select with an assignment, where the select is creating a new concept.
This new concept can be used in another query without requiring redefinition. An explicit definition is generally preferable, but this is useful for one-off queries.

```sql
select 
  repo.license,
  count(repo.repo) -> license_count
order by 
  license_count desc
limit 10;
``` 

## Filtering

While PreQL supports a global where clause similar to SQL, it's often preferred to declare a new filtered key. This enables easy reuse of the concept and specificity.
        A global where clause will filter all datasources with the provided key, where a scoped where clause will filter only the scoped concept.
        Scoped concepts are preferred as they are more explicit and reusable. 
        
        

## Filtering Examples

These first two queries will produce identical outputs, but the filtered concept can be re-used in other queries.

```sql
auto mit_repo <- filter repo.repo where repo.license = 'mit';

select 
  mit_repo,
  repo.license,
limit 10;

select 
  repo.repo,
  repo.license,
where 
  repo.license = 'mit'
limit 10;

select
  count(mit_repo)->mit_repo_count,
limit 10;
```
        

## Windows and Ranks

PreQL supports window functions through specific window keywords, such as rank, lag, and lead. As with filters, ranking should generally be defined explicitly 
        as a new concept to enable reuse, then referenced in the query. 
        
        

## Rank Example

```sql
auto license_repo_count <- count(repo.repo) by repo.license;

auto license_rank <- rank repo.license by license_repo_count desc;

select 
  repo.license,
  license_repo_count,
  license_rank
where 
  license_rank<10
order by 
  license_rank asc
limit 10;
```
        

## Datasources

Datasources are the interface between a model and a database. They can be created, migrated, and altered without requiring any changes
        to queries built on top of the model, assuming that there are still valid mappings for each required concept. Static analysis can can be used
        to ensure that all queries are still valid after a datasource is altered, enabling safe migrations. 
        
        

## Datasource Examples

A basic datasource defines a mapping between columns and concepts, a grain, or granularity of table and the address in the underlying database that will be queried.

Datasources can also be defined off queries, which enables expressing any kind of logic the underlying
SQL database can support. However, datasources using custom SQL will be harder to migrate between
different SQL dialects.

```sql
datasource licenses (
  repo_name:repo.repo,
  license: repo.license,
)
grain (repo.repo)
address bigquery-public-data.github_repos.licenses;

datasource languages (
  repo_name:repo.repo,
  language.name: language.language,
  language.bytes: language.per_repo_bytes
  )
  grain (repo.repo, language.language)
  query '''
select
  repo_name,
  language
FROM '''bigquery-public-data.github_repos.languages'''
CROSS JOIN UNNEST(language) AS language
''';
```

    
