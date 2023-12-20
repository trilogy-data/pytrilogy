## Exploring the Titanic

This titanic demo is a simple example of how PreQL syntax works. 
It uses the common titanic dataset, a single table with the following fields
about passengers.


<div>
<span class="column-badge" style="margin-right: 5px;" v-for="field in fields">
 <Badge :text="field" />
</span>
</div>

## Our First Queries


::: tip
For basic queries, PreQL should be almost identical to SQL. When in doubt, try the SQL syntax! This demo uses in-memory DuckDB, but the principles will be the same for a database such as Postgres,
Bigquery, or Snowflake - and PreQL will adjust syntax automatically to match.
:::

<QueryComponent v-for="query in startQueries"  :title='query.title' :query = 'query.query'
model='titanic'>
</QueryComponent>

## The Model

Those queries could run because we have a *model* defined already; we'll dig into this more later.
For now, just note that each concept is declared with a type, we have one derived
metric, and there is a datasource defined that maps these concepts to a table.

::: tip
A model describes the relationships between PreQL concepts and the underlying data. It is a contract that can be used to generate queries.
:::


```sql

key passenger.id int;
property passenger.id.age int;
property passenger.id.survived bool;
property passenger.id.name string;
property passenger.id.class int;
property passenger.id.fare float;
property passenger.id.cabin string;
property passenger.id.embarked bool;
property passenger.id.last_name <- split(passenger.name,',')[1];

metric passenger.id.count <- count(passenger.id);

datasource raw_data (
    passengerid:passenger.id, #numeric identifier created for dataset
    age:passenger.age, # age of passenger
    survived:passenger.survived, # 1 = survived, 0 = died
    pclass:passenger.class, #class of passenger
    name:passenger.name, #full name of the passenger
    fare:passenger.fare, # the price paid by passneger
    cabin:passenger.cabin, # the cabin the passenger was in
    embarked:passenger.embarked,
)
grain (passenger.id)
address raw_titanic;
```

## Derived Concepts and Filtering


::: tip
We'll use 'last_name' as a rough family identifier in these queries; if you explore in the sandbox
you'll find that's not quite right, but it's good enough for now. 
:::


<QueryComponent v-for="query in detailQueries"  :title='query.title' :query = 'query.query'
:dependencies = 'query.dependencies'>
</QueryComponent>

## Sandbox

Now, try writing your own queries in the sandbox below. The following concepts
will be predefined for you.

Each query is stateless, so must contain all dependencies. If you want to define a new concept to 
use in your query, you must define it in the query and
separate it with a semicolon from the query that uses it. 

#### Try answering these questions (click to show a possible answer)

<ul>
<Accordian  title="Did different classes have different average fares?" >
<SQL  maxWidthScaling=".8"  query="select passenger.class, avg(passenger.fare)->avg_class_fare;"/>
</Accordian>
<Accordian  title="Were people in higher classes more likely to survive?" >
<SQL maxWidthScaling=".8" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
select passenger.class, count(survivor)/count(passenger.id)*100->survival_rate;
"/>
</Accordian>
<Accordian  title="Were certain ages more likely to survive?" >
<SQL maxWidthScaling=".8" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
select 
    cast(passenger.age / 10 as int) * 10 -> passenger_decade, 
    count(survivor)/count(passenger.id)->survival_rate,
    count(passenger.id) -> bucket_size
order by passenger_decade desc
;
"/>
</Accordian>
<Accordian  title="What was the average family (assume one last name is one family) survival rate in each class?" >
<SQL maxWidthScaling=".8" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
SELECT
    passenger.class,
    avg( count(survivor) by passenger.last_name / count(passenger.id) by passenger.last_name ) -> avg_class_family_survival_rate,
    avg( count(passenger.id) by passenger.last_name ) -> avg_class_family_size
ORDER BY  
    passenger.class asc
;
"/>
</Accordian>
</ul>

#### Available Concepts
<div style="margin-top: 5px;">
<span class="column-badge" style="margin-right: 5px;" v-for="concept in concepts">
 <Badge :text="concept" />
</span>
</div>

<FreeformQueryComponent model='titanic'/>

## Multiple Tables

You've been able to do some initial analysis on the titanic dataset, but now your
data engineer has gotten excited about someone named Kimball. They've 
refactored your dataset to normalize it, and now you have the following tables.

- fact_titanic
- dim_class
- dim_passenger

Let's see how we can use PreQL to query this new dataset.

<QueryComponent v-for="query in startQueries.concat(detailQueries)"  :title='query.title' :query = 'query.query'
model = 'titanic_normalized'>
</QueryComponent>

This should look pretty familiar. What's going on? Take a look at the queries being generated by
clicking the 'sql' tab to confirm that something has changed. 


## The Model

Let's look at our new model.

```sql
key passenger.id int;
key _class_id int;
property passenger.id.age int;
property passenger.id.survived bool;
property passenger.id.name string;
property passenger.id.fare float;
property passenger.id.cabin string;
property passenger.id.embarked int;
property passenger.last_name <- index_access(split(passenger.name,','),1);



datasource dim_passenger (
    id:passenger.id,
	age:passenger.age,
	name:passenger.name,
	last_name:passenger.last_name
    ) 
grain (passenger.id) 
address dim_passenger;

datasource fact_titanic (
    passengerid:passenger.id,
	survived:passenger.survived,
	class_id:_class_id,
	fare:passenger.fare,
	cabin:passenger.cabin,
	embarked:passenger.embarked
    ) 
grain (passenger.id) 
address fact_titanic;

datasource dim_class (
    id:_class_id,
	class:passenger.class
    ) 
grain (_class_id) 
address dim_class;


``` 

Note that our concepts are _almost_ unchanged. We've added a new _class_id concept to capture the new surrogate key added to dim_class. 

We've also changed the datasource definitions to point to the new tables.

But as a consumer - you haven't needed to change a thing. We are still encoding the same logical concepts,
and the semantic relations between them have not changed. 

This is the convenience of separating the query language from the data model. The data model expresses a contract that can be evolved independently of the underlying materialized database tables, enabling transparent refactoring, aggregation, and remodeling to reflect the changing needs of the business.

## Sandbox Two

Try querying the new model in the sandbox below. 

<FreeformQueryComponent model='titanic_normalized' />

## Saving Results / ETL

Imagine you want to create tables or save the outputs of a query to power a dashboard. 

Preql supports this through the `persist` keyword. 

This keyword is used to signify that any table created by preql is effectively a _cache_ of a 
given output. You've already defined canonical sources; if we copy that data into a new table
it's only valid until the sources change. 

In base PreQL, this can be used to create or update a table, such
as on powering a dashboard.

::: tip
PreQL asserts that a warehouse will have a number
of roots that contain core definitions, and a number of caches derived from them that are refreshed
on some cadence. Note however that if your sources are not incremental, these caches _are_ the source
of truth for a point in time value!
:::

The first query here shows a persist command; the second shows how the generated query
will reference the persisted value. 

<QueryComponent v-for="query in etlQueries"  :title='query.title' :query = 'query.query'
model = 'titanic' :dependencies='query.dependencies'> 
</QueryComponent>

::: tip
PreQLT is a superset of PreQL under development that adds additional keywords to support ETL workflows
and integrates closely with DBT to support a full data warehouse workflow.
:::

<script>
export default {
data() {
    return {
        startQueries: [{
            'title': 'Basic Select',
            'query': 'select passenger.name, passenger.id limit 5;',
            'description': "A basic select to see what our names and IDs look like."
        },
        {
            'title': 'How many survived?',
            'query': `select 
    passenger.survived, 
    passenger.id.count, 
    count(passenger.id)-> passenger_count_alt
;`,
            'description': `As we have a aggregate defined already, we can query that directly, or create a derived metric directly in the query. Both passenger.id.count and passenger_count_alt will be the same here.`
        }],
        detailQueries: [{
            'title': 'Family Sizing (By Last Name)',
            'query': `select 
    passenger.last_name, 
    passenger.id.count
order by 
    passenger.id.count desc
limit 5;`,
            'description': "We can define new concepts that are transformations of existing concepts and reuse them in queries. Here we split the name field on the comma, and take the first element, which is the last name. We then count the number of passengers with each last name, and order by that count."
        },
        {
            'title': 'How many survived from each family (by last name)?',
            'query': `auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.last_name,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;`,
            'description': `While where clauses can be used to filter the output of a query, many common patterns can instead by implemented by creating filtered concepts. Here we create a new concept, surviving_passenger, which is a subset of passenger.id where passenger.survived = 1. We then use this concept to count the number of surviving passengers with each last name.`,
            'dependencies':[]

        },
        {
            'title': 'Familes (by last name) where everyone survived',
            'query': `auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.last_name,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
where
    passenger.id.count=surviving_size
order by
    passenger.id.count desc
limit 5;`,
            'description': `The where clause only has access to the output statements of the select. To filter on the output of a derived metric, we can use a where clause on the select itself. Here we only return large families where at least two people survived. For simple filtering, this is more idiomatic than creating a new concept.`,
            'dependencies':[]

        }],
        etlQueries: [{
            'title': 'Basic Persist',
            'query': `
property passenger.id.split_cabin <- unnest(split(passenger.cabin, ' '));
persist cabin_info into dim_cabins from 
select 
    passenger.id, 
    passenger.split_cabin;`,
'description': "Create a dim table."
        },
        {
            'title': 'Query Our Persisted Table',
            'query': `select passenger.id, passenger.split_cabin;`,
            'description': `As we have persisted into a new table, our query will now reference this.`,
            'dependencies': [`property passenger.id.split_cabin <- unnest(split(passenger.cabin, ' '));
persist cabin_info into dim_cabins from select passenger.id, passenger.split_cabin;`]
        }],
        fields: ['PassengerId','Survived','Pclass','Name','Sex','Age','SibSp','Parch','Ticket','Fare','Cabin','Embarked'],
        concepts: ['passenger.id', 'passenger.age', 'passenger.survived', 'passenger.name', 'passenger.class', 'passenger.fare', 'passenger.cabin', 'passenger.embarked']
    };
}
}
</script>
