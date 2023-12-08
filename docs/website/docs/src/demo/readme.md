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

<QueryComponent v-for="query in startQueries"  :title='query.title' :query = 'query.query'>
</QueryComponent>

## The Model

Those queries could run because we have a model defined already; we'll dig into this more later.
For now, just note that each concept is declared with a type, we have one derived
metric, and there is a datasource defined that maps these concepts to a table.

(In memory in DuckDB for this demo, but in a real system this would be a database)

```sql

key passenger.id int;
property passenger.id.age int;
property passenger.id.survived bool;
property passenger.id.name string;
property passenger.id.class int;
property passenger.id.fare float;
property passenger.id.cabin string;
property passenger.id.embarked bool;

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

<QueryComponent v-for="query in detailQueries"  :title='query.title' :query = 'query.query'
:dependencies = 'query.dependencies'>
</QueryComponent>

## Sandbox

Now, try writing your own queries in the sandbox below. The following concepts
will be predefined for you.

Each query is stateless, so if you want to define a new concept in your query,
separate it with a semicolon from the query that uses it. 

#### Try answering these questions (click to show)

<ul>
<Accordian  title="Did different classes have different average fares?" >
<SQL  query="select passenger.class, avg(passenger.fare)->avg_class_fare;"/>
</Accordian>
<Accordian  title="Were people in higher classes more likely to survive?" >
<SQL maxWidthScaling=".7" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
select passenger.class, count(survivor)/count(passenger.id)*100->survival_rate;
"/>
</Accordian>
<Accordian  title="Were certain ages more likely to survive?" >
<SQL maxWidthScaling=".7" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
select 
    cast(passenger.age / 10 as int) * 10 -> passenger_decade, 
    count(survivor)/count(passenger.id)->survival_rate,
    count(passenger.id) -> bucket_size
order by passenger_decade desc
;
"/>
</Accordian>
<Accordian  title="For each family, how much did their survival rate differ from the average for their class?" >
<SQL maxWidthScaling=".7" query="
auto survivor <- filter passenger.id where passenger.survived = 1;
auto survival_rate <- count(survivor)/count(passenger.id)*100;
select passenger.class, per_class_survival_rate-> class_survival_rate;
select passenger.family, survival_rate->family_survival_rate;
select 
    passenger.family,
    survival_rate -> family_survival_rate,
    survival_rate - class_survival_rate -> family_survival_rate_diff
order by 
    abs(family_survival_rate_diff) desc
;
"/>
</Accordian>
</ul>

#### Available Concepts
<div>
<span class="column-badge" style="margin-right: 5px;" v-for="concept in concepts">
 <Badge :text="concept" />
</span>
</div>

<FreeformQueryComponent/>
<!-- 
## Multiple Tables

You've been able to do some great analysis on the titanic dataset, but now your
data engineer has gotten excited about someone named Kimball. They've 
refactored your dataset to normalize it, and now you have the following tables.

- fact_titanic
- dim_cabin
- dim_passenger
- dim_family

Let's see how we can use PreQL to query this new dataset.

<QueryComponent v-for="query in startQueries"  :title='query.title' :query = 'query.query'
:model = 'titanic_normalized'>
</QueryComponent>

This should look pretty familiar. What's going on?


## The Model

Let's look at our new model.

```sql
key passenger.id int;
property passenger.id.age int;
property passenger.id.survived bool;
property passenger.id.name string;
property passenger.id.class int;
property passenger.id.fare float;
property passenger.id.cabin string;
property passenger.id.embarked bool;
``` 
-->

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
            'title': 'Family Sizing',
            'query': `property passenger.id.family <- split(passenger.name, ',')[1];

select 
    passenger.family, 
    passenger.id.count
order by 
    passenger.id.count desc
limit 5;`,
            'description': "We can define new concepts that are transformations of existing concepts and reuse them in queries. Here we split the name field on the comma, and take the first element, which is the family name. We then count the number of passengers in each family, and order by that count."
        },
        {
            'title': 'How many survived from each family?',
            'query': `auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;`,
            'description': `While where clauses can be used to filter the output of a query, many common patterns can instead by implemented by creating filtered concepts. Here we create a new concept, surviving_passenger, which is a subset of passenger.id where passenger.survived = 1. We then use this concept to count the number of surviving passengers in each family.`,
            'dependencies':[`property passenger.id.family <- split(passenger.name, ',')[1];`]

        },
        {
            'title': 'Familes where everyone survived',
            'query': `auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
where
    passenger.id.count=surviving_size
order by
    passenger.id.count desc
limit 5;`,
            'description': `The where clause only has access to the output statements of the select. To filter on the output of a derived metric, we can use a where clause on the select itself. Here we only return large families where at least two people survived. For simple filtering, this is more idiomatic than creating a new concept.`,
            'dependencies':[`property passenger.id.family <- split(passenger.name, ',')[1];`]

        }],
        fields: ['PassengerId','Survived','Pclass','Name','Sex','Age','SibSp','Parch','Ticket','Fare','Cabin','Embarked'],
        concepts: ['passenger.id', 'passenger.age', 'passenger.survived', 'passenger.name', 'passenger.class', 'passenger.fare', 'passenger.cabin', 'passenger.embarked']
    };
}
}
</script>

