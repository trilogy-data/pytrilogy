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
property passenger.age int;
property passenger.survived bool;
property passenger.name string;
property passenger.passenger_class int;
property passenger.fare float;
property passenger.cabin string;
property passenger.embarked bool;

metric passenger.id.count <- count(passenger.id);

datasource raw_data (
    passengerid:passenger.id, #numeric identifier created for dataset
    age:passenger.age, # age of passenger
    survived:passenger.survived, # 1 = survived, 0 = died
    pclass:passenger.passenger_class, #class of passenger
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
            'title': 'How many survived?',
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

        }],
        fields: ['PassengerId','Survived','Pclass','Name','Sex','Age','SibSp','Parch','Ticket','Fare','Cabin','Embarked']
    };
}
}
</script>

