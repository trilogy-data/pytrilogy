
# NYC Citibike Dataset



## Examples
This example walks through an [existing analysis](https://fitriwidyan.medium.com/nyc-citi-bike-trips-data-analysis-a07a1db9c1be) of New York Citibike usage, 
but writes out the queries in PreQL. The full code can be found in the script.preql
section in this folder.

For this kind of one-off analysis, the queries should look quite similar. Note that the
preql examples are intended to be run sequentially as they define a few concepts
that are re-used in later queries. 


## Comparisons

### Basic Select
The first query counts bikes in the system by year.

SQL:
```sql
SELECT 
    EXTRACT(YEAR FROM starttime) AS year, 
    COUNT(DISTINCT(bikeid)) AS num_bikes
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
GROUP BY year
ORDER BY year;
```

In preql, we already have a bike.count metric defined in the public model, 
so we'll reference that and derive the year from the start time.
These queries are similar. 

PreQL:
```sql
property trip.year<-year(trip.start_time);

SELECT
	trip.year,
	bike.count
ORDER BY
    trip.year asc
limit 100;
```

### Case Statement

Next we'll look at travel by 'generation'. Since we haven't implemented PreQL case/switch statements yet,
this gets a bit hacky.

```sql
WITH new_view AS(
 SELECT birth_year,
 CASE WHEN birth_year BETWEEN 1883 AND 1900 THEN ‘Lost Generation’
      WHEN birth_year BETWEEN 1901 AND 1927 THEN ‘G.I. Generation’
      WHEN birth_year BETWEEN 1928 AND 1945 THEN ‘Silent Generation’         
      WHEN birth_year BETWEEN 1946 AND 1964 THEN ‘Baby Boomers’
      WHEN birth_year BETWEEN 1965 AND 1980 THEN ‘Generation X’
      WHEN birth_year BETWEEN 1981 AND 1996 THEN ‘Millenials'         
      WHEN birth_year BETWEEN 1997 AND 2012 THEN ‘Generation Z' 
      ELSE 'Other'
    END AS generation
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`)
SELECT COUNT(birth_year) AS users, generation
FROM new_view
GROUP BY generation
ORDER BY users DESC;
```

In preql we'll define a new property of the birth year for the generation,
then provide a datasource off a query as our source.
```preql
property rider.birth_year.generation string;

datasource generations (
    birth_year:rider.birth_year,
    generation:generation
    )
    grain(rider.birth_year)
query '''SELECT birth_year,
 CASE WHEN birth_year BETWEEN 1883 AND 1900 THEN 'Lost Generation'
      WHEN birth_year BETWEEN 1901 AND 1927 THEN 'G.I. Generation'
      WHEN birth_year BETWEEN 1928 AND 1945 THEN 'Silent Generation'
      WHEN birth_year BETWEEN 1946 AND 1964 THEN 'Baby Boomers'
      WHEN birth_year BETWEEN 1965 AND 1980 THEN 'Generation X'
      WHEN birth_year BETWEEN 1981 AND 1996 THEN 'Millenials'
      WHEN birth_year BETWEEN 1997 AND 2012 THEN 'Generation Z'
      ELSE 'Other'
    END AS generation
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`
  group by birth_year''';

select
    generation,
    trip.count
order by
    trip.count desc;
```

### Stations by 2016 trips

This sql find the number of rides that started from given stations
by a subscriber in 2016.

```sql
SELECT start_station_name, num_station
FROM 
  (SELECT start_station_name, COUNT(start_station_name) AS       
   num_station, EXTRACT(YEAR FROM starttime) AS year
   FROM `bigquery-public-data.new_york_citibike.citibike_trips`
   WHERE usertype = 'Subscriber'
   GROUP BY start_station_name,year
   ORDER BY year)
WHERE year = 2016
ORDER BY num_station DESC
LIMIT 10
```


In preql, we'll reuse the year we already defined, filter to trips in that year
where the type were subscriber, count those, and provide trip.start_station_name
in the output to aggregate to that level. 
```sql
key subscriber_rides_2016 <- filter trip.start_time where trip.year=2016 and trip.user_type='Subscriber';

metric subscriber_ride_count_2016 <- count(subscriber_rides_2016);

select
    trip.start_station_name,
    subscriber_ride_count_2016,
order by
    subscriber_ride_count_2016 desc
limit 10;

```


### Bike Stats


```sql
SELECT bikeid, num_trip, duration, ROUND((duration/num_trip), 2) AS avg_duration_trip
FROM
    (SELECT bikeid, SUM(tripduration) AS duration, COUNT(*) AS    
     num_trip
     FROM `bigquery-public-data.new_york_citibike.citibike_trips`
     WHERE bikeid IS NOT NULL
     GROUP BY bikeid
     ORDER BY duration DESC)
LIMIT 10;
```

We have a few of these defined in our public model, so the only additional
one is average trip duration. Then this is a striaghtforward select -
no nesting required.
```preql
metric trip.avg_duration <- trip.total_duration / trip.count;

select
    bike.id,
    trip.count,
    trip.total_duration,
    trip.avg_duration,
order by
    trip.total_duration desc
limit 10;
```

### Rides by Gender

Rides by gender is straightforward in SQL.
```sql
SELECT EXTRACT(YEAR FROM starttime) AS year,
       COUNT(CASE WHEN gender = ‘female’ THEN 1 END ) AS 
       count_female,
       COUNT(CASE WHEN gender = ‘male’ THEN 1 END ) AS count_male
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
GROUP BY year
ORDER BY year;
```

We'll highlight two strategies to answer this.
We can either define a generic aggregation - male_trips - that can be created at any grain,
or define a property of year that is the output of aggregating to that level in a query.

For this query, those produce identical outcomes - but if you reuse the concepts later 
on you
```preql
property male_trip  <- filter trip.start_time where rider.gender = 'male';
property female_trip <- filter trip.start_time where rider.gender = 'female';

# we can either define an arbitrary grain metric here, that can be aggregated
# to any possible grain later (like trip.month)
metric male_trips <- count(male_trip);
metric female_trips <- count(female_trip);

# or create a new property of the year implicitly via a select query. 

select
    trip.year,
    count(male_trip)-> yearly_male_trips,
    count(female_trip) -> yearly_female_trips
order by
    trip.year
    asc;

property trip.month <- month(trip.start_time);

select
    trip.year,
    trip.month,
    male_trips,
    female_trips,
    male_trips / yearly_male_trips -> percent_of_yearly_total_male_trips,
    female_trips /yearly_female_trips -> percent_of_yearly_total_female_trips
  
where trip.year = 2018
order by trip.month desc;
```

## Subscriber

Subscriber query is straightforward.

```sql
SELECT EXTRACT(YEAR FROM starttime) AS year,
       COUNT(CASE WHEN usertype = ‘Subscriber’ THEN 1 END ) AS
       count_subscriber,
       COUNT(CASE WHEN usertype = ‘Customer’ THEN 1 END ) AS    
       count_customer
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
GROUP BY year
ORDER BY year;
```


```sql

property subscriber_trip <- filter trip.start_time where trip.user_type = 'Subscriber';
property customer_trip <- filter trip.start_time where trip.user_type = 'Customer';

select 
    trip.year,
    count(trip.start_time) -> yearly_trips,
    count(subscriber_trip) -> yearly_subscriber_trips,
    count(customer_trip) -> yearly_customer_trips
order by 
    trip.year asc;
   
```


## Trip Growth

Calculating trip growth shows how to use window functions, such as lag/lead. 

```sql
SELECT year, trip, previous, trip-previous AS trip_growth, 
       ROUND((trip-previous)/previous*100, 2) AS 
       percentage_trip_growth
FROM (SELECT year, trip, LAG(trip) OVER (ORDER BY year) AS previous
  FROM (SELECT EXTRACT(YEAR FROM starttime) AS year,
        COUNT(start_station_id) AS trip
        FROM `bigquery-public-data.new_york_citibike.citibike_trips`
        GROUP BY year)
  WHERE year IS NOT NULL
  GROUP BY year, trip
  ORDER BY year)
ORDER BY year


```


```sql
metric lagging_yearly_trips <- lag yearly_trips by trip.year asc;
metric yoy_trip_growth <- yearly_trips - lagging_yearly_trips;
metric yoy_growth_ratio <- round(yoy_trip_growth / lagging_yearly_trips * 100 ,2);

select
    trip.year,
    yearly_trips,
    lagging_yearly_trips,
    yoy_trip_growth,
    yoy_growth_ratio
order by 
    trip.year asc;
```