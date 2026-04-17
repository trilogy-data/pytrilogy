create or replace table airport as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/airports.parquet'
;

create or replace table flight as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/flights.parquet'
;


create or replace table aircraft as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/aircraft.parquet'
;

create or replace table aircraft_model as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/aircraft_models.parquet'
;

create or replace table carrier as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/carriers.parquet'
;
