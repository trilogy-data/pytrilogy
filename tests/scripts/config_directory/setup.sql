CREATE TABLE world_capitals (
    country TEXT,
    capital TEXT,
    population INTEGER,        -- city population
    latitude DOUBLE,
    longitude DOUBLE
);

INSERT INTO world_capitals (country, capital, population, latitude, longitude) VALUES
    ('United States',       'Washington, D.C.',   689545,   38.8951,  -77.0364),
    ('Canada',              'Ottawa',             934243,   45.4215,  -75.6972),
    ('United Kingdom',      'London',             8982000,  51.5074,   -0.1278),
    ('France',              'Paris',              2148000,  48.8566,    2.3522),
    ('Germany',             'Berlin',             3769000,  52.5200,   13.4050),
    ('Italy',               'Rome',               2873000,  41.9028,   12.4964),
    ('Spain',               'Madrid',             3223000,  40.4168,   -3.7038),
    ('Australia',           'Canberra',           462000,   -35.2809, 149.1300),
    ('Japan',               'Tokyo',              13960000, 35.6895,  139.6917),
    ('China',               'Beijing',            21540000, 39.9042,  116.4074),
    ('India',               'New Delhi',          25700000, 28.6139,   77.2090),
    ('Brazil',              'Brasília',           3055149,  -15.8267, -47.9218),
    ('Mexico',              'Mexico City',        9209944,  19.4326,  -99.1332),
    ('South Africa',        'Pretoria',           741651,   -25.7479,  28.2293),
    ('Egypt',               'Cairo',              9900000,  30.0444,   31.2357);

CREATE TABLE users_with_pk (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP
);

INSERT INTO users_with_pk (user_id, username, email, created_at) VALUES
    (1, 'alice', 'alice@example.com', '2023-01-01 10:00:00'),
    (2, 'bob', 'bob@example.com', '2023-01-02 11:00:00'),
    (3, 'charlie', 'charlie@example.com', '2023-01-03 12:00:00');
