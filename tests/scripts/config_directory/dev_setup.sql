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
