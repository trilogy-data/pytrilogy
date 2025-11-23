-- Multi-Year Bike Ride Data Generator
-- Generates realistic bike ride data spanning 2020-2024 (5 years)
-- Includes seasonal variations, weather patterns, and realistic rider behavior

CREATE OR REPLACE TABLE tbl_ride_data AS
WITH date_series AS (
  -- Generate all dates from 2020-01-01 to 2024-12-31 using DuckDB's range function
  SELECT '2020-01-01'::DATE + INTERVAL (n) DAY AS calendar_date
  FROM range(0, 1827) AS t(n)  -- 1827 days from 2020-01-01 to 2024-12-31
),
seasonal_weather AS (
  SELECT 
    calendar_date,
    date_part('month', calendar_date) AS month_num,
    date_part('year', calendar_date) AS year_num,
    
    -- Seasonal temperature patterns (Fahrenheit)
    CASE 
      WHEN date_part('month', calendar_date) IN (12, 1, 2) THEN 25 + random() * 25  -- Winter: 25-50°F
      WHEN date_part('month', calendar_date) IN (3, 4, 5) THEN 45 + random() * 30   -- Spring: 45-75°F
      WHEN date_part('month', calendar_date) IN (6, 7, 8) THEN 65 + random() * 25   -- Summer: 65-90°F
      ELSE 40 + random() * 35  -- Fall: 40-75°F
    END AS base_temp,
    
    -- Seasonal weather probability
    CASE 
      WHEN date_part('month', calendar_date) IN (6, 7, 8) THEN 
        CASE 
          WHEN random() < 0.7 THEN 'Sunny'
          WHEN random() < 0.9 THEN 'Partly Cloudy'
          ELSE 'Overcast'
        END
      WHEN date_part('month', calendar_date) IN (12, 1, 2) THEN 
        CASE 
          WHEN random() < 0.3 THEN 'Sunny'
          WHEN random() < 0.6 THEN 'Partly Cloudy'
          WHEN random() < 0.8 THEN 'Overcast'
          ELSE 'Light Rain'
        END
      ELSE 
        CASE 
          WHEN random() < 0.5 THEN 'Sunny'
          WHEN random() < 0.75 THEN 'Partly Cloudy'
          WHEN random() < 0.9 THEN 'Overcast'
          ELSE 'Light Rain'
        END
    END AS weather_condition
  FROM date_series
),
rider_pool AS (
  -- Create a consistent pool of riders
  SELECT 
    'rider_' || n AS rider_id,
    ['Alex', 'Jordan', 'Casey', 'Morgan', 'Riley', 'Avery', 'Quinn', 'Blake', 
     'Cameron', 'Drew', 'Sam', 'Taylor', 'Reese', 'Sage', 'River', 'Phoenix',
     'Skyler', 'Rowan', 'Parker', 'Emery'][
      (random() * 20)::int + 1
    ] AS rider_name,
    
    -- Rider activity level affects frequency
    CASE 
      WHEN random() < 0.3 THEN 'casual'     -- rides 1-2x per week
      WHEN random() < 0.7 THEN 'regular'    -- rides 3-4x per week  
      ELSE 'enthusiast'                     -- rides 5-7x per week
    END AS activity_level,
    
    -- Preferred bike type
    ['Road Bike', 'Mountain Bike', 'Hybrid', 'E-bike', 'Fixie', 'Cruiser'][
      (random() * 6)::int + 1
    ] AS preferred_bike
    
  FROM range(1, 151) AS t(n)  -- 150 unique riders
),
base_rides AS (
  -- Generate rides based on seasonal patterns and rider activity
  SELECT 
    rp.rider_id,
    rp.rider_name,
    rp.activity_level,
    rp.preferred_bike AS bike_type,
    sw.calendar_date AS ride_date,
    sw.base_temp,
    sw.weather_condition,
    sw.month_num,
    sw.year_num,
    
    -- Ride frequency varies by activity level and season
    CASE 
      WHEN rp.activity_level = 'casual' THEN random() < 0.2
      WHEN rp.activity_level = 'regular' THEN random() < 0.5  
      ELSE random() < 0.8
    END AS should_ride,
    
    -- Seasonal ride frequency adjustment
    CASE 
      WHEN sw.month_num IN (6, 7, 8) THEN 1.5  -- Summer boost
      WHEN sw.month_num IN (12, 1, 2) THEN 0.4  -- Winter reduction
      ELSE 1.0
    END AS seasonal_multiplier,
    
    random() AS rand1,
    random() AS rand2,
    random() AS rand3,
    random() AS rand4
    
  FROM rider_pool rp
  CROSS JOIN seasonal_weather sw
  WHERE 
    -- Apply activity-based filtering with seasonal adjustments
    (CASE 
      WHEN rp.activity_level = 'casual' THEN random() < (0.15 * (CASE WHEN sw.month_num IN (6,7,8) THEN 1.5 WHEN sw.month_num IN (12,1,2) THEN 0.4 ELSE 1.0 END))
      WHEN rp.activity_level = 'regular' THEN random() < (0.4 * (CASE WHEN sw.month_num IN (6,7,8) THEN 1.3 WHEN sw.month_num IN (12,1,2) THEN 0.6 ELSE 1.0 END))
      ELSE random() < (0.7 * (CASE WHEN sw.month_num IN (6,7,8) THEN 1.2 WHEN sw.month_num IN (12,1,2) THEN 0.8 ELSE 1.0 END))
    END)
    -- Weather filtering - less likely to ride in bad weather
    AND (sw.weather_condition != 'Light Rain' OR random() < 0.3)
),
ride_details AS (
  SELECT 
    rider_id,
    rider_name,
    activity_level,
    bike_type,
    ride_date,
    ROUND(base_temp + (rand1 * 10 - 5), 1) AS temp_f,  -- Add daily variation
    weather_condition AS weather,
    
    -- Ride type varies by day of week and season
    CASE 
      WHEN date_part('dow', ride_date) IN (1,2,3,4,5) AND rand2 < 0.6 THEN 'Morning Commute'
      WHEN date_part('dow', ride_date) IN (1,2,3,4,5) AND rand2 < 0.8 THEN 'Lunch Break Ride'
      WHEN date_part('dow', ride_date) IN (1,2,3,4,5) THEN 'Evening Cruise'
      WHEN rand2 < 0.4 THEN 'Weekend Adventure'
      WHEN rand2 < 0.6 THEN 'Training Session'
      WHEN rand2 < 0.75 THEN 'Park Loop'
      WHEN rand2 < 0.85 THEN 'Coffee Shop Visit'
      ELSE 'Grocery Run'
    END AS ride_type,
    
    -- Route difficulty
    ['Easy', 'Easy', 'Easy', 'Moderate', 'Moderate', 'Challenging', 'Extreme'][
      (rand3 * 7)::int + 1
    ] AS difficulty,
    
    -- Starting location
    ['Downtown', 'Riverside Park', 'University District', 'Suburbs', 'Waterfront', 
     'Historic District', 'Business Park', 'Residential', 'Industrial', 'Hillside'][
      (rand4 * 10)::int + 1
    ] AS start_location,
    
    month_num,
    rand1, rand2, rand3, rand4
    
  FROM base_rides
),
calculated_rides AS (
  SELECT 
    rider_id,
    rider_name,
    activity_level,
    bike_type,
    ride_date,
    temp_f,
    weather,
    ride_type,
    difficulty,
    start_location,
    
    -- Distance varies by ride type and rider experience
    ROUND(CASE 
      WHEN ride_type IN ('Morning Commute', 'Lunch Break Ride') THEN 
        (rand1 * 6 + 2) * (CASE WHEN activity_level = 'enthusiast' THEN 1.3 ELSE 1.0 END)
      WHEN ride_type IN ('Evening Cruise', 'Coffee Shop Visit', 'Grocery Run') THEN 
        (rand1 * 12 + 3) * (CASE WHEN activity_level = 'casual' THEN 0.8 ELSE 1.0 END)
      WHEN ride_type = 'Park Loop' THEN 
        rand1 * 8 + 4
      WHEN ride_type = 'Training Session' THEN 
        (rand1 * 25 + 10) * (CASE WHEN activity_level = 'enthusiast' THEN 1.5 ELSE 1.2 END)
      ELSE -- Weekend Adventure
        (rand1 * 45 + 15) * (CASE WHEN activity_level = 'casual' THEN 0.7 WHEN activity_level = 'enthusiast' THEN 1.4 ELSE 1.0 END)
    END, 1) AS distance_miles,
    
    rand1, rand2, rand3, rand4
    
  FROM ride_details
),
final_calculations AS (
  SELECT 
    rider_id,
    rider_name,
    activity_level,
    bike_type,
    ride_date,
    temp_f,
    weather,
    ride_type,
    difficulty,
    start_location,
    distance_miles,
    
    -- Duration with variability for stops, traffic, fitness level
    ROUND(
      (distance_miles * (
        CASE 
          WHEN bike_type = 'E-bike' THEN (6 + rand2 * 3)  -- Faster
          WHEN bike_type = 'Road Bike' THEN (7 + rand2 * 4)
          WHEN bike_type = 'Mountain Bike' THEN (9 + rand2 * 5)
          ELSE (8 + rand2 * 4)  -- Hybrid, Fixie, Cruiser
        END
      )) + 
      (rand3 * 15 - 5) +  -- Random stops/delays
      (CASE WHEN difficulty = 'Extreme' THEN 10 WHEN difficulty = 'Challenging' THEN 5 ELSE 0 END)
    , 1) AS duration_minutes,
    
    -- Calories burned based on distance, bike type, difficulty, rider fitness
    ROUND(
      distance_miles * (
        CASE 
          WHEN bike_type = 'E-bike' THEN (20 + rand4 * 15)  -- Less effort
          WHEN bike_type = 'Mountain Bike' THEN (35 + rand4 * 20)  -- More effort
          WHEN bike_type = 'Road Bike' THEN (30 + rand4 * 15)
          ELSE (25 + rand4 * 15)
        END
      ) * 
      (CASE 
        WHEN difficulty = 'Easy' THEN 0.8
        WHEN difficulty = 'Moderate' THEN 1.0
        WHEN difficulty = 'Challenging' THEN 1.3
        ELSE 1.6  -- Extreme
      END) +
      (rand2 * 100)  -- Base metabolic variation
    , 0) AS calories_burned,
    
    rand1, rand2, rand3, rand4
    
  FROM calculated_rides
  WHERE distance_miles > 0.5  -- Filter out unrealistic short rides
)
SELECT 
  row_number() OVER (ORDER BY ride_date, rider_id) AS ride_id,
  rider_id,
  rider_name,
  ride_date,
  date_part('year', ride_date)::int AS ride_year,
  date_part('month', ride_date)::int AS ride_month,
  date_part('dow', ride_date)::int AS day_of_week,  -- 0=Sunday, 6=Saturday
  ride_type,
  distance_miles,
  duration_minutes,
  weather,
  temp_f,
  difficulty,
  bike_type,
  start_location,
  calories_burned,
  
  -- Average speed (with safety check for zero duration)
  CASE 
    WHEN duration_minutes > 0 THEN ROUND(distance_miles / (duration_minutes / 60), 1)
    ELSE 0
  END AS avg_speed_mph,
  
  -- Fun rating influenced by weather, temperature, and personal factors
  CASE 
    WHEN weather = 'Sunny' AND temp_f BETWEEN 60 AND 80 THEN floor(rand1 * 3 + 8)::int
    WHEN weather = 'Sunny' THEN floor(rand1 * 4 + 6)::int
    WHEN weather IN ('Partly Cloudy', 'Overcast') AND temp_f BETWEEN 50 AND 75 THEN floor(rand1 * 4 + 5)::int
    WHEN weather IN ('Partly Cloudy', 'Overcast') THEN floor(rand1 * 5 + 4)::int
    WHEN weather = 'Light Rain' THEN floor(rand1 * 3 + 3)::int
    ELSE floor(rand1 * 6 + 3)::int  -- Windy or other conditions
  END AS fun_rating,
  
  -- Additional useful fields for analysis
  activity_level AS rider_activity_level,
  
  -- Calculate if this is a personal record distance for this rider
  CASE 
    WHEN distance_miles = MAX(distance_miles) OVER (PARTITION BY rider_id) THEN true
    ELSE false
  END AS is_personal_record

FROM final_calculations
WHERE distance_miles BETWEEN 0.5 AND 200  -- Reasonable distance bounds
ORDER BY ride_date, rider_id;

-- Create helpful indexes for analysis
CREATE INDEX IF NOT EXISTS idx_ride_data_date ON tbl_ride_data(ride_date);
CREATE INDEX IF NOT EXISTS idx_ride_data_rider ON tbl_ride_data(rider_id);
CREATE INDEX IF NOT EXISTS idx_ride_data_year_month ON tbl_ride_data(ride_year, ride_month);
CREATE INDEX IF NOT EXISTS idx_ride_data_weather ON tbl_ride_data(weather);
CREATE INDEX IF NOT EXISTS idx_ride_data_type ON tbl_ride_data(ride_type);

-- Sample queries to verify the data
/*
-- Check data distribution by year
SELECT ride_year, COUNT(*) as rides, AVG(distance_miles) as avg_distance 
FROM tbl_ride_data 
GROUP BY ride_year 
ORDER BY ride_year;

-- Check seasonal patterns
SELECT 
  ride_month,
  COUNT(*) as rides,
  AVG(distance_miles) as avg_distance,
  AVG(fun_rating) as avg_fun_rating
FROM tbl_ride_data 
GROUP BY ride_month 
ORDER BY ride_month;

-- Check rider activity levels
SELECT 
  rider_activity_level,
  COUNT(*) as total_rides,
  COUNT(DISTINCT rider_id) as unique_riders,
  AVG(distance_miles) as avg_distance
FROM tbl_ride_data 
GROUP BY rider_activity_level;
*/