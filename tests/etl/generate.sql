
CREATE OR REPLACE TABLE tbl_ride_data AS
WITH base_data AS (
  SELECT 
    -- Rider info
    'rider_' || (random() * 1000)::int AS rider_id,
    ['Alex', 'Jordan', 'Casey', 'Morgan', 'Riley', 'Avery', 'Quinn', 'Blake', 'Cameron', 'Drew'][
      (random() * 10)::int + 1
    ] AS rider_name,
    
    -- Ride details
    ('2024-' || LPAD(((random() * 11)::int + 1)::varchar, 2, '0') || '-' || 
    LPAD(((random() * 28)::int + 1)::varchar, 2, '0'))::Date AS ride_date,
    
    ['Morning Commute', 'Lunch Break Ride', 'Evening Cruise', 'Weekend Adventure', 
     'Training Session', 'Grocery Run', 'Coffee Shop Visit', 'Park Loop'][
      (random() * 8)::int + 1
    ] AS ride_type,
    
    -- Generate ride type index for distance calculation
    (random() * 8)::int + 1 AS ride_type_idx,
    
    -- Weather affects ride enjoyment
    ['Sunny', 'Partly Cloudy', 'Overcast', 'Light Rain', 'Windy'][
      (random() * 5)::int + 1
    ] AS weather,
    
    -- Temperature (Fahrenheit)
    ROUND(35 + random() * 50, 1) AS temp_f,
    
    -- Route difficulty
    ['Easy', 'Easy', 'Moderate', 'Moderate', 'Challenging', 'Extreme'][
      (random() * 6)::int + 1
    ] AS difficulty,
    
    -- Bike types
    ['Road Bike', 'Mountain Bike', 'Hybrid', 'E-bike', 'Fixie', 'Cruiser'][
      (random() * 6)::int + 1
    ] AS bike_type,
    
    -- Starting location
    ['Downtown', 'Riverside Park', 'University District', 'Suburbs', 'Waterfront', 
     'Historic District', 'Business Park', 'Residential'][
      (random() * 8)::int + 1
    ] AS start_location,
    
    -- Random values for calculations
    random() AS rand1,
    random() AS rand2,
    random() AS rand3,
    random() AS rand4
    
  FROM generate_series(1, 500)
),
bike_rides AS (
  SELECT 

    rider_id,
    rider_name,
    ride_date,
    ride_type,
    
    -- Distance varies by ride type (commutes shorter, adventures longer)
    ROUND(CASE 
      WHEN ride_type_idx <= 2 THEN (rand1 * 8 + 2)  -- commutes: 2-10 miles
      WHEN ride_type_idx <= 5 THEN (rand1 * 15 + 3) -- casual: 3-18 miles  
      ELSE (rand1 * 40 + 15)  -- adventures: 15-55 miles
    END, 1) AS distance_miles,
    
    weather,
    temp_f,
    difficulty,
    bike_type,
    start_location,
    rand2, rand3, rand4  -- Keep for next calculations
    
  FROM base_data
),
final_rides AS (
  SELECT 
    rider_id,
    rider_name,
    ride_date,
    ride_type,
    distance_miles,
    
    -- Duration correlates roughly with distance but has variability for stops/speed
    ROUND((distance_miles * (8 + rand2 * 7)) + (rand3 * 20 - 10), 1) AS duration_minutes,
    
    weather,
    temp_f,
    difficulty,
    bike_type,
    start_location,
    
    -- Calories burned (roughly based on distance and intensity)
    ROUND(distance_miles * (25 + rand4 * 20) + rand2 * 100, 0) AS calories_burned
    
  FROM bike_rides
)
SELECT 
    row_number() OVER () AS ride_id,
  rider_id,
  rider_name,
  ride_date,
  ride_type,
  distance_miles,
  duration_minutes,
  weather,
  temp_f,
  difficulty,
  bike_type,
  start_location,
  calories_burned,
  
  -- Average speed (now we can safely reference the calculated columns)
  ROUND(distance_miles / (duration_minutes / 60), 1) AS avg_speed_mph,
  
  -- Fun rating (weather influences this)
  CASE 
    WHEN weather = 'Sunny' THEN FLOOR(random() * 3 + 7)::int
    WHEN weather IN ('Partly Cloudy', 'Overcast') THEN FLOOR(random() * 5 + 4)::int
    ELSE FLOOR(random() * 4 + 3)::int
  END AS fun_rating

FROM final_rides;