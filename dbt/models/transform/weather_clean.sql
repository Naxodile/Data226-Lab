SELECT
    city,
    latitude,
    longitude,
    CAST(date AS DATE)                      AS obs_date,
    temp_max,
    temp_min,
    (temp_max + temp_min) / 2.0             AS temp_mean,
    COALESCE(precipitation, 0)              AS precipitation_mm,
    weather_code
FROM {{ source('weather', 'weather_data') }}
WHERE date IS NOT NULL
  AND temp_max IS NOT NULL
  AND temp_min IS NOT NULL
