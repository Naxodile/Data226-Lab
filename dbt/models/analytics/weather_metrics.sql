{# A day with less than this much rain counts as "dry" #}
{% set dry_threshold_mm = 1.0 %}

WITH w AS (
    SELECT * FROM {{ ref('weather_clean') }}
),

base AS (
    SELECT
        *,
        IFF(precipitation_mm < {{ dry_threshold_mm }}, 1, 0) AS is_dry,
        -- increments on every wet day, so consecutive dry days share one group id
        SUM(IFF(precipitation_mm >= {{ dry_threshold_mm }}, 1, 0))
            OVER (PARTITION BY latitude, longitude ORDER BY obs_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wet_group
    FROM w
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'obs_date']) }} AS weather_key,
    city,
    latitude,
    longitude,
    obs_date,
    temp_max,
    temp_min,
    temp_mean,
    precipitation_mm,
    weather_code,

    -- Moving averages of mean temperature
    AVG(temp_mean) OVER (PARTITION BY latitude, longitude ORDER BY obs_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)   AS temp_ma_7d,
    AVG(temp_mean) OVER (PARTITION BY latitude, longitude ORDER BY obs_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)  AS temp_ma_30d,

    -- Temperature anomaly: deviation from the location's average over the loaded window
    temp_mean - AVG(temp_mean) OVER (PARTITION BY latitude, longitude) AS temp_anomaly,

    -- Rolling rainfall totals
    SUM(precipitation_mm) OVER (PARTITION BY latitude, longitude ORDER BY obs_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)   AS rain_7d,
    SUM(precipitation_mm) OVER (PARTITION BY latitude, longitude ORDER BY obs_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)  AS rain_30d,

    -- Dry spell length: consecutive days below the rain threshold (0 on a wet day)
    IFF(is_dry = 1,
        ROW_NUMBER() OVER (PARTITION BY latitude, longitude, wet_group ORDER BY obs_date),
        0)                                          AS dry_spell_days
FROM base
