{# Track only the raw observed values. Derived metrics shift every day as the
   rolling window moves and would create noisy history rows. #}
{% snapshot snapshot_weather_metrics %}

{{
  config(
    target_schema='snapshot',
    unique_key='weather_key',
    strategy='check',
    check_cols=['temp_max', 'temp_min', 'precipitation_mm', 'weather_code'],
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('weather_metrics') }}

{% endsnapshot %}
