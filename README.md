## DATA 226 Lab
This project combines Snowflake, Airflow, and dbt to create metrics for use in weather analytics.

An Airflow DAG (`HW3`) loads the last 60 days of daily weather for two cities from the [Open-Meteo API](https://open-meteo.com/en/docs) into Snowflake. When the load finishes, it triggers a second DAG (`dbt_weather_analytics`) that runs dbt to compute moving averages, temperature anomaly, rolling rainfall and dry spell length.

```
Open-Meteo API -> Airflow ETL (HW3) -> Snowflake raw table -> Airflow dbt DAG -> metrics table + snapshot
```

## Project structure

```
dags/
  etl_dag.py                  # extract -> transform -> load -> trigger dbt
  dbt_dag.py                  # dbt deps -> run -> test -> snapshot
dbt/
  dbt_project.yml
  profiles.yml
  packages.yml
  macros/
  models/
    sources.yml
    schema.yml                # tests
    transform/                # weather_clean (CTE)
    analytics/                # weather_metrics (table)
  snapshots/                  # snapshot_weather_metrics
```

## Setup

1. Create an Airflow connection named `snowflake_conn`.
2. Have `dags/` mounted into your Airflow `dags` folder and `dbt/` to `dbt`.
3. Optional: set an Airflow Variable `cities` (JSON list of `{"city", "lat", "lon"}`) to change the default cities (San Jose, Los Angeles).

## Running

Unpause both DAGs in Airflow. The dbt DAG starts automatically after etl_dag succeeds.

To run dbt by hand from the `dbt/` folder:

```bash
dbt deps     --profiles-dir .
dbt run      --profiles-dir .
dbt test     --profiles-dir .
dbt snapshot --profiles-dir .
```
